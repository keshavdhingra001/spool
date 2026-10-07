//! The reference queue (M1): plain ordered collections, written to be obviously
//! correct rather than fast. Every later implementation (durable, replicated)
//! is checked against it.
//!
//! Layout (D17):
//! - `jobs`: every live job (not yet acked) by id. Looked up, never iterated on
//!   the apply path, so `HashMap` order cannot reach the output (D4).
//! - per queue, `waiting`: `(ready_at, id)` of every job waiting for a lease,
//!   whether its delay or backoff is still running or it is ready now. The first
//!   entry is the next job to lease, if its `ready_at` has come (D18).
//! - per queue, `dead`: ids of dead-lettered jobs (D16).
//! - `leases`: `(deadline, id)` of every leased job, so expiry pops from the front.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;

use crate::command::{Command, Event, Op, RejectReason, ReleaseReason};
use crate::queue::{Clock, Queue};
use crate::retry::QueueConfig;
use crate::types::{JobId, Lease, Millis, Payload, QueueName, Time, Token};

/// Where a job is in its life. Acked jobs are removed, not kept in a state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobState {
    /// Waiting for a lease; leasable once the clock reaches `ready_at`.
    Waiting { ready_at: Time },
    /// Leased under `token` until `deadline`.
    Leased { token: Token, deadline: Time },
    /// Used its last attempt; stays until a redrive.
    Dead,
}

#[derive(Clone, Debug)]
struct Job {
    queue: QueueName,
    payload: Payload,
    /// Leases so far (D15). Reset to 0 by a redrive.
    attempts: u32,
    state: JobState,
}

#[derive(Clone, Debug, Default)]
struct QueueState {
    config: QueueConfig,
    waiting: BTreeSet<(Time, JobId)>,
    dead: BTreeSet<JobId>,
}

/// One job as seen from outside, for the REPL and for comparing two queues.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobView {
    pub id: JobId,
    pub queue: QueueName,
    pub attempts: u32,
    pub state: JobState,
}

/// Number of jobs in each state, plus every job ever acked.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    pub waiting: u64,
    pub leased: u64,
    pub dead: u64,
    pub acked: u64,
}

impl Counts {
    /// Every job ever enqueued is in exactly one of these (D19).
    pub fn total(&self) -> u64 {
        self.waiting + self.leased + self.dead + self.acked
    }
}

#[derive(Clone, Debug, Default)]
pub struct ReferenceQueue {
    clock: Clock,
    /// The last job id assigned; ids are 1, 2, 3, ... (D10).
    last_job: u64,
    /// The last fencing token issued, across all queues (D10).
    last_token: u64,
    jobs: HashMap<JobId, Job>,
    queues: BTreeMap<QueueName, QueueState>,
    leases: BTreeSet<(Time, JobId)>,
    acked: u64,
}

impl Queue for ReferenceQueue {
    fn apply(&mut self, cmd: &Command, out: &mut Vec<Event>) {
        let now = self.clock.advance(cmd.at);
        self.expire(now, out);
        match &cmd.op {
            Op::Enqueue {
                queue,
                payload,
                delay,
            } => self.enqueue(now, queue, payload, *delay, out),
            Op::Lease { queue, visibility } => self.lease(now, queue, *visibility, out),
            Op::Heartbeat {
                job,
                token,
                visibility,
            } => self.heartbeat(now, *job, *token, *visibility, out),
            Op::Ack { job, token } => self.ack(*job, *token, out),
            Op::Nack { job, token } => self.nack(now, *job, *token, out),
            Op::Configure { queue, config } => self.configure(queue, *config, out),
            Op::Redrive { queue } => self.redrive(now, queue, out),
            Op::Tick => {}
        }
    }

    fn now(&self) -> Time {
        self.clock.now()
    }
}

impl ReferenceQueue {
    pub fn new() -> Self {
        Self::default()
    }

    /// End every lease whose deadline is at or before `now` (D18), oldest
    /// deadline first. Each one fails as of its deadline, not as of `now`, so
    /// the state after a command does not depend on how often ticks arrived.
    fn expire(&mut self, now: Time, out: &mut Vec<Event>) {
        while let Some(&(deadline, id)) = self.leases.first() {
            if deadline > now {
                break;
            }
            self.leases.pop_first();
            let JobState::Leased { token, .. } = self.jobs[&id].state else {
                unreachable!("job {id} is in the lease index but not leased");
            };
            out.push(Event::Released {
                job: id,
                token,
                reason: ReleaseReason::Expired,
            });
            self.fail(id, deadline, out);
        }
    }

    /// A lease of `id` ended without an ack at `at`: retry after a backoff, or
    /// dead-letter it if that was its last attempt (D13, D15). The caller has
    /// already removed it from `leases`.
    fn fail(&mut self, id: JobId, at: Time, out: &mut Vec<Event>) {
        let job = self.jobs.get_mut(&id).expect("failed job exists");
        let q = self.queues.get_mut(&job.queue).expect("job's queue exists");
        if job.attempts >= q.config.max_attempts {
            job.state = JobState::Dead;
            q.dead.insert(id);
            out.push(Event::DeadLettered { job: id });
        } else {
            let ready_at = at.plus(q.config.retry_delay(id, job.attempts));
            job.state = JobState::Waiting { ready_at };
            q.waiting.insert((ready_at, id));
            out.push(Event::Retrying { job: id, ready_at });
        }
    }

    fn enqueue(
        &mut self,
        now: Time,
        queue: &QueueName,
        payload: &Payload,
        delay: Millis,
        out: &mut Vec<Event>,
    ) {
        self.last_job += 1;
        let id = JobId(self.last_job);
        let ready_at = now.plus(delay);
        self.queue_mut(queue).waiting.insert((ready_at, id));
        self.jobs.insert(
            id,
            Job {
                queue: queue.clone(),
                payload: payload.clone(),
                attempts: 0,
                state: JobState::Waiting { ready_at },
            },
        );
        out.push(Event::Enqueued {
            job: id,
            queue: queue.clone(),
            ready_at,
        });
    }

    fn lease(&mut self, now: Time, queue: &QueueName, visibility: Millis, out: &mut Vec<Event>) {
        if visibility.0 == 0 {
            return reject(RejectReason::ZeroVisibility, out);
        }
        // A lease on a queue that was never used does not create it.
        let next = self.queues.get_mut(queue).and_then(|q| {
            let &(ready_at, id) = q.waiting.first()?;
            (ready_at <= now).then(|| {
                q.waiting.pop_first();
                id
            })
        });
        let Some(id) = next else {
            out.push(Event::Empty {
                queue: queue.clone(),
            });
            return;
        };
        self.last_token += 1;
        let lease = Lease {
            job: id,
            token: Token(self.last_token),
            deadline: now.plus(visibility),
        };
        let job = self.jobs.get_mut(&id).expect("waiting job exists");
        job.attempts += 1;
        job.state = JobState::Leased {
            token: lease.token,
            deadline: lease.deadline,
        };
        self.leases.insert((lease.deadline, id));
        out.push(Event::Leased {
            lease,
            attempt: job.attempts,
            payload: job.payload.clone(),
        });
    }

    fn heartbeat(
        &mut self,
        now: Time,
        id: JobId,
        token: Token,
        visibility: Millis,
        out: &mut Vec<Event>,
    ) {
        let old_deadline = match self.check_lease(id, token) {
            Ok(deadline) => deadline,
            Err(reason) => return reject(reason, out),
        };
        if visibility.0 == 0 {
            return reject(RejectReason::ZeroVisibility, out);
        }
        // May also shorten the lease, as SQS's ChangeMessageVisibility does.
        let deadline = now.plus(visibility);
        self.leases.remove(&(old_deadline, id));
        self.leases.insert((deadline, id));
        self.jobs.get_mut(&id).expect("checked").state = JobState::Leased { token, deadline };
        out.push(Event::Renewed {
            lease: Lease {
                job: id,
                token,
                deadline,
            },
        });
    }

    fn ack(&mut self, id: JobId, token: Token, out: &mut Vec<Event>) {
        let deadline = match self.check_lease(id, token) {
            Ok(deadline) => deadline,
            Err(reason) => return reject(reason, out),
        };
        self.leases.remove(&(deadline, id));
        self.jobs.remove(&id);
        self.acked += 1;
        out.push(Event::Acked { job: id });
    }

    fn nack(&mut self, now: Time, id: JobId, token: Token, out: &mut Vec<Event>) {
        let deadline = match self.check_lease(id, token) {
            Ok(deadline) => deadline,
            Err(reason) => return reject(reason, out),
        };
        self.leases.remove(&(deadline, id));
        out.push(Event::Released {
            job: id,
            token,
            reason: ReleaseReason::Nack,
        });
        self.fail(id, now, out);
    }

    fn configure(&mut self, queue: &QueueName, config: QueueConfig, out: &mut Vec<Event>) {
        if !config.is_valid() {
            return reject(RejectReason::BadConfig, out);
        }
        self.queue_mut(queue).config = config;
        out.push(Event::Configured {
            queue: queue.clone(),
            config,
        });
    }

    fn redrive(&mut self, now: Time, queue: &QueueName, out: &mut Vec<Event>) {
        let Some(q) = self.queues.get_mut(queue) else {
            return;
        };
        for id in std::mem::take(&mut q.dead) {
            let job = self.jobs.get_mut(&id).expect("dead job exists");
            job.attempts = 0;
            job.state = JobState::Waiting { ready_at: now };
            q.waiting.insert((now, id));
            out.push(Event::Redriven { job: id });
        }
    }

    /// The deadline of `id`'s lease if `token` is its current token, otherwise
    /// why the caller holds no valid lease (D5).
    fn check_lease(&self, id: JobId, token: Token) -> Result<Time, RejectReason> {
        match self.jobs.get(&id).map(|j| j.state) {
            None => Err(RejectReason::UnknownJob),
            Some(JobState::Leased { token: t, deadline }) if t == token => Ok(deadline),
            Some(JobState::Leased { .. }) => Err(RejectReason::StaleToken),
            Some(_) => Err(RejectReason::NotLeased),
        }
    }

    fn queue_mut(&mut self, name: &QueueName) -> &mut QueueState {
        self.queues.entry(name.clone()).or_default()
    }

    /// Every live job, by id.
    pub fn jobs(&self) -> Vec<JobView> {
        let mut jobs: Vec<JobView> = self
            .jobs
            .iter()
            .map(|(&id, j)| JobView {
                id,
                queue: j.queue.clone(),
                attempts: j.attempts,
                state: j.state,
            })
            .collect();
        jobs.sort_by_key(|j| j.id);
        jobs
    }

    pub fn counts(&self) -> Counts {
        let mut c = Counts {
            acked: self.acked,
            ..Counts::default()
        };
        for job in self.jobs.values() {
            match job.state {
                JobState::Waiting { .. } => c.waiting += 1,
                JobState::Leased { .. } => c.leased += 1,
                JobState::Dead => c.dead += 1,
            }
        }
        c
    }

    pub fn config(&self, queue: &QueueName) -> Option<QueueConfig> {
        self.queues.get(queue).map(|q| q.config)
    }

    /// Check every structural invariant (D19). Called after every command in
    /// tests; too slow (linear in the number of jobs) for the hot path.
    pub fn check_invariants(&self) -> Result<(), String> {
        let now = self.clock.now();
        // Every job is in its own queue's index for its state, and in no other
        // index; together with the size check below, this makes the indexes and
        // the job table a one-to-one match.
        let mut tokens = HashSet::new();
        for (&id, job) in &self.jobs {
            if id.0 == 0 || id.0 > self.last_job {
                return Err(format!("job {id} was never assigned"));
            }
            let q = self
                .queues
                .get(&job.queue)
                .ok_or_else(|| format!("job {id}: queue {} missing", job.queue))?;
            let indexed = match job.state {
                JobState::Waiting { ready_at } => q.waiting.contains(&(ready_at, id)),
                JobState::Leased { token, deadline } => {
                    if deadline <= now {
                        return Err(format!("job {id}: lease past its deadline {deadline}"));
                    }
                    if token.0 == 0 || token.0 > self.last_token || !tokens.insert(token) {
                        return Err(format!("job {id}: token {token} not unique or not issued"));
                    }
                    if job.attempts == 0 {
                        return Err(format!("job {id}: leased with 0 attempts"));
                    }
                    self.leases.contains(&(deadline, id))
                }
                JobState::Dead => q.dead.contains(&id),
            };
            if !indexed {
                return Err(format!("job {id} ({:?}) missing from its index", job.state));
            }
        }
        let indexed: usize = self.leases.len()
            + self
                .queues
                .values()
                .map(|q| q.waiting.len() + q.dead.len())
                .sum::<usize>();
        if indexed != self.jobs.len() {
            return Err(format!(
                "{indexed} index entries for {} jobs",
                self.jobs.len()
            ));
        }
        // Nothing lost or duplicated: every id ever assigned is live or acked.
        if self.jobs.len() as u64 + self.acked != self.last_job {
            return Err(format!(
                "{} live + {} acked != {} enqueued",
                self.jobs.len(),
                self.acked,
                self.last_job
            ));
        }
        for (name, q) in &self.queues {
            if !q.config.is_valid() {
                return Err(format!("queue {name}: invalid config {:?}", q.config));
            }
        }
        Ok(())
    }
}

fn reject(reason: RejectReason, out: &mut Vec<Event>) {
    out.push(Event::Rejected { reason });
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JobState::Waiting { ready_at } => write!(f, "waiting ready_at={ready_at}"),
            JobState::Leased { token, deadline } => {
                write!(f, "leased token={token} deadline={deadline}")
            }
            JobState::Dead => write!(f, "dead"),
        }
    }
}

impl fmt::Display for JobView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "job={} queue={} attempts={} {}",
            self.id, self.queue, self.attempts, self.state
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Apply text commands and return each command's events as lines.
    fn run(q: &mut ReferenceQueue, lines: &[&str]) -> Vec<String> {
        let mut out = Vec::new();
        for line in lines {
            q.apply(&line.parse().unwrap(), &mut out);
            q.check_invariants().unwrap();
        }
        out.iter().map(ToString::to_string).collect()
    }

    fn queue_with_lease() -> ReferenceQueue {
        let mut q = ReferenceQueue::new();
        run(
            &mut q,
            &["@0 enqueue a x", "@0 enqueue a y", "@1 lease a 10"],
        );
        q
    }

    #[test]
    fn lease_takes_smallest_ready_at_then_smallest_id() {
        let mut q = ReferenceQueue::new();
        let got = run(
            &mut q,
            &[
                "@0 enqueue a late delay=5",
                "@0 enqueue a first",
                "@0 enqueue a second",
                "@1 lease a 100",
                "@1 lease a 100",
                "@1 lease a 100",
                "@5 lease a 100",
            ],
        );
        assert_eq!(
            got[3..],
            [
                "leased job=2 token=1 deadline=101 attempt=1 payload=first",
                "leased job=3 token=2 deadline=101 attempt=1 payload=second",
                "empty queue=a",
                "leased job=1 token=3 deadline=105 attempt=1 payload=late",
            ]
        );
    }

    #[test]
    fn expiry_is_exclusive_of_the_deadline_and_fails_at_the_deadline() {
        let mut q = queue_with_lease();
        // Lease of job 1 has deadline 11. At 10 it is still valid.
        assert_eq!(run(&mut q, &["@10 tick"]), Vec::<String>::new());
        let got = run(&mut q, &["@500 tick"]);
        // Backoff runs from the deadline (11), not from the tick (500).
        let delay = QueueConfig::default().retry_delay(JobId(1), 1);
        assert_eq!(
            got,
            [
                "released job=1 token=1 reason=expired".to_string(),
                format!("retrying job=1 ready_at={}", Time(11).plus(delay)),
            ]
        );
    }

    #[test]
    fn checker_catches_corruption() {
        type Corrupt = fn(&mut ReferenceQueue);
        let corruptions: [(&str, Corrupt); 8] = [
            ("lost lease index", |q| q.leases.clear()),
            ("lost job", |q| {
                q.jobs.remove(&JobId(2));
                q.queues
                    .get_mut(&QueueName::new("a").unwrap())
                    .unwrap()
                    .waiting
                    .clear();
            }),
            ("double index", |q| {
                let qs = q.queues.get_mut(&QueueName::new("a").unwrap()).unwrap();
                qs.dead.insert(JobId(2));
            }),
            ("wrong state", |q| {
                q.jobs.get_mut(&JobId(2)).unwrap().state = JobState::Dead;
            }),
            ("token from the future", |q| q.last_token = 0),
            ("overdue lease", |q| {
                q.clock.advance(Time(1_000));
            }),
            // Job 1's deadline is 11; a lease is over at its deadline, not after it.
            ("lease exactly at its deadline", |q| {
                q.clock.advance(Time(11));
            }),
            ("bad config", |q| {
                let qs = q.queues.get_mut(&QueueName::new("a").unwrap()).unwrap();
                qs.config.max_attempts = 0;
            }),
        ];
        for (name, corrupt) in corruptions {
            let mut q = queue_with_lease();
            corrupt(&mut q);
            assert!(q.check_invariants().is_err(), "{name} not detected");
        }
    }
}
