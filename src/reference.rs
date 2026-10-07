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
//! - `dedup`: `(queue, key) -> (job, expires_at)` for every key still in its
//!   window (D35), and `dedup_expiry`: the same entries by expiry time.
//! - `results`: `job -> (token, result, expires_at)` for every completed job
//!   whose result is still kept (D41), and `results_expiry` by expiry time.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;

use crate::codec::{self, DecodeError, Reader};
use crate::command::{Command, Event, Op, RejectReason, ReleaseReason, ResultStatus};
use crate::queue::{Clock, Queue, Snapshot};
use crate::retry::QueueConfig;
use crate::types::{DedupKey, JobId, Lease, Millis, Payload, QueueName, Time, Token};

/// How long a dedup key is remembered after the enqueue that used it (D35).
pub const DEDUP_WINDOW: Millis = Millis(300_000);

/// How long a job's result is kept after it completes (D41).
pub const RESULT_WINDOW: Millis = Millis(300_000);

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
    dedup: BTreeMap<(QueueName, DedupKey), (JobId, Time)>,
    dedup_expiry: BTreeSet<(Time, QueueName, DedupKey)>,
    results: BTreeMap<JobId, (Token, Payload, Time)>,
    results_expiry: BTreeSet<(Time, JobId)>,
}

impl Queue for ReferenceQueue {
    fn apply(&mut self, cmd: &Command, out: &mut Vec<Event>) {
        let now = self.clock.advance(cmd.at);
        self.expire(now, out);
        self.expire_keys(now);
        self.expire_results(now);
        match &cmd.op {
            Op::Enqueue {
                queue,
                payload,
                delay,
                key,
            } => self.enqueue(now, queue, payload, *delay, key.as_ref(), out),
            Op::Lease { queue, visibility } => self.lease(now, queue, *visibility, out),
            Op::Heartbeat {
                job,
                token,
                visibility,
            } => self.heartbeat(now, *job, *token, *visibility, out),
            Op::Ack { job, token } => self.ack(*job, *token, out),
            Op::Complete { job, token, result } => self.complete(now, *job, *token, result, out),
            Op::Result { job } => self.result(*job, out),
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

    /// Forget every dedup key whose window ended at or before `now` (D35). The
    /// boundary is exclusive like a lease's (D18). No events: a key ending is
    /// not something a client can observe except by reusing it.
    fn expire_keys(&mut self, now: Time) {
        while let Some((expires_at, ..)) = self.dedup_expiry.first() {
            if *expires_at > now {
                break;
            }
            let (_, queue, key) = self.dedup_expiry.pop_first().expect("checked");
            self.dedup.remove(&(queue, key));
        }
    }

    /// Forget every result whose window ended at or before `now` (D41).
    fn expire_results(&mut self, now: Time) {
        while let Some(&(expires_at, id)) = self.results_expiry.first() {
            if expires_at > now {
                break;
            }
            self.results_expiry.pop_first();
            self.results.remove(&id);
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
        key: Option<&DedupKey>,
        out: &mut Vec<Event>,
    ) {
        if let Some(key) = key
            && let Some(&(job, _)) = self.dedup.get(&(queue.clone(), key.clone()))
        {
            out.push(Event::Deduplicated {
                job,
                queue: queue.clone(),
                key: key.clone(),
            });
            return;
        }
        self.last_job += 1;
        let id = JobId(self.last_job);
        let ready_at = now.plus(delay);
        if let Some(key) = key {
            let expires_at = now.plus(DEDUP_WINDOW);
            // At the very end of time the window saturates to nothing.
            if expires_at > now {
                self.dedup
                    .insert((queue.clone(), key.clone()), (id, expires_at));
                self.dedup_expiry
                    .insert((expires_at, queue.clone(), key.clone()));
            }
        }
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
            key: key.cloned(),
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

    /// Ack and store the result in one step (D41). A repeat by the lease that
    /// already completed the job succeeds again and changes nothing (D42).
    fn complete(
        &mut self,
        now: Time,
        id: JobId,
        token: Token,
        result: &Payload,
        out: &mut Vec<Event>,
    ) {
        let deadline = match self.check_lease(id, token) {
            Ok(deadline) => deadline,
            Err(RejectReason::UnknownJob)
                if self.results.get(&id).is_some_and(|r| r.0 == token) =>
            {
                out.push(Event::Completed { job: id, token });
                return;
            }
            Err(reason) => return reject(reason, out),
        };
        self.leases.remove(&(deadline, id));
        self.jobs.remove(&id);
        self.acked += 1;
        let expires_at = now.plus(RESULT_WINDOW);
        // At the very end of time the window saturates to nothing (as D35).
        if expires_at > now {
            self.results.insert(id, (token, result.clone(), expires_at));
            self.results_expiry.insert((expires_at, id));
        }
        out.push(Event::Completed { job: id, token });
    }

    fn result(&self, id: JobId, out: &mut Vec<Event>) {
        let status = if self.jobs.contains_key(&id) {
            ResultStatus::Pending
        } else if let Some((token, payload, _)) = self.results.get(&id) {
            ResultStatus::Done {
                token: *token,
                payload: payload.clone(),
            }
        } else {
            ResultStatus::Unknown
        };
        out.push(Event::Result { job: id, status });
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

    /// Every kept result (D41): `(job, token, result, expires_at)`, by job.
    pub fn results(&self) -> Vec<(JobId, Token, Payload, Time)> {
        self.results
            .iter()
            .map(|(&job, (token, payload, at))| (job, *token, payload.clone(), *at))
            .collect()
    }

    /// Every remembered dedup key (D35): `(queue, key, job, expires_at)`, in
    /// `(queue, key)` order.
    pub fn dedup_keys(&self) -> Vec<(QueueName, DedupKey, JobId, Time)> {
        self.dedup
            .iter()
            .map(|((q, k), &(job, at))| (q.clone(), k.clone(), job, at))
            .collect()
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
        // Dedup keys (D35): each names an assigned job, is still in its window,
        // was created no later than now, and the expiry index matches exactly.
        for ((queue, key), &(job, expires_at)) in &self.dedup {
            let what = format!("dedup key {queue}/{key}");
            if job.0 == 0 || job.0 > self.last_job {
                return Err(format!("{what}: job {job} was never assigned"));
            }
            if expires_at <= now || expires_at > now.plus(DEDUP_WINDOW) {
                return Err(format!("{what}: expires at {expires_at}, now {now}"));
            }
            if !self
                .dedup_expiry
                .contains(&(expires_at, queue.clone(), key.clone()))
            {
                return Err(format!("{what}: missing from the expiry index"));
            }
        }
        if self.dedup_expiry.len() != self.dedup.len() {
            return Err(format!(
                "{} dedup expiry entries for {} keys",
                self.dedup_expiry.len(),
                self.dedup.len()
            ));
        }
        // Results (D41): each belongs to an assigned job that is gone (it was
        // completed), under an issued token, still in its window, and the
        // expiry index matches exactly. A completed job counts as acked.
        for (&job, &(token, _, expires_at)) in &self.results {
            if job.0 == 0 || job.0 > self.last_job || self.jobs.contains_key(&job) {
                return Err(format!("result of job {job}: job not completed"));
            }
            if token.0 == 0 || token.0 > self.last_token {
                return Err(format!("result of job {job}: token {token} not issued"));
            }
            if expires_at <= now || expires_at > now.plus(RESULT_WINDOW) {
                return Err(format!(
                    "result of job {job}: expires at {expires_at}, now {now}"
                ));
            }
            if !self.results_expiry.contains(&(expires_at, job)) {
                return Err(format!(
                    "result of job {job}: missing from the expiry index"
                ));
            }
        }
        if self.results_expiry.len() != self.results.len() {
            return Err(format!(
                "{} result expiry entries for {} results",
                self.results_expiry.len(),
                self.results.len()
            ));
        }
        // "No more results than acks" needs no check of its own: each result
        // is a distinct assigned job that is gone, and every assigned job that
        // is gone was acked (live + acked == assigned, above).
        Ok(())
    }
}

const WAITING: u8 = 1;
const LEASED: u8 = 2;
const DEAD: u8 = 3;

/// Snapshot body (D27), little-endian, using the codec's field encodings (D24):
///
/// ```text
/// now:u64 last_job:u64 last_token:u64 acked:u64
/// queue_count:u32 { name config }            in name order
/// job_count:u64   { id:u64 name payload attempts:u32 state }   in id order
/// state = 1 ready_at:u64 | 2 token:u64 deadline:u64 | 3
/// key_count:u64   { name key job:u64 expires_at:u64 }   in (name, key) order, version 2 on
/// result_count:u64 { job:u64 token:u64 payload expires_at:u64 }   in job order, version 3 on
/// ```
///
/// The indexes are not stored: they are rebuilt from the jobs, keys and
/// results. Older versions load with the tables they lack empty (D35, D45).
impl Snapshot for ReferenceQueue {
    const STATE_VERSION: u32 = 3;

    fn encode_state(&self, out: &mut Vec<u8>) {
        codec::put_u64(out, self.clock.now().0);
        codec::put_u64(out, self.last_job);
        codec::put_u64(out, self.last_token);
        codec::put_u64(out, self.acked);
        let queues = u32::try_from(self.queues.len()).expect("under 2^32 queues");
        codec::put_u32(out, queues);
        for (name, q) in &self.queues {
            codec::put_name(out, name);
            codec::put_config(out, &q.config);
        }
        // Sorted, so the HashMap's iteration order never reaches the bytes (D4).
        let mut ids: Vec<JobId> = self.jobs.keys().copied().collect();
        ids.sort();
        codec::put_u64(out, ids.len() as u64);
        for id in ids {
            let job = &self.jobs[&id];
            codec::put_u64(out, id.0);
            codec::put_name(out, &job.queue);
            codec::put_payload(out, &job.payload);
            codec::put_u32(out, job.attempts);
            match job.state {
                JobState::Waiting { ready_at } => {
                    out.push(WAITING);
                    codec::put_u64(out, ready_at.0);
                }
                JobState::Leased { token, deadline } => {
                    out.push(LEASED);
                    codec::put_u64(out, token.0);
                    codec::put_u64(out, deadline.0);
                }
                JobState::Dead => out.push(DEAD),
            }
        }
        codec::put_u64(out, self.dedup.len() as u64);
        for ((queue, key), &(job, expires_at)) in &self.dedup {
            codec::put_name(out, queue);
            codec::put_key(out, key);
            codec::put_u64(out, job.0);
            codec::put_u64(out, expires_at.0);
        }
        codec::put_u64(out, self.results.len() as u64);
        for (job, (token, payload, expires_at)) in &self.results {
            codec::put_u64(out, job.0);
            codec::put_u64(out, token.0);
            codec::put_payload(out, payload);
            codec::put_u64(out, expires_at.0);
        }
    }

    fn decode_state(version: u32, bytes: &[u8]) -> Result<Self, DecodeError> {
        if !(1..=Self::STATE_VERSION).contains(&version) {
            return Err(DecodeError::Invalid(format!(
                "unknown snapshot version {version}"
            )));
        }
        let mut r = Reader::new(bytes);
        let mut clock = Clock::default();
        clock.advance(Time(r.u64()?));
        let mut q = ReferenceQueue {
            clock,
            last_job: r.u64()?,
            last_token: r.u64()?,
            acked: r.u64()?,
            ..Self::default()
        };
        let invalid = |what: String| DecodeError::Invalid(format!("snapshot: {what}"));
        let mut prev: Option<QueueName> = None;
        for _ in 0..r.u32()? {
            let name = r.name()?;
            if prev.as_ref().is_some_and(|p| *p >= name) {
                return Err(invalid(format!("queue {name} out of order")));
            }
            let config = r.config()?;
            q.queues.insert(
                name.clone(),
                QueueState {
                    config,
                    ..QueueState::default()
                },
            );
            prev = Some(name);
        }
        let mut prev_id = JobId(0);
        for _ in 0..r.u64()? {
            let id = JobId(r.u64()?);
            if id <= prev_id {
                return Err(invalid(format!("job {id} out of order")));
            }
            prev_id = id;
            let queue = r.name()?;
            let payload = r.payload()?;
            let attempts = r.u32()?;
            let state = match r.u8()? {
                WAITING => JobState::Waiting {
                    ready_at: Time(r.u64()?),
                },
                LEASED => JobState::Leased {
                    token: Token(r.u64()?),
                    deadline: Time(r.u64()?),
                },
                DEAD => JobState::Dead,
                tag => {
                    return Err(DecodeError::UnknownTag {
                        what: "job state",
                        tag,
                    });
                }
            };
            let qs = q
                .queues
                .get_mut(&queue)
                .ok_or_else(|| invalid(format!("job {id}: unknown queue {queue}")))?;
            match state {
                JobState::Waiting { ready_at } => {
                    qs.waiting.insert((ready_at, id));
                }
                JobState::Leased { deadline, .. } => {
                    q.leases.insert((deadline, id));
                }
                JobState::Dead => {
                    qs.dead.insert(id);
                }
            }
            q.jobs.insert(
                id,
                Job {
                    queue,
                    payload,
                    attempts,
                    state,
                },
            );
        }
        if version >= 2 {
            let mut prev: Option<(QueueName, DedupKey)> = None;
            for _ in 0..r.u64()? {
                let entry = (r.name()?, r.key()?);
                if prev.as_ref().is_some_and(|p| *p >= entry) {
                    return Err(invalid(format!(
                        "dedup key {}/{} out of order",
                        entry.0, entry.1
                    )));
                }
                let (job, expires_at) = (JobId(r.u64()?), Time(r.u64()?));
                q.dedup_expiry
                    .insert((expires_at, entry.0.clone(), entry.1.clone()));
                q.dedup.insert(entry.clone(), (job, expires_at));
                prev = Some(entry);
            }
        }
        if version >= 3 {
            let mut prev = JobId(0);
            for _ in 0..r.u64()? {
                let job = JobId(r.u64()?);
                if job <= prev {
                    return Err(invalid(format!("result of job {job} out of order")));
                }
                prev = job;
                let (token, payload) = (Token(r.u64()?), r.payload()?);
                let expires_at = Time(r.u64()?);
                q.results_expiry.insert((expires_at, job));
                q.results.insert(job, (token, payload, expires_at));
            }
        }
        r.finish()?;
        // Everything else a valid state must satisfy (D19): ids and tokens
        // issued, nothing lost, no lease past its deadline, valid configs.
        q.check_invariants().map_err(invalid)?;
        Ok(q)
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
    fn dedup_window_saturates_at_the_end_of_time() {
        let mut q = ReferenceQueue::new();
        let max = u64::MAX;
        let got = run(
            &mut q,
            &[
                &format!("@{} enqueue a x key=k", max - 1),
                &format!("@{} enqueue a x key=k", max - 1),
                &format!("@{max} enqueue a x key=k"),
                &format!("@{max} enqueue a x key=k"),
            ],
        );
        // At max - 1 the window is one millisecond; at max it is empty, so
        // nothing is remembered and every enqueue adds a job.
        assert_eq!(
            got,
            [
                format!("enqueued job=1 queue=a ready_at={} key=k", max - 1),
                "deduplicated job=1 queue=a key=k".to_string(),
                format!("enqueued job=2 queue=a ready_at={max} key=k"),
                format!("enqueued job=3 queue=a ready_at={max} key=k"),
            ]
        );
        assert!(q.dedup_keys().is_empty());
    }

    #[test]
    fn checker_catches_dedup_corruption() {
        let keyed = || {
            let mut q = queue_with_lease();
            run(&mut q, &["@2 enqueue a z key=k"]);
            q
        };
        type Corrupt = fn(&mut ReferenceQueue);
        let corruptions: [(&str, Corrupt); 6] = [
            ("lost expiry index", |q| q.dedup_expiry.clear()),
            ("orphan expiry entry", |q| {
                let k = (QueueName::new("b").unwrap(), DedupKey::new("j").unwrap());
                q.dedup_expiry.insert((Time(9), k.0, k.1));
            }),
            ("extra expiry entry", |q| {
                let (k, _) = q.dedup.pop_first().unwrap();
                q.dedup_expiry.insert((Time(9), k.0, k.1));
                q.dedup.insert(
                    (QueueName::new("b").unwrap(), DedupKey::new("j").unwrap()),
                    (JobId(1), Time(9)),
                );
            }),
            ("key past its window", |q| {
                q.clock.advance(Time(2).plus(DEDUP_WINDOW));
                q.leases.clear();
                q.jobs
                    .retain(|_, j| !matches!(j.state, JobState::Leased { .. }));
                q.acked += 1;
            }),
            ("key names a job never assigned", |q| {
                q.dedup.values_mut().for_each(|e| e.0 = JobId(99));
            }),
            ("key from the future", |q| {
                let ((name, key), (job, at)) = q.dedup.pop_first().unwrap();
                q.dedup_expiry.clear();
                let later = at.plus(Millis(1));
                q.dedup.insert((name.clone(), key.clone()), (job, later));
                q.dedup_expiry.insert((later, name, key));
            }),
        ];
        keyed().check_invariants().unwrap();
        for (name, corrupt) in corruptions {
            let mut q = keyed();
            corrupt(&mut q);
            assert!(q.check_invariants().is_err(), "{name} not detected");
        }
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

    fn snapshot(q: &ReferenceQueue) -> Vec<u8> {
        let mut out = Vec::new();
        q.encode_state(&mut out);
        out
    }

    #[test]
    fn snapshot_round_trips_every_state() {
        let mut q = queue_with_lease();
        // Job 2 becomes dead, a third job waits with a delay, queue b is empty.
        run(
            &mut q,
            &[
                "@1 configure a 1 0 0",
                "@1 lease a 10",
                "@1 nack 2 2",
                "@1 enqueue a z delay=50",
                "@1 configure b 2 5 9",
                "@1 enqueue b k1 key=x",
                "@1 enqueue a k2 key=x",
            ],
        );
        assert_eq!(q.dedup_keys().len(), 2);
        let c = q.counts();
        assert_eq!((c.waiting, c.leased, c.dead), (3, 1, 1));
        let bytes = snapshot(&q);
        let back = ReferenceQueue::decode_state(3, &bytes).unwrap();
        assert_eq!(snapshot(&back), bytes);
        assert_eq!(back.jobs(), q.jobs());
        assert_eq!(back.dedup_keys(), q.dedup_keys());
        assert_eq!(back.counts(), q.counts());
        assert_eq!(back.now(), q.now());
        // The rebuilt indexes behave like the originals.
        let more = [
            "@2 lease a 10",
            "@3 enqueue a again key=x",
            "@20 redrive a",
            "@20 lease a 5",
            "@60 tick",
        ];
        let (mut a, mut b) = (q, back);
        assert_eq!(run(&mut a, &more), run(&mut b, &more));
        assert_eq!(snapshot(&a), snapshot(&b));
    }

    #[test]
    fn older_snapshots_load_with_empty_tables() {
        // Version 1 (M2) is version 3 without the key and result tables at
        // the end; version 2 (M3) without the result table.
        let q = queue_with_lease();
        let v3 = snapshot(&q);
        let (v1, v2) = (&v3[..v3.len() - 16], &v3[..v3.len() - 8]);
        assert_eq!(snapshot(&ReferenceQueue::decode_state(1, v1).unwrap()), v3);
        assert_eq!(snapshot(&ReferenceQueue::decode_state(2, v2).unwrap()), v3);
        assert_eq!(
            ReferenceQueue::decode_state(1, v2).unwrap_err(),
            DecodeError::TrailingBytes(8)
        );
        assert_eq!(
            ReferenceQueue::decode_state(2, &v3).unwrap_err(),
            DecodeError::TrailingBytes(8)
        );
        for version in [0, 4] {
            assert!(matches!(
                ReferenceQueue::decode_state(version, &v3),
                Err(DecodeError::Invalid(m)) if m.contains("unknown snapshot version")
            ));
        }
    }

    #[test]
    fn complete_stores_the_result_once() {
        let mut q = queue_with_lease();
        let got = run(
            &mut q,
            &[
                "@2 result 1",
                "@2 complete 1 1 ok",
                "@3 complete 1 1 other",
                "@3 complete 1 2 ok",
                "@3 ack 1 1",
                "@4 result 1",
                "@4 result 9",
                "@4 lease a 10",
                "@5 ack 2 2",
                "@5 complete 2 2 late",
                "@5 result 2",
            ],
        );
        assert_eq!(
            got,
            [
                "result job=1 pending",
                "completed job=1 token=1",
                // The same lease again: success, and the first result stays (D42).
                "completed job=1 token=1",
                "rejected reason=unknown_job",
                "rejected reason=unknown_job",
                "result job=1 done token=1 payload=ok",
                "result job=9 unknown",
                "leased job=2 token=2 deadline=14 attempt=1 payload=y",
                "acked job=2",
                // Acked without a result: nothing to repeat.
                "rejected reason=unknown_job",
                "result job=2 unknown",
            ]
        );
        assert_eq!(q.counts().acked, 2);
        // The window: kept until 2 + 300000, exclusive.
        let end = 2 + RESULT_WINDOW.0;
        let got = run(
            &mut q,
            &[
                &format!("@{} result 1", end - 1),
                &format!("@{} complete 1 1 ok", end - 1),
                &format!("@{end} result 1"),
                &format!("@{end} complete 1 1 ok"),
            ],
        );
        assert_eq!(
            got,
            [
                "result job=1 done token=1 payload=ok",
                "completed job=1 token=1",
                "result job=1 unknown",
                "rejected reason=unknown_job",
            ]
        );
    }

    #[test]
    fn checker_catches_result_corruption() {
        let done = || {
            let mut q = queue_with_lease();
            run(&mut q, &["@2 complete 1 1 ok"]);
            q
        };
        type Corrupt = fn(&mut ReferenceQueue);
        let corruptions: [(&str, Corrupt); 6] = [
            ("lost expiry index", |q| q.results_expiry.clear()),
            ("orphan expiry entry", |q| {
                q.results_expiry.insert((Time(50), JobId(1)));
            }),
            ("result of a live job", |q| {
                let r = q.results.remove(&JobId(1)).unwrap();
                q.results_expiry.clear();
                q.results_expiry.insert((r.2, JobId(2)));
                q.results.insert(JobId(2), r);
            }),
            ("token never issued", |q| {
                q.results.values_mut().for_each(|r| r.0 = Token(99));
            }),
            ("result past its window", |q| {
                q.clock.advance(Time(2).plus(RESULT_WINDOW));
                q.jobs.clear();
                q.leases.clear();
                q.queues.values_mut().for_each(|qs| qs.waiting.clear());
                q.acked = 2;
            }),
            ("more results than acks", |q| q.acked = 0),
        ];
        done().check_invariants().unwrap();
        for (name, corrupt) in corruptions {
            let mut q = done();
            corrupt(&mut q);
            assert!(q.check_invariants().is_err(), "{name} not detected");
        }
        // The snapshot keeps results, and refuses them out of order.
        let mut q = done();
        run(&mut q, &["@2 lease a 10", "@2 complete 2 2 two"]);
        let bytes = snapshot(&q);
        let back = ReferenceQueue::decode_state(3, &bytes).unwrap();
        assert_eq!(back.results(), q.results());
        // Each result here is job (8) + token (8) + payload (4 + 2 or 3) + expiry (8).
        let second = bytes.len() - (8 + 8 + 4 + 3 + 8);
        assert_eq!(bytes[second], 2);
        let mut swapped = bytes.clone();
        swapped[second] = 1;
        assert!(matches!(
            ReferenceQueue::decode_state(3, &swapped),
            Err(DecodeError::Invalid(m)) if m.contains("result of job 1 out of order")
        ));
    }

    #[test]
    fn snapshot_refuses_bad_key_tables() {
        let mut q = queue_with_lease();
        run(&mut q, &["@2 enqueue a z key=k1", "@2 enqueue a z key=k2"]);
        let bytes = snapshot(&q);
        // Each entry is name (2) + key (3) + job (8) + expires_at (8) = 21 bytes.
        let second_key = bytes.len() - 8 - 21 + 2 + 1;
        assert_eq!(&bytes[second_key..second_key + 2], b"k2");
        let mut swapped = bytes.clone();
        swapped[second_key + 1] = b'0';
        assert!(matches!(
            ReferenceQueue::decode_state(3, &swapped),
            Err(DecodeError::Invalid(m)) if m.contains("dedup key a/k0 out of order")
        ));
        // A key past its window is refused by the invariant checker.
        let mut stale = bytes.clone();
        let expires = bytes.len() - 16;
        stale[expires..expires + 8].copy_from_slice(&2u64.to_le_bytes());
        assert!(matches!(
            ReferenceQueue::decode_state(3, &stale),
            Err(DecodeError::Invalid(m)) if m.contains("expires at 2")
        ));
    }

    #[test]
    fn snapshot_decode_refuses_invalid_states() {
        let bytes = snapshot(&queue_with_lease());
        for len in 0..bytes.len() {
            assert!(
                ReferenceQueue::decode_state(3, &bytes[..len]).is_err(),
                "{len}"
            );
        }
        let mut long = bytes.clone();
        long.push(0);
        assert_eq!(
            ReferenceQueue::decode_state(3, &long).unwrap_err(),
            DecodeError::TrailingBytes(1)
        );
        // States whose bytes parse but break an invariant (D19).
        type Corrupt = fn(&mut ReferenceQueue);
        let corruptions: [(&str, Corrupt); 5] = [
            ("lost job", |q| {
                q.jobs.remove(&JobId(2));
            }),
            ("token from the future", |q| q.last_token = 0),
            ("lease at its deadline", |q| {
                q.clock.advance(Time(11));
            }),
            ("bad config", |q| {
                let qs = q.queues.get_mut(&QueueName::new("a").unwrap()).unwrap();
                qs.config.max_attempts = 0;
            }),
            ("job in a queue that does not exist", |q| {
                q.queues.clear();
            }),
        ];
        for (name, corrupt) in corruptions {
            let mut q = queue_with_lease();
            corrupt(&mut q);
            let err = ReferenceQueue::decode_state(3, &snapshot(&q));
            assert!(
                matches!(err, Err(DecodeError::Invalid(_))),
                "{name}: {err:?}"
            );
        }
        // Jobs must be in strictly increasing id order (header 36 bytes, queue
        // "a" 22 bytes, job count 8 bytes, then job 1's id).
        let mut swapped = bytes.clone();
        swapped[66] = 7;
        assert!(matches!(
            ReferenceQueue::decode_state(3, &swapped),
            Err(DecodeError::Invalid(m)) if m.contains("out of order")
        ));
        // A duplicate id is out of order too (the invariant checker would also
        // refuse it, as two index entries for one job, but less clearly).
        let mut duplicate = bytes.clone();
        let job2 = 66 + 8 + 2 + 5 + 4 + 1 + 16;
        assert_eq!(duplicate[job2], 2);
        duplicate[job2] = 1;
        assert!(matches!(
            ReferenceQueue::decode_state(3, &duplicate),
            Err(DecodeError::Invalid(m)) if m.contains("job 1 out of order")
        ));
        let mut bad_tag = bytes;
        let job1_state = 66 + 8 + 2 + 5 + 4;
        assert_eq!(bad_tag[job1_state], LEASED);
        bad_tag[job1_state] = 9;
        assert_eq!(
            ReferenceQueue::decode_state(3, &bad_tag).unwrap_err(),
            DecodeError::UnknownTag {
                what: "job state",
                tag: 9
            }
        );
    }
}
