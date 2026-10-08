//! The M5 world (D51): one queue server, an external fenced store, producers
//! and workers, under the faults of D52, checked as D53 says.
//!
//! ```text
//! producers ──enqueue (keyed)──┐                ┌── write(job, token, value) ──> store
//!                              ▼                │                              (FencedStore)
//!                         server (Core on a SimDisk) <── lease / heartbeat / complete ── workers
//! ```
//!
//! Node ids: the server is n0, the store n1, then the producers, then the
//! workers. Every queue message is one protocol frame (D32); request ids come
//! from one counter shared by every node, so a reply from a node's earlier
//! life can never be taken for a reply to a request of its current one.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::rc::Rc;

use super::{Ctx, MILLION, Message, Net, NodeId, Process, Rng, START, SimDisk, World, WorldStats};
use crate::command::{Command, Event, Op, ReleaseReason};
use crate::durable::{Durable, Options as DurableOptions, Recovery};
use crate::error::StoreError;
use crate::fence::FencedStore;
use crate::protocol::{self, Reply, Request};
use crate::queue::{Queue, Snapshot};
use crate::raft;
use crate::reference::{Counts, ReferenceQueue};
use crate::retry::QueueConfig;
use crate::server::{Core, ServerError};
use crate::storage::MemStorage;
use crate::types::{DedupKey, JobId, Lease, Millis, Payload, QueueName, Time, Token};

/// Lease length the workers ask for.
pub const VISIBILITY: Millis = Millis(1_000);
/// Faults are injected this long; then the world heals (D52).
pub const FAULT_PHASE: Millis = Millis(30_000);
/// After the fault phase, every job must be done within this.
pub const FINISH_WITHIN: Millis = Millis(60_000);
/// How long producers and workers wait for a reply before resending.
const TIMEOUT: Millis = Millis(250);
/// Small, so snapshots are taken (and torn) often.
const SNAPSHOT_EVERY: u64 = 64;
const MAX_BATCH: usize = 256;

const SERVER: NodeId = NodeId(0);

/// A bug planted on purpose, to show the simulator finds it (D54).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bug {
    /// The store accepts a write whatever its token.
    NoFence,
    /// Producers enqueue without a dedup key, so a retry adds a second job.
    NoDedupKey,
}

#[derive(Clone, Debug)]
pub struct Options {
    pub seed: u64,
    pub producers: u32,
    /// Jobs each producer enqueues.
    pub jobs: u64,
    pub workers: u32,
    pub bug: Option<Bug>,
    pub trace: bool,
}

impl Options {
    pub fn new(seed: u64) -> Self {
        Options {
            seed,
            producers: 2,
            jobs: 15,
            workers: 3,
            bug: None,
            trace: false,
        }
    }
}

/// Which faults this seed turned on, and how strong (D52).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Swarm {
    pub drop_ppm: u32,
    pub dup_ppm: u32,
    pub spike_ppm: u32,
    pub partitions: bool,
    pub crashes: bool,
    pub server_crashes: bool,
    pub torn_writes: bool,
    pub pauses: bool,
    pub clock_jumps: bool,
}

impl Swarm {
    fn pick(rng: &mut Rng) -> Swarm {
        let on = |rng: &mut Rng| rng.chance(MILLION / 2);
        let level = |rng: &mut Rng, max: u64| {
            if on(rng) { rng.range(1, max) as u32 } else { 0 }
        };
        Swarm {
            drop_ppm: level(rng, 200_000),
            dup_ppm: level(rng, 50_000),
            spike_ppm: level(rng, 20_000),
            partitions: on(rng),
            crashes: on(rng),
            server_crashes: on(rng),
            torn_writes: on(rng),
            pauses: on(rng),
            clock_jumps: on(rng),
        }
    }

    fn net(&self) -> Net {
        Net {
            drop_ppm: self.drop_ppm,
            dup_ppm: self.dup_ppm,
            spike_ppm: self.spike_ppm,
            spike_max: Millis(2_000),
            ..Net::default()
        }
    }

    fn faults(&self) -> Vec<Fault> {
        let mut faults = Vec::new();
        for (on, fault) in [
            (self.partitions, Fault::Partition),
            (self.crashes, Fault::Crash),
            (self.server_crashes, Fault::ServerCrash),
            (self.torn_writes, Fault::TornWrite),
            (self.pauses, Fault::Pause),
            (self.clock_jumps, Fault::ClockJump),
        ] {
            if on {
                faults.push(fault);
            }
        }
        faults
    }
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    Partition,
    Crash,
    ServerCrash,
    TornWrite,
    Pause,
    ClockJump,
}

/// What a run reached: the cases a test asserts were covered (D55).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Coverage {
    pub completions: u64,
    pub enqueue_retries: u64,
    pub deduplicated: u64,
    pub lease_retries: u64,
    pub write_retries: u64,
    /// Zombie writes the store's fence refused.
    pub writes_refused: u64,
    /// Stale writes accepted because of [`Bug::NoFence`].
    pub unfenced_accepts: u64,
    pub complete_retries: u64,
    /// Completes answered `Completed` after being sent more than once (D42).
    pub completed_after_retry: u64,
    pub completes_rejected: u64,
    pub heartbeats_rejected: u64,
    pub leases_expired: u64,
    pub recoveries: u64,
    pub recovered_from_snapshot: u64,
    pub torn_tails: u64,
    /// Recoveries that lost commands written but never replied to.
    pub unsynced_lost: u64,
    /// Recoveries that kept commands whose batch failed before its reply.
    pub unreplied_survived: u64,
    /// Recoveries that hit an armed disk failure and crashed again.
    pub recovery_failures: u64,
    /// Batches that failed on the disk and stopped the server (D38).
    pub disk_failures: u64,
    pub partitions: u64,
    pub client_crashes: u64,
    pub server_crashes: u64,
    pub torn_armed: u64,
    pub pauses: u64,
    pub clock_forward: u64,
    pub clock_back: u64,
    pub batches: u64,
    pub largest_batch: u64,
    /// Answers from a replicated queue (M7): redirects to a named leader,
    /// `not_leader` with no leader known, and `unknown` outcomes (D71).
    pub redirects: u64,
    pub not_leader: u64,
    pub unknown: u64,
}

impl Coverage {
    /// Field-wise sum, keeping the largest batch as a maximum.
    pub fn add(&mut self, o: &Coverage) {
        macro_rules! sum {
            ($($f:ident),*) => { $(self.$f += o.$f;)* };
        }
        sum!(
            completions,
            enqueue_retries,
            deduplicated,
            lease_retries,
            write_retries,
            writes_refused,
            unfenced_accepts,
            complete_retries,
            completed_after_retry,
            completes_rejected,
            heartbeats_rejected,
            leases_expired,
            recoveries,
            recovered_from_snapshot,
            torn_tails,
            unsynced_lost,
            unreplied_survived,
            recovery_failures,
            disk_failures,
            partitions,
            client_crashes,
            server_crashes,
            torn_armed,
            pauses,
            clock_forward,
            clock_back,
            batches,
            redirects,
            not_leader,
            unknown
        );
        self.largest_batch = self.largest_batch.max(o.largest_batch);
    }
}

#[derive(Clone, Debug)]
pub struct Report {
    pub seed: u64,
    /// The trace hash (D48).
    pub hash: u64,
    pub swarm: Swarm,
    pub world: WorldStats,
    pub coverage: Coverage,
    /// Simulated time from the start until every job was done.
    pub finished: Millis,
    pub trace: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct Failure {
    pub message: String,
    pub report: Box<Report>,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "seed {} failed: {}\n  swarm: {:?}\n  replay: cargo run -- sim --seed {} --trace",
            self.report.seed, self.message, self.report.swarm, self.report.seed
        )
    }
}

/// What travels in the queue world.
#[derive(Clone)]
pub enum Msg {
    /// One protocol frame (D32), to or from the server.
    Frame(Vec<u8>),
    /// A worker's effect, sent to the store.
    Write {
        req: u64,
        job: JobId,
        token: Token,
        value: Payload,
    },
    /// The store's answer: `ok` is false when the fence refused the write.
    Written { req: u64, ok: bool },
    /// Between the replicas of a replicated queue (M7).
    Raft(raft::Message),
}

impl Message for Msg {
    fn digest(&self) -> u64 {
        match self {
            Msg::Frame(bytes) => super::digest(bytes),
            Msg::Write {
                req,
                job,
                token,
                value,
            } => {
                let h = super::digest(&value.0);
                [*req, job.0, token.0]
                    .iter()
                    .fold(h, |h, w| super::fnv(h, &w.to_le_bytes()))
            }
            Msg::Written { req, ok } => {
                super::digest(&[&req.to_le_bytes()[..], &[*ok as u8]].concat())
            }
            Msg::Raft(m) => super::digest_of(m),
        }
    }
}

impl fmt::Debug for Msg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Msg::Frame(bytes) => match protocol::decode_frame(bytes) {
                Ok(frame) => match (frame.request(), frame.reply()) {
                    (Ok(Request::Op(op)), _) => {
                        let text = Command { at: Time(0), op }.to_string();
                        write!(f, "#{} {}", frame.id, text.trim_start_matches("@0 "))
                    }
                    (_, Ok(Reply::Events(events))) => {
                        let events: Vec<String> = events.iter().map(Event::to_string).collect();
                        write!(f, "#{} -> {}", frame.id, events.join("; "))
                    }
                    (_, Ok(Reply::NotLeader { leader })) => {
                        write!(f, "#{} -> not leader, leader {leader:?}", frame.id)
                    }
                    (_, Ok(Reply::Unknown)) => write!(f, "#{} -> unknown", frame.id),
                    (req, reply) => write!(f, "#{} {req:?} {reply:?}", frame.id),
                },
                Err(e) => write!(f, "bad frame: {e}"),
            },
            Msg::Write {
                req,
                job,
                token,
                value,
            } => write!(f, "#{req} write job={job} token={token} value={value}"),
            Msg::Written { req, ok } => write!(f, "#{req} -> written ok={ok}"),
            Msg::Raft(m) => write!(f, "{:?}", super::raft::Msg::Raft(m.clone())),
        }
    }
}

/// What the processes share with the run: the server's history, the store,
/// the producers' answers and the first broken check.
pub(super) struct Shared {
    pub(super) bug: Option<Bug>,
    /// The queue's servers: one, or the replicas of a cluster (M7), whose
    /// Raft ids are their indexes here.
    pub(super) servers: Vec<NodeId>,
    pub(super) store: NodeId,
    next_req: u64,
    /// Every command the server tried to log, in log order: command `i` has
    /// LSN `i + 1`. Commands of a failed batch stay until recovery decides.
    pub(super) history: Vec<Command>,
    /// The last LSN in a batch the server answered: durable (D22).
    pub(super) replied: u64,
    /// The queue's counts after the last batch.
    pub(super) counts: Counts,
    /// The job id each producer key got.
    pub(super) keys: BTreeMap<String, JobId>,
    pub(super) producers_done: BTreeSet<u32>,
    pub(super) fence: FencedStore<JobId, Payload>,
    /// The store's contents: what the fence let through.
    pub(super) values: BTreeMap<JobId, (Token, Payload)>,
    /// Every write the store accepted, in order.
    pub(super) accepted: Vec<(JobId, Token)>,
    pub(super) failure: Option<String>,
    pub(super) cov: Coverage,
}

pub(super) type Sh = Rc<RefCell<Shared>>;

impl Shared {
    pub(super) fn new(bug: Option<Bug>, servers: Vec<NodeId>, store: NodeId) -> Self {
        Shared {
            bug,
            servers,
            store,
            next_req: 0,
            history: Vec::new(),
            replied: 0,
            counts: Counts::default(),
            keys: BTreeMap::new(),
            producers_done: BTreeSet::new(),
            fence: FencedStore::new(),
            values: BTreeMap::new(),
            accepted: Vec::new(),
            failure: None,
            cov: Coverage::default(),
        }
    }

    pub(super) fn req(&mut self) -> u64 {
        self.next_req += 1;
        self.next_req
    }

    pub(super) fn fail(&mut self, message: String) {
        self.failure.get_or_insert(message);
    }
}

pub(super) fn queue_name() -> QueueName {
    QueueName::new("jobs").unwrap()
}

fn frame(id: u64, op: Op) -> Msg {
    let mut bytes = Vec::new();
    protocol::encode_request(id, &Request::Op(op), &mut bytes);
    Msg::Frame(bytes)
}

/// What a server answered a request.
enum Answer {
    Events(Vec<Event>),
    /// From a replica that does not lead (D71): the leader's Raft id if known.
    NotLeader(Option<u32>),
    /// The outcome is unknown; resending is safe (D72).
    Unknown,
}

/// The reply id and answer in a frame from a server.
fn answer(msg: &Msg) -> Option<(u64, Answer)> {
    let Msg::Frame(bytes) = msg else {
        return None;
    };
    let frame = protocol::decode_frame(bytes).ok()?;
    let answer = match frame.reply().ok()? {
        Reply::Events(events) => Answer::Events(events),
        Reply::NotLeader { leader } => Answer::NotLeader(leader),
        Reply::Unknown => Answer::Unknown,
        _ => return None,
    };
    Some((frame.id, answer))
}

/// Which server a client sends to, and how it moves on (D71): to a named
/// leader at once, otherwise to the next server.
#[derive(Clone, Copy, Debug, Default)]
struct Route {
    target: u32,
}

impl Route {
    fn server(&self, sh: &Shared) -> NodeId {
        sh.servers[self.target as usize]
    }

    fn next(&mut self, sh: &Shared) {
        self.target = (self.target + 1) % sh.servers.len() as u32;
    }

    /// Follow a non-event answer. True if the request should be resent now:
    /// to a named leader, or after `unknown`. Without a known leader the
    /// client moves on and waits for its timeout, so a cluster with no leader
    /// is not flooded.
    fn follow(&mut self, sh: &mut Shared, answer: &Answer) -> bool {
        match *answer {
            Answer::Events(_) => false,
            Answer::NotLeader(Some(l)) if l != self.target => {
                sh.cov.redirects += 1;
                self.target = l;
                true
            }
            Answer::NotLeader(_) => {
                sh.cov.not_leader += 1;
                self.next(sh);
                false
            }
            Answer::Unknown => {
                sh.cov.unknown += 1;
                true
            }
        }
    }
}

/// The value a lease writes and completes with: whose effect it is.
fn effect(lease: &Lease) -> Payload {
    Payload(format!("{}:{}", lease.job, lease.token).into_bytes())
}

// ---------------------------------------------------------------- server

/// The queue server (D51): `Core` over the node's disk, batching what arrives
/// while a sync is in progress.
struct Server {
    sh: Sh,
    disk: SimDisk,
    core: Option<Core<SimDisk>>,
    pending: Vec<(NodeId, u64, Op)>,
    /// The batch being synced, all stamped `batch_at`.
    batch: Vec<(NodeId, u64, Op)>,
    batch_at: Time,
}

impl Server {
    /// Check a recovery against the history (D53): it kept every answered
    /// command, nothing never written, and equals a replay of what it kept.
    fn recovered(&mut self, core: &Core<SimDisk>, recovery: Recovery) {
        let mut sh = self.sh.borrow_mut();
        let k = core.durable().next_lsn() - 1;
        let (replied, written) = (sh.replied, sh.history.len() as u64);
        if written == 0 {
            return;
        }
        sh.cov.recoveries += 1;
        sh.cov.recovered_from_snapshot += u64::from(recovery.snapshot_lsn > 0);
        sh.cov.torn_tails += u64::from(recovery.truncated > 0);
        if k < replied {
            return sh.fail(format!(
                "recovery lost answered commands: kept {k}, answered up to {replied}"
            ));
        }
        if k > written {
            return sh.fail(format!(
                "recovery found {k} commands, only {written} were written"
            ));
        }
        sh.cov.unsynced_lost += u64::from(k < written);
        sh.cov.unreplied_survived += u64::from(k > replied);
        sh.history.truncate(k as usize);
        sh.replied = k;
        let mut replay = ReferenceQueue::new();
        let mut out = Vec::new();
        for cmd in &sh.history {
            replay.apply(cmd, &mut out);
            out.clear();
        }
        let (mut a, mut b) = (Vec::new(), Vec::new());
        replay.encode_state(&mut a);
        core.durable().queue().encode_state(&mut b);
        if a != b {
            sh.fail(format!(
                "recovered state differs from a replay of its {k} commands"
            ));
        }
    }

    fn start_batch(&mut self, ctx: &mut Ctx<'_, Msg>) {
        if self.pending.is_empty() || !self.batch.is_empty() {
            return;
        }
        let n = self.pending.len().min(MAX_BATCH);
        self.batch = self.pending.drain(..n).collect();
        // One clock read per batch (D31).
        self.batch_at = ctx.now();
        let sync = Millis(ctx.rng().range(1, 5));
        ctx.set_timer(sync, 0);
    }

    fn finish_batch(&mut self, ctx: &mut Ctx<'_, Msg>) {
        let Some(core) = &mut self.core else {
            return;
        };
        let at = self.batch_at;
        let ops: Vec<Op> = self.batch.iter().map(|(_, _, op)| op.clone()).collect();
        self.sh
            .borrow_mut()
            .history
            .extend(ops.iter().map(|op| Command { at, op: op.clone() }));
        match core.apply(at, ops) {
            Ok(events) => {
                let mut sh = self.sh.borrow_mut();
                let lsn = core.durable().next_lsn() - 1;
                let written = sh.history.len() as u64;
                if lsn != written {
                    sh.fail(format!("log at LSN {lsn}, history at {written}"));
                }
                sh.replied = lsn;
                sh.counts = core.durable().queue().counts();
                sh.cov.batches += 1;
                sh.cov.largest_batch = sh.cov.largest_batch.max(self.batch.len() as u64);
                for e in events.iter().flatten() {
                    match e {
                        Event::Released {
                            reason: ReleaseReason::Expired,
                            ..
                        } => sh.cov.leases_expired += 1,
                        Event::Completed { .. } => sh.cov.completions += 1,
                        _ => {}
                    }
                }
                drop(sh);
                for ((to, id, _), events) in self.batch.drain(..).zip(events) {
                    let mut bytes = Vec::new();
                    protocol::encode_reply(id, &Reply::Events(events), &mut bytes);
                    ctx.send(to, Msg::Frame(bytes));
                }
                self.start_batch(ctx);
            }
            Err(ServerError::Invariant(e)) => self.sh.borrow_mut().fail(e),
            // The disk failed: stop, as the real core thread does (D38).
            Err(_) => {
                self.sh.borrow_mut().cov.disk_failures += 1;
                ctx.halt();
            }
        }
    }
}

impl Process<Msg> for Server {
    fn start(&mut self, ctx: &mut Ctx<'_, Msg>) {
        let options = DurableOptions {
            snapshot_every: SNAPSHOT_EVERY,
        };
        match Core::open(self.disk.clone(), options, true) {
            Ok((core, recovery)) => {
                self.recovered(&core, recovery);
                self.core = Some(core);
            }
            // A failure armed while the server was down hit recovery itself:
            // crash again, which tests a crash during recovery.
            Err(StoreError::Io(_)) => {
                self.sh.borrow_mut().cov.recovery_failures += 1;
                ctx.halt();
            }
            Err(e) => self.sh.borrow_mut().fail(format!("recovery failed: {e}")),
        }
    }

    fn receive(&mut self, ctx: &mut Ctx<'_, Msg>, from: NodeId, msg: Msg) {
        let Msg::Frame(bytes) = msg else { return };
        match protocol::decode_frame(&bytes).and_then(|f| Ok((f.id, f.request()?))) {
            Ok((id, Request::Op(op))) => {
                self.pending.push((from, id, op));
                self.start_batch(ctx);
            }
            other => self
                .sh
                .borrow_mut()
                .fail(format!("server got a bad frame from {from}: {other:?}")),
        }
    }

    fn timer(&mut self, ctx: &mut Ctx<'_, Msg>, _: u64) {
        self.finish_batch(ctx);
    }
}

// ---------------------------------------------------------------- store

/// The external store (D43, D51): a fenced write per job, over the network.
pub(super) struct Store {
    pub(super) sh: Sh,
}

impl Process<Msg> for Store {
    fn start(&mut self, _: &mut Ctx<'_, Msg>) {}

    fn receive(&mut self, ctx: &mut Ctx<'_, Msg>, from: NodeId, msg: Msg) {
        let Msg::Write {
            req,
            job,
            token,
            value,
        } = msg
        else {
            return;
        };
        let mut sh = self.sh.borrow_mut();
        let fenced = sh.fence.write(job, token, value.clone()).is_ok();
        let ok = fenced || sh.bug == Some(Bug::NoFence);
        if ok {
            sh.cov.unfenced_accepts += u64::from(!fenced);
            sh.values.insert(job, (token, value));
            sh.accepted.push((job, token));
        } else {
            sh.cov.writes_refused += 1;
        }
        drop(sh);
        ctx.send(from, Msg::Written { req, ok });
    }

    fn timer(&mut self, _: &mut Ctx<'_, Msg>, _: u64) {}
}

// ---------------------------------------------------------------- producer

/// Configures the queue, then enqueues its jobs one at a time, each with a
/// dedup key (D35), resending until it has the job id, with a pause of up to
/// 3 s between jobs so the work spans the fault phase. After a crash it starts
/// over from the first job: the keys make that safe.
pub(super) struct Producer {
    sh: Sh,
    index: u32,
    jobs: u64,
    route: Route,
    /// 0: configure; `1..=jobs`: enqueue job `next`.
    next: u64,
    /// The request waiting for its reply, 0 between jobs.
    req: u64,
    gap_timer: u64,
}

impl Producer {
    fn key(&self) -> String {
        format!("p{}-{}", self.index, self.next)
    }

    fn send(&mut self, ctx: &mut Ctx<'_, Msg>) {
        let mut sh = self.sh.borrow_mut();
        self.req = sh.req();
        let op = if self.next == 0 {
            Op::Configure {
                queue: queue_name(),
                config: QueueConfig {
                    max_attempts: 1_000,
                    backoff_base: Millis(10),
                    backoff_cap: Millis(100),
                },
            }
        } else {
            let key = self.key();
            Op::Enqueue {
                queue: queue_name(),
                payload: Payload(key.clone().into_bytes()),
                delay: Millis(0),
                key: (sh.bug != Some(Bug::NoDedupKey)).then(|| DedupKey::new(&key).unwrap()),
            }
        };
        let to = self.route.server(&sh);
        drop(sh);
        ctx.send(to, frame(self.req, op));
        ctx.set_timer(TIMEOUT, self.req);
    }

    pub(super) fn new(sh: Sh, index: u32, jobs: u64) -> Self {
        Producer {
            sh,
            index,
            jobs,
            route: Route::default(),
            next: 0,
            req: 0,
            gap_timer: 0,
        }
    }
}

impl Process<Msg> for Producer {
    fn start(&mut self, ctx: &mut Ctx<'_, Msg>) {
        self.send(ctx);
    }

    fn receive(&mut self, ctx: &mut Ctx<'_, Msg>, _: NodeId, msg: Msg) {
        let Some((id, answer)) = answer(&msg) else {
            return;
        };
        if id != self.req || self.next > self.jobs {
            return;
        }
        let mut sh = self.sh.borrow_mut();
        let Answer::Events(events) = answer else {
            if self.route.follow(&mut sh, &answer) {
                drop(sh);
                self.send(ctx);
            }
            return;
        };
        for e in &events {
            match e {
                Event::Configured { .. } if self.next == 0 => self.next = 1,
                Event::Enqueued { job, .. } | Event::Deduplicated { job, .. } if self.next > 0 => {
                    sh.cov.deduplicated += u64::from(matches!(e, Event::Deduplicated { .. }));
                    let key = self.key();
                    let first = *sh.keys.entry(key.clone()).or_insert(*job);
                    if first != *job {
                        sh.fail(format!("key {key} got job {first} and then job {job}"));
                    }
                    self.next += 1;
                }
                _ => continue,
            }
            self.req = 0;
            if self.next > self.jobs {
                sh.producers_done.insert(self.index);
                return;
            }
            self.gap_timer = sh.req();
            drop(sh);
            let gap = Millis(ctx.rng().range(0, 3_000));
            return ctx.set_timer(gap, self.gap_timer);
        }
    }

    fn timer(&mut self, ctx: &mut Ctx<'_, Msg>, id: u64) {
        if id == self.gap_timer {
            self.send(ctx);
        } else if id == self.req && self.req != 0 {
            let mut sh = self.sh.borrow_mut();
            sh.cov.enqueue_retries += 1;
            self.route.next(&sh);
            drop(sh);
            self.send(ctx);
        }
    }
}

// ---------------------------------------------------------------- worker

#[derive(Clone, Debug)]
enum Doing {
    /// Waiting for the poll timer.
    Idle,
    Leasing,
    /// Running the job until the work timer.
    Working(Lease),
    /// Waiting for the store to take the effect.
    Writing(Lease),
    /// Waiting for the queue to take the complete; `tries` sends so far.
    Completing(Lease, u32),
}

/// The worker loop of D36/D44: lease, heartbeat every visibility/3, effect
/// to the store with the lease token, then complete with that effect as the
/// result, resending the complete with the same token on timeout (D42).
/// Every timer gets a fresh id, and the worker remembers which ids it still
/// expects, so timers and replies left over from an abandoned job do nothing.
pub(super) struct Worker {
    sh: Sh,
    route: Route,
    doing: Doing,
    /// The request the worker waits on: lease, write or complete.
    req: u64,
    heartbeat: u64,
    work_timer: u64,
    poll_timer: u64,
    heartbeat_timer: u64,
}

impl Worker {
    fn id(&self) -> u64 {
        self.sh.borrow_mut().req()
    }

    pub(super) fn new(sh: Sh) -> Self {
        Worker {
            sh,
            route: Route::default(),
            doing: Doing::Idle,
            req: 0,
            heartbeat: 0,
            work_timer: 0,
            poll_timer: 0,
            heartbeat_timer: 0,
        }
    }

    fn server(&self) -> NodeId {
        self.route.server(&self.sh.borrow())
    }

    fn request(&mut self, ctx: &mut Ctx<'_, Msg>, op: Op) {
        self.req = self.id();
        ctx.send(self.server(), frame(self.req, op));
        ctx.set_timer(TIMEOUT, self.req);
    }

    /// Resend the request in flight, after a timeout or an answer that is not
    /// an outcome (D72): a lease asks again (and may orphan the first one), a
    /// write or a complete goes again with the same token.
    fn resend(&mut self, ctx: &mut Ctx<'_, Msg>) {
        match self.doing {
            Doing::Leasing => {
                self.sh.borrow_mut().cov.lease_retries += 1;
                self.poll(ctx);
            }
            Doing::Writing(lease) => {
                self.sh.borrow_mut().cov.write_retries += 1;
                self.write(ctx, lease);
            }
            Doing::Completing(lease, tries) => {
                self.sh.borrow_mut().cov.complete_retries += 1;
                self.doing = Doing::Completing(lease, tries + 1);
                self.complete(ctx, lease);
            }
            Doing::Idle | Doing::Working(_) => {}
        }
    }

    fn poll(&mut self, ctx: &mut Ctx<'_, Msg>) {
        self.doing = Doing::Leasing;
        self.heartbeat = 0;
        self.request(
            ctx,
            Op::Lease {
                queue: queue_name(),
                visibility: VISIBILITY,
            },
        );
    }

    fn idle(&mut self, ctx: &mut Ctx<'_, Msg>) {
        self.doing = Doing::Idle;
        self.poll_timer = self.id();
        let backoff = Millis(ctx.rng().range(20, 200));
        ctx.set_timer(backoff, self.poll_timer);
    }

    fn write(&mut self, ctx: &mut Ctx<'_, Msg>, lease: Lease) {
        self.req = self.id();
        let msg = Msg::Write {
            req: self.req,
            job: lease.job,
            token: lease.token,
            value: effect(&lease),
        };
        let store = self.sh.borrow().store;
        ctx.send(store, msg);
        ctx.set_timer(TIMEOUT, self.req);
    }

    fn complete(&mut self, ctx: &mut Ctx<'_, Msg>, lease: Lease) {
        let op = Op::Complete {
            job: lease.job,
            token: lease.token,
            result: effect(&lease),
        };
        self.request(ctx, op);
    }

    fn on_reply(&mut self, ctx: &mut Ctx<'_, Msg>, events: &[Event]) {
        match self.doing.clone() {
            Doing::Leasing => {
                for e in events {
                    match e {
                        Event::Leased { lease, .. } => {
                            self.doing = Doing::Working(*lease);
                            // Mostly short jobs; some outlive a lease and
                            // need their heartbeats.
                            let work = if ctx.rng().chance(MILLION / 20) {
                                ctx.rng().range(VISIBILITY.0, 3 * VISIBILITY.0)
                            } else {
                                ctx.rng().range(10, 300)
                            };
                            self.work_timer = self.id();
                            ctx.set_timer(Millis(work), self.work_timer);
                            self.heartbeat_timer = self.id();
                            ctx.set_timer(Millis(VISIBILITY.0 / 3), self.heartbeat_timer);
                            return;
                        }
                        Event::Empty { .. } => return self.idle(ctx),
                        _ => {}
                    }
                }
            }
            Doing::Completing(lease, tries) => {
                for e in events {
                    match e {
                        Event::Completed { job, .. } if *job == lease.job => {
                            let mut sh = self.sh.borrow_mut();
                            sh.cov.completed_after_retry += u64::from(tries > 1);
                            drop(sh);
                            return self.poll(ctx);
                        }
                        Event::Rejected { .. } => {
                            self.sh.borrow_mut().cov.completes_rejected += 1;
                            return self.poll(ctx);
                        }
                        _ => {}
                    }
                }
            }
            Doing::Idle | Doing::Working(_) | Doing::Writing(_) => {}
        }
    }

    fn on_heartbeat(&mut self, ctx: &mut Ctx<'_, Msg>, events: &[Event]) {
        if events.iter().any(|e| matches!(e, Event::Rejected { .. }))
            && matches!(self.doing, Doing::Working(_) | Doing::Writing(_))
        {
            // The lease is gone: drop the job (D36).
            self.sh.borrow_mut().cov.heartbeats_rejected += 1;
            self.poll(ctx);
        }
    }
}

impl Process<Msg> for Worker {
    fn start(&mut self, ctx: &mut Ctx<'_, Msg>) {
        self.poll(ctx);
    }

    fn receive(&mut self, ctx: &mut Ctx<'_, Msg>, _: NodeId, msg: Msg) {
        if let Msg::Written { req, ok } = msg {
            if req != self.req {
                return;
            }
            if let Doing::Writing(lease) = self.doing {
                if ok {
                    self.doing = Doing::Completing(lease, 1);
                    self.complete(ctx, lease);
                } else {
                    // A newer lease wrote first: ours is gone.
                    self.poll(ctx);
                }
            }
            return;
        }
        let Some((id, answer)) = answer(&msg) else {
            return;
        };
        let Answer::Events(events) = answer else {
            let mine = id == self.req || (id == self.heartbeat && self.heartbeat != 0);
            if mine {
                let now = self.route.follow(&mut self.sh.borrow_mut(), &answer);
                // A heartbeat is not resent: the next one goes to the new target.
                if now && id == self.req {
                    self.resend(ctx);
                }
            }
            return;
        };
        if id == self.req {
            self.on_reply(ctx, &events);
        } else if id == self.heartbeat && self.heartbeat != 0 {
            self.on_heartbeat(ctx, &events);
        }
    }

    fn timer(&mut self, ctx: &mut Ctx<'_, Msg>, id: u64) {
        if id == self.poll_timer && matches!(self.doing, Doing::Idle) {
            self.poll(ctx);
        } else if id == self.work_timer
            && let Doing::Working(lease) = self.doing
        {
            self.doing = Doing::Writing(lease);
            self.write(ctx, lease);
        } else if id == self.heartbeat_timer
            && let Doing::Working(lease) | Doing::Writing(lease) = self.doing
        {
            self.heartbeat = self.id();
            let op = Op::Heartbeat {
                job: lease.job,
                token: lease.token,
                visibility: VISIBILITY,
            };
            ctx.send(self.server(), frame(self.heartbeat, op));
            self.heartbeat_timer = self.id();
            ctx.set_timer(Millis(VISIBILITY.0 / 3), self.heartbeat_timer);
        } else if id == self.req {
            // The request timed out: try the next server (the store is one
            // node) and resend.
            if !matches!(self.doing, Doing::Writing(_)) {
                let sh = self.sh.borrow();
                let mut route = self.route;
                route.next(&sh);
                drop(sh);
                self.route = route;
            }
            self.resend(ctx);
        }
    }
}

// ---------------------------------------------------------------- the run

/// Run one seed: build the world, inject faults for `FAULT_PHASE`, heal, wait
/// for every job to finish, then check the end state (D53).
pub fn run(options: &Options) -> Result<Report, Failure> {
    let sh: Sh = Rc::new(RefCell::new(Shared::new(
        options.bug,
        vec![SERVER],
        NodeId(1),
    )));
    let mut world = World::<Msg>::new(options.seed);
    if options.trace {
        world.enable_trace();
    }
    let s = sh.clone();
    world.add(Box::new(move |_, disk| {
        Box::new(Server {
            sh: s.clone(),
            disk,
            core: None,
            pending: Vec::new(),
            batch: Vec::new(),
            batch_at: Time(0),
        })
    }));
    let s = sh.clone();
    world.add(Box::new(move |_, _| Box::new(Store { sh: s.clone() })));
    let mut clients = Vec::new();
    for index in 0..options.producers {
        let (s, jobs) = (sh.clone(), options.jobs);
        clients.push(world.add(Box::new(move |_, _| {
            Box::new(Producer::new(s.clone(), index, jobs))
        })));
    }
    for _ in 0..options.workers {
        let s = sh.clone();
        clients.push(world.add(Box::new(move |_, _| Box::new(Worker::new(s.clone())))));
    }
    let nodes = 2 + clients.len() as u32;

    let swarm = Swarm::pick(world.rng());
    world.net = swarm.net();
    let faults = swarm.faults();
    let quiet_at = START.plus(FAULT_PHASE);
    let deadline = quiet_at.plus(FINISH_WITHIN);
    let mut next_fault = START.plus(Millis(world.rng().range(100, 2_000)));
    let mut heal_at: Option<Time> = None;
    let mut quiet = false;

    let outcome = loop {
        let mut until = world.now().plus(Millis(100));
        if !quiet {
            until = until.min(next_fault).min(quiet_at);
        }
        if let Some(t) = heal_at {
            until = until.min(t);
        }
        world.run_until(until);
        let now = world.now();
        if let Some(e) = sh.borrow_mut().failure.take() {
            break Err(e);
        }
        if heal_at.is_some_and(|t| t <= now) {
            world.heal();
            heal_at = None;
        }
        if !quiet && now >= quiet_at {
            quiet = true;
            world.heal();
            heal_at = None;
            world.net = Net::default();
        }
        if !quiet && now >= next_fault {
            if !faults.is_empty() {
                let fault = *world.rng().pick(&faults);
                inject(&mut world, &sh, fault, &clients, nodes, &mut heal_at);
            }
            next_fault = now.plus(Millis(world.rng().range(100, 2_000)));
        }
        if quiet && done(&sh, options) && world.is_up(SERVER) {
            break check_end(&world, &sh);
        }
        if now >= deadline {
            let s = sh.borrow();
            break Err(format!(
                "not finished {} ms after the faults stopped: producers done {}/{}, counts {:?}",
                FINISH_WITHIN.0,
                s.producers_done.len(),
                options.producers,
                s.counts
            ));
        }
    };
    let report = Report {
        seed: options.seed,
        hash: world.hash(),
        swarm,
        world: world.stats(),
        coverage: sh.borrow().cov,
        finished: Millis(world.now().0 - START.0),
        trace: world.take_trace(),
    };
    match outcome {
        Ok(()) => Ok(report),
        Err(message) => Err(Failure {
            message,
            report: Box::new(report),
        }),
    }
}

fn inject(
    world: &mut World<Msg>,
    sh: &Sh,
    fault: Fault,
    clients: &[NodeId],
    nodes: u32,
    heal_at: &mut Option<Time>,
) {
    let mut cov = sh.borrow().cov;
    let now = world.now();
    match fault {
        Fault::Partition => {
            if heal_at.is_none() {
                let node = NodeId(world.rng().below(u64::from(nodes)) as u32);
                world.isolate(node);
                *heal_at = Some(now.plus(Millis(world.rng().range(100, 3_000))));
                cov.partitions += 1;
            }
        }
        Fault::Crash => {
            let node = *world.rng().pick(clients);
            if world.is_up(node) {
                world.crash(node);
                let after = Millis(world.rng().range(50, 2_000));
                world.restart_after(node, after);
                cov.client_crashes += 1;
            }
        }
        Fault::ServerCrash => {
            if world.is_up(SERVER) {
                world.crash(SERVER);
                let after = Millis(world.rng().range(50, 2_000));
                world.restart_after(SERVER, after);
                cov.server_crashes += 1;
            }
        }
        Fault::TornWrite => {
            let calls = world.rng().range(0, 6);
            world.disk(SERVER).fail_in(calls);
            cov.torn_armed += 1;
        }
        Fault::Pause => {
            let node = NodeId(world.rng().below(u64::from(nodes)) as u32);
            let until = now.plus(Millis(world.rng().range(100, 3 * VISIBILITY.0)));
            world.pause(node, until);
            cov.pauses += 1;
        }
        Fault::ClockJump => {
            // Forward expires leases early; back is clamped by D9 and freezes
            // the queue's clock for as long as the step, so it stays short.
            if world.rng().chance(MILLION / 2) {
                let ms = world.rng().range(1, 2 * VISIBILITY.0);
                world.shift_clock(SERVER, ms as i64);
                cov.clock_forward += 1;
            } else {
                let ms = world.rng().range(1, 5_000);
                world.shift_clock(SERVER, -(ms as i64));
                cov.clock_back += 1;
            }
        }
    }
    sh.borrow_mut().cov = cov;
}

/// Every producer has its ids and the queue holds no job.
fn done(sh: &Sh, options: &Options) -> bool {
    producers_done(&sh.borrow(), options.producers)
}

pub(super) fn producers_done(s: &Shared, producers: u32) -> bool {
    let c = s.counts;
    s.producers_done.len() == producers as usize && c.waiting + c.leased + c.dead == 0
}

/// The end-of-run checks of D53, on a queue recovered from the server's disk.
fn check_end(world: &World<Msg>, sh: &Sh) -> Result<(), String> {
    let files = world.disk(SERVER).files();
    let options = DurableOptions { snapshot_every: 0 };
    let (durable, _) = Durable::<MemStorage>::open(MemStorage::from_files(files), options)
        .map_err(|e| format!("final recovery failed: {e}"))?;
    check_final(durable.queue(), &sh.borrow())
}

/// The end state of D53: an empty queue, one completed job per key, each
/// job's result equal to the store's value by the completing token, and the
/// store's tokens per job never going back.
pub(super) fn check_final(q: &ReferenceQueue, s: &Shared) -> Result<(), String> {
    let c = q.counts();
    if c.waiting + c.leased + c.dead != 0 {
        return Err(format!("jobs left in the queue: {c:?}"));
    }
    let jobs: BTreeSet<JobId> = s.keys.values().copied().collect();
    if c.acked != jobs.len() as u64 {
        return Err(format!(
            "{} jobs completed for {} keys: a retry added a job",
            c.acked,
            jobs.len()
        ));
    }
    let results: BTreeMap<JobId, (Token, Payload)> = q
        .results()
        .into_iter()
        .map(|(job, token, payload, _)| (job, (token, payload)))
        .collect();
    if results.len() != jobs.len() {
        return Err(format!("{} results for {} jobs", results.len(), jobs.len()));
    }
    for job in &jobs {
        let Some((token, payload)) = results.get(job) else {
            return Err(format!("job {job} has no result"));
        };
        match s.values.get(job) {
            Some((t, v)) if t == token && v == payload => {}
            Some((t, _)) => {
                return Err(format!(
                    "job {job} completed by token {token}, but the store holds the effect of token {t}"
                ));
            }
            None => return Err(format!("job {job} completed with no effect in the store")),
        }
    }
    let mut last: BTreeMap<JobId, Token> = BTreeMap::new();
    for &(job, token) in &s.accepted {
        let prev = last.entry(job).or_insert(token);
        if token < *prev {
            return Err(format!(
                "the store accepted token {token} for job {job} after token {prev}"
            ));
        }
        *prev = token;
    }
    Ok(())
}
