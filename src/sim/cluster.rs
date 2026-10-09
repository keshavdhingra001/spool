//! The M7 world (D73), partitioned in M8 (D82): the M5 producers, workers
//! and fenced store against a replicated queue of 3 or 5 nodes, each running
//! one [`Replica`] of each of 1 to 3 partitions, under the faults of D52 and
//! D63.
//!
//! ```text
//! producers ──enqueue (keyed, some ordered, some fanned out)──┐   not_leader / unknown:
//!                                                             ▼   retry (D71, D72)
//!            n0 ◄──raft(p)──► n1 ◄──raft(p)──► n2      a Replica per partition p,
//!                              ▲                       logs p<p>- on the node's SimDisk
//! workers ── lease / heartbeat / complete ──┘      ── write(job, token) ──> store
//! ```
//!
//! Node ids: the nodes are n0.. in the order of their Raft ids, then the
//! store, the producers and the workers. Producers route by key (D76) and
//! workers lease from every queue and partition in turn (D78). Checks, live,
//! per partition: every replica applies the same entry at each index and
//! reaches the same queue state after it (the replica itself runs both
//! checkers after every command, unless turned off), no two leaders share a
//! term, and every answer fits its op. At the end every partition's queue is
//! rebuilt from n0's log; the checks of D53 run on them together, and the
//! applied events are checked against D79: per ordering key, one lease at a
//! time and completions in enqueue order.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::rc::Rc;

use super::queue::{self, Kind, Msg, Producer, Sh, Shared, Store, Worker};
use super::{Ctx, MILLION, Net, NodeId, Process, Rng, START, SimDisk, World, WorldStats};
use crate::command::{Command, Event, Op, ReleaseReason};
use crate::error::StoreError;
use crate::protocol::{self, Reply as WireReply, Request};
use crate::queue::{Queue, Snapshot};
use crate::raft::Id;
use crate::raft::store::RaftLog;
use crate::reference::{Counts, ReferenceQueue};
use crate::replica::{self, Applied, Bug, Output, Replica, ReplicaError, Reply};
use crate::storage::{MemStorage, Prefixed};
use crate::types::{JobId, Millis, OrderKey, QueueName, Time};

pub const FAULT_PHASE: Millis = queue::FAULT_PHASE;
pub const FINISH_WITHIN: Millis = queue::FINISH_WITHIN;
/// How often a leader sends appends, and the election timeout range (D59).
const HEARTBEAT: Millis = Millis(50);
const ELECTION: (Millis, Millis) = (Millis(150), Millis(300));
const HEARTBEAT_TIMER: u64 = 0;
const MAX_BATCH: usize = 256;

#[derive(Clone, Debug)]
pub struct Options {
    pub seed: u64,
    /// Cluster size; `None` lets the seed pick 3 or 5.
    pub nodes: Option<u32>,
    /// Partitions (D74); `None` lets the seed pick 1 to 3.
    pub partitions: Option<u16>,
    /// Whether every replica runs both checkers after every command (D19).
    /// Off, only the world's own checks remain (D82).
    pub checks: bool,
    pub producers: u32,
    pub jobs: u64,
    pub workers: u32,
    /// A bug in the producers or the store (D54).
    pub queue_bug: Option<queue::Bug>,
    /// A bug in the replicas (D73).
    pub bug: Option<Bug>,
    pub trace: bool,
}

impl Options {
    pub fn new(seed: u64) -> Self {
        Options {
            seed,
            nodes: None,
            partitions: None,
            checks: true,
            producers: 3,
            jobs: 15,
            workers: 3,
            queue_bug: None,
            bug: None,
            trace: false,
        }
    }
}

/// Which faults this seed turned on (D52, D63).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Swarm {
    pub nodes: u32,
    /// Queue partitions, each a Raft group (D74).
    pub queue_partitions: u16,
    pub drop_ppm: u32,
    pub dup_ppm: u32,
    pub spike_ppm: u32,
    pub partitions: bool,
    pub one_way_cuts: bool,
    pub client_crashes: bool,
    pub replica_crashes: bool,
    pub torn_writes: bool,
    pub pauses: bool,
    pub clock_jumps: bool,
}

impl Swarm {
    fn pick(rng: &mut Rng, nodes: Option<u32>, partitions: Option<u16>) -> Swarm {
        let on = |rng: &mut Rng| rng.chance(MILLION / 2);
        let level = |rng: &mut Rng, max: u64| {
            if on(rng) { rng.range(1, max) as u32 } else { 0 }
        };
        let five = on(rng);
        let drawn = rng.range(1, 3) as u16;
        Swarm {
            nodes: nodes.unwrap_or(if five { 5 } else { 3 }),
            queue_partitions: partitions.unwrap_or(drawn),
            drop_ppm: level(rng, 200_000),
            dup_ppm: level(rng, 50_000),
            spike_ppm: level(rng, 20_000),
            partitions: on(rng),
            one_way_cuts: on(rng),
            client_crashes: on(rng),
            replica_crashes: on(rng),
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
        [
            (self.partitions, Fault::Partition),
            (self.one_way_cuts, Fault::OneWayCut),
            (self.client_crashes, Fault::ClientCrash),
            (self.replica_crashes, Fault::ReplicaCrash),
            (self.torn_writes, Fault::TornWrite),
            (self.pauses, Fault::Pause),
            (self.clock_jumps, Fault::ClockJump),
        ]
        .into_iter()
        .filter_map(|(on, f)| on.then_some(f))
        .collect()
    }
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    Partition,
    OneWayCut,
    ClientCrash,
    ReplicaCrash,
    TornWrite,
    Pause,
    ClockJump,
}

/// What a run reached: the queue world's counters plus the cluster's.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Coverage {
    pub queue: queue::Coverage,
    /// Entries committed: the length of the checker's ledger.
    pub committed: u64,
    pub max_term: u64,
    pub elections_won: u64,
    pub leader_crashes: u64,
    pub leader_pauses: u64,
    pub leader_partitions: u64,
    pub one_way_cuts: u64,
    /// Replies `unknown` sent because another entry took a request's index.
    pub replaced: u64,
    /// Completions of jobs with an ordering key (D79).
    pub ordered_completions: u64,
    /// The most partitions a run had.
    pub max_partitions: u64,
}

impl Coverage {
    pub fn add(&mut self, o: &Coverage) {
        self.queue.add(&o.queue);
        self.committed += o.committed;
        self.max_term = self.max_term.max(o.max_term);
        self.elections_won += o.elections_won;
        self.leader_crashes += o.leader_crashes;
        self.leader_pauses += o.leader_pauses;
        self.leader_partitions += o.leader_partitions;
        self.one_way_cuts += o.one_way_cuts;
        self.replaced += o.replaced;
        self.ordered_completions += o.ordered_completions;
        self.max_partitions = self.max_partitions.max(o.max_partitions);
    }
}

#[derive(Clone, Debug)]
pub struct Report {
    pub seed: u64,
    /// The `spool sim` arguments that replay this run.
    pub replay: String,
    pub hash: u64,
    pub swarm: Swarm,
    pub world: WorldStats,
    pub coverage: Coverage,
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
            "seed {} failed: {}\n  swarm: {:?}\n  replay: cargo run -- sim {} --trace",
            self.report.seed, self.message, self.report.swarm, self.report.replay
        )
    }
}

/// One partition's live checks.
#[derive(Default)]
struct PartChecks {
    /// For every committed index: a digest of the entry's data and of the
    /// queue state after it. Every replica must reach both.
    ledger: Vec<(u64, u64)>,
    /// The last index each replica applied in its current life.
    applied: BTreeMap<Id, u64>,
    /// Who won each term: election safety.
    leaders: BTreeMap<u64, Id>,
    /// Replicas that believe they lead, with their terms.
    leading: BTreeMap<Id, u64>,
    /// The queue's counts after the last committed entry.
    counts: Counts,
}

/// The cluster's live checks, per partition.
struct Checks {
    members: Vec<Id>,
    parts: Vec<PartChecks>,
    cov: Coverage,
}

type Ck = Rc<RefCell<Checks>>;

impl Checks {
    fn apply(&mut self, sh: &mut Shared, p: u16, id: Id, a: &Applied) {
        let part = &mut self.parts[usize::from(p)];
        let last = part.applied.insert(id, a.index).unwrap_or(0);
        if a.index != last + 1 {
            return sh.fail(format!(
                "p{p}: n{id} applied entry {} right after {last}",
                a.index
            ));
        }
        let state = a.state.as_deref().map_or(0, super::digest);
        let mine = (super::digest(&a.data), state);
        match part.ledger.get(a.index as usize - 1) {
            Some(&(data, _)) if data != mine.0 => sh.fail(format!(
                "state machine safety: p{p}: n{id} applied other data at {} than another replica",
                a.index
            )),
            Some(&(_, s)) if s != mine.1 => sh.fail(format!(
                "p{p}: n{id} reached another queue state after entry {} than another replica",
                a.index
            )),
            Some(_) => {}
            None => {
                part.ledger.push(mine);
                self.cov.committed += 1;
            }
        }
    }

    /// The highest-term leader of partition `p`, if one believes it leads.
    fn leader(&self, p: u16) -> Option<Id> {
        let leading = &self.parts[usize::from(p)].leading;
        leading.iter().max_by_key(|&(_, t)| *t).map(|(&id, _)| id)
    }
}

// ---------------------------------------------------------------- replicas

type Tag = (NodeId, u64);

/// A partition's log on its node's disk (D81).
type Disk = Prefixed<SimDisk>;

fn partition_disk(disk: &SimDisk, p: u16) -> Disk {
    Prefixed {
        inner: disk.clone(),
        prefix: format!("p{p}-"),
    }
}

/// One partition's replica on a node, with what its driver keeps.
#[derive(Default)]
struct Part {
    replica: Option<Replica<Disk, Tag>>,
    election_timer: u64,
    batch_timer: u64,
    pending: Vec<(Tag, Op)>,
    /// Every request proposed and not yet answered, to check that an answer
    /// is about the op it answers.
    asked: BTreeMap<Tag, Op>,
}

/// One node: a [`Replica`] per partition over the node's disk (D75).
/// Requests for a partition that arrive within a 1–5 ms window are proposed
/// as one entry (D66), stamped with the node's clock. A log that fails stops
/// the whole node, which recovers every partition on restart (D38, D63).
struct Server {
    sh: Sh,
    ck: Ck,
    id: Id,
    disk: SimDisk,
    bug: Option<Bug>,
    checks: bool,
    parts: Vec<Part>,
}

impl Server {
    fn drive(&mut self, ctx: &mut Ctx<'_, Msg>, p: u16, out: Result<Output<Tag>, ReplicaError>) {
        let out = match out {
            Ok(out) => out,
            Err(ReplicaError::Store(StoreError::Io(_))) => {
                // The log failed: stop and recover, like the server (D38, D63).
                self.sh.borrow_mut().cov.disk_failures += 1;
                self.parts.iter_mut().for_each(|part| part.replica = None);
                return ctx.halt();
            }
            Err(e) => {
                self.parts.iter_mut().for_each(|part| part.replica = None);
                return self
                    .sh
                    .borrow_mut()
                    .fail(format!("p{p}: n{}: {e}", self.id));
            }
        };
        let part = &mut self.parts[usize::from(p)];
        let Some(replica) = &part.replica else {
            return;
        };
        for (to, m) in out.send {
            ctx.send(NodeId(to), Msg::Raft(p, m));
        }
        let mut sh = self.sh.borrow_mut();
        let mut ck = self.ck.borrow_mut();
        for ((to, id), reply) in out.replies {
            let op = part.asked.remove(&(to, id));
            let wire = match reply {
                Reply::Events(events) => {
                    if let Some(op) = &op
                        && !answers(op, &events)
                    {
                        sh.fail(format!(
                            "p{p}: n{} answered `{}` with events of another command: {events:?}",
                            self.id,
                            Command {
                                at: Time(0),
                                op: op.clone()
                            }
                        ));
                    }
                    for e in &events {
                        match e {
                            Event::Released {
                                reason: ReleaseReason::Expired,
                                ..
                            } => sh.cov.leases_expired += 1,
                            Event::Completed { .. } => sh.cov.completions += 1,
                            _ => {}
                        }
                    }
                    WireReply::Events(events)
                }
                Reply::NotLeader(leader) => WireReply::NotLeader { leader },
                Reply::Unknown => {
                    ck.cov.replaced += 1;
                    WireReply::Unknown
                }
            };
            let mut bytes = Vec::new();
            protocol::encode_reply(id, &wire, &mut bytes);
            ctx.send(to, Msg::Frame(bytes));
        }
        for a in &out.applied {
            ck.apply(&mut sh, p, self.id, a);
            let pc = &mut ck.parts[usize::from(p)];
            if a.index == pc.ledger.len() as u64 {
                pc.counts = replica.queue().counts();
                sh.counts = sum(ck.parts.iter().map(|pc| pc.counts));
            }
        }
        let node = replica.node();
        let pc = &mut ck.parts[usize::from(p)];
        let mut won = false;
        if replica.leading() {
            if !pc.leading.contains_key(&self.id) {
                won = true;
                if let Some(other) = pc.leaders.insert(node.term(), self.id)
                    && other != self.id
                {
                    sh.fail(format!(
                        "election safety: p{p}: n{other} and n{} both won term {}",
                        self.id,
                        node.term()
                    ));
                }
            }
            pc.leading.insert(self.id, node.term());
        } else {
            pc.leading.remove(&self.id);
        }
        ck.cov.elections_won += u64::from(won);
        ck.cov.max_term = ck.cov.max_term.max(node.term());
        drop((sh, ck));
        if out.reset_election_timer {
            part.election_timer = self.sh.borrow_mut().req();
            let after = ctx.rng().range(ELECTION.0.0, ELECTION.1.0);
            ctx.set_timer(Millis(after), part.election_timer);
        }
    }

    fn propose(&mut self, ctx: &mut Ctx<'_, Msg>, p: u16) {
        let part = &mut self.parts[usize::from(p)];
        part.batch_timer = 0;
        let Some(replica) = &mut part.replica else {
            return;
        };
        let n = part.pending.len().min(MAX_BATCH);
        let batch: Vec<(Tag, Op)> = part.pending.drain(..n).collect();
        part.asked
            .extend(batch.iter().map(|(tag, op)| (*tag, op.clone())));
        let out = replica.propose(ctx.now(), batch);
        self.drive(ctx, p, out);
        self.start_batch(ctx, p);
    }

    fn start_batch(&mut self, ctx: &mut Ctx<'_, Msg>, p: u16) {
        let part = &mut self.parts[usize::from(p)];
        if part.pending.is_empty() || part.batch_timer != 0 {
            return;
        }
        part.batch_timer = self.sh.borrow_mut().req();
        let after = Millis(ctx.rng().range(1, 5));
        ctx.set_timer(after, part.batch_timer);
    }
}

fn sum(counts: impl Iterator<Item = Counts>) -> Counts {
    counts.fold(Counts::default(), |a, c| Counts {
        waiting: a.waiting + c.waiting,
        leased: a.leased + c.leased,
        dead: a.dead + c.dead,
        acked: a.acked + c.acked,
    })
}

impl Process<Msg> for Server {
    fn start(&mut self, ctx: &mut Ctx<'_, Msg>) {
        let (members, partitions) = {
            let mut ck = self.ck.borrow_mut();
            for pc in &mut ck.parts {
                pc.applied.insert(self.id, 0);
                pc.leading.remove(&self.id);
            }
            (ck.members.clone(), ck.parts.len() as u16)
        };
        self.parts = (0..partitions).map(|_| Part::default()).collect();
        let mut outs = Vec::new();
        for p in 0..partitions {
            let disk = partition_disk(&self.disk, p);
            match Replica::open(self.id, &members, p, disk, self.checks) {
                Ok((replica, opened)) => {
                    let mut sh = self.sh.borrow_mut();
                    sh.cov.recoveries += u64::from(opened.records > 0);
                    sh.cov.torn_tails += u64::from(opened.torn);
                    drop(sh);
                    let mut replica = replica.with_bugs(None, self.bug);
                    outs.push(replica.start());
                    self.parts[usize::from(p)].replica = Some(replica);
                }
                Err(StoreError::Io(_)) => {
                    self.sh.borrow_mut().cov.recovery_failures += 1;
                    self.parts.iter_mut().for_each(|part| part.replica = None);
                    return ctx.halt();
                }
                Err(e) => {
                    return self
                        .sh
                        .borrow_mut()
                        .fail(format!("p{p}: n{} recovery: {e}", self.id));
                }
            }
        }
        ctx.set_timer(HEARTBEAT, HEARTBEAT_TIMER);
        for (p, out) in outs.into_iter().enumerate() {
            self.drive(ctx, p as u16, out);
        }
    }

    fn receive(&mut self, ctx: &mut Ctx<'_, Msg>, from: NodeId, msg: Msg) {
        match msg {
            Msg::Raft(p, m) => {
                let Some(replica) = self
                    .parts
                    .get_mut(usize::from(p))
                    .and_then(|part| part.replica.as_mut())
                else {
                    return;
                };
                let out = replica.receive(from.0, m);
                self.drive(ctx, p, out);
            }
            Msg::Frame(bytes) => {
                let request = protocol::decode_frame(&bytes).and_then(|f| Ok((f.id, f.request()?)));
                let (id, p, op) = match request {
                    Ok((id, Request::Op(op))) => (id, 0, op),
                    Ok((id, Request::Routed { partition, op })) => (id, partition, op),
                    other => {
                        return self.sh.borrow_mut().fail(format!(
                            "n{} got a bad frame from {from}: {other:?}",
                            self.id
                        ));
                    }
                };
                let Some(part) = self.parts.get_mut(usize::from(p)) else {
                    return self.sh.borrow_mut().fail(format!(
                        "n{} got a request for partition {p} from {from}",
                        self.id
                    ));
                };
                if part.replica.is_none() {
                    return;
                }
                part.pending.push(((from, id), op));
                self.start_batch(ctx, p);
            }
            Msg::Write { .. } | Msg::Written { .. } => {}
        }
    }

    fn timer(&mut self, ctx: &mut Ctx<'_, Msg>, id: u64) {
        if id == HEARTBEAT_TIMER {
            ctx.set_timer(HEARTBEAT, HEARTBEAT_TIMER);
            for p in 0..self.parts.len() as u16 {
                if let Some(replica) = &mut self.parts[usize::from(p)].replica {
                    let out = replica.heartbeat();
                    self.drive(ctx, p, out);
                }
            }
            return;
        }
        for p in 0..self.parts.len() as u16 {
            let part = &mut self.parts[usize::from(p)];
            let Some(replica) = &mut part.replica else {
                continue;
            };
            if id == part.election_timer {
                let out = replica.election_timeout();
                return self.drive(ctx, p, out);
            } else if id == part.batch_timer {
                return self.propose(ctx, p);
            }
        }
    }
}

/// Whether `events` can be the answer to `op`: the last event is about the
/// op itself (D33), so it must be of the op's kind and name its job or key.
fn answers(op: &Op, events: &[Event]) -> bool {
    let Some(last) = events.last() else {
        return false;
    };
    if matches!(last, Event::Rejected { .. }) {
        return !matches!(
            op,
            Op::Enqueue { .. } | Op::Lease { .. } | Op::Result { .. }
        );
    }
    match (op, last) {
        (Op::Enqueue { key, .. }, Event::Enqueued { key: k, .. }) => key == k,
        (Op::Enqueue { key: Some(key), .. }, Event::Deduplicated { key: k, .. }) => key == k,
        (Op::Lease { queue, .. }, Event::Empty { queue: q }) => queue == q,
        (Op::Lease { .. }, Event::Leased { .. }) => true,
        (Op::Heartbeat { job, token, .. }, Event::Renewed { lease }) => {
            lease.job == *job && lease.token == *token
        }
        (Op::Ack { job, .. }, Event::Acked { job: j }) => job == j,
        (Op::Complete { job, token, .. }, Event::Completed { job: j, token: t }) => {
            job == j && token == t
        }
        (Op::Result { job }, Event::Result { job: j, .. }) => job == j,
        (Op::Nack { job, .. }, Event::Retrying { job: j, .. } | Event::DeadLettered { job: j }) => {
            job == j
        }
        (Op::Configure { queue, .. }, Event::Configured { queue: q, .. }) => queue == q,
        (Op::Subscribe { queue, group }, Event::Subscribed { queue: q, group: g }) => {
            queue == q && group == g
        }
        _ => false,
    }
}

// ---------------------------------------------------------------- the run

pub fn run(options: &Options) -> Result<Report, Failure> {
    let mut world = World::<Msg>::new(options.seed);
    if options.trace {
        world.enable_trace();
    }
    let swarm = Swarm::pick(world.rng(), options.nodes, options.partitions);
    let members: Vec<Id> = (0..swarm.nodes).collect();
    let servers: Vec<NodeId> = members.iter().map(|&m| NodeId(m)).collect();
    let store = NodeId(swarm.nodes);
    let mut shared = Shared::new(options.queue_bug, servers.clone(), store);
    shared.partitions = swarm.queue_partitions;
    let fan = queue::fan_name();
    shared.queues = std::iter::once(queue::queue_name())
        .chain(queue::fan_groups().iter().map(|g| fan.group(g).unwrap()))
        .collect();
    let sh: Sh = Rc::new(RefCell::new(shared));
    let ck: Ck = Rc::new(RefCell::new(Checks {
        members: members.clone(),
        parts: (0..swarm.queue_partitions)
            .map(|_| PartChecks::default())
            .collect(),
        cov: Coverage {
            max_partitions: u64::from(swarm.queue_partitions),
            ..Coverage::default()
        },
    }));
    for &id in &members {
        let (s, c, bug, checks) = (sh.clone(), ck.clone(), options.bug, options.checks);
        world.add(Box::new(move |_, disk| {
            Box::new(Server {
                sh: s.clone(),
                ck: c.clone(),
                id,
                disk,
                bug,
                checks,
                parts: Vec::new(),
            })
        }));
    }
    let s = sh.clone();
    world.add(Box::new(move |_, _| Box::new(Store { sh: s.clone() })));
    let mut clients = Vec::new();
    // Plain, ordered (D79) and fanned out to consumer groups (D80), in turn.
    for index in 0..options.producers {
        let (s, jobs) = (sh.clone(), options.jobs);
        let kind = [Kind::Plain, Kind::Ordered, Kind::Fan][index as usize % 3];
        clients.push(world.add(Box::new(move |_, _| {
            Box::new(Producer::new(s.clone(), index, jobs, kind))
        })));
    }
    for _ in 0..options.workers {
        let s = sh.clone();
        clients.push(world.add(Box::new(move |_, _| Box::new(Worker::new(s.clone())))));
    }
    let nodes = swarm.nodes + 1 + clients.len() as u32;

    world.net = swarm.net();
    world.halt_downtime = (Millis(10), Millis(1_000));
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
                inject(
                    &mut world,
                    &sh,
                    &ck,
                    fault,
                    &swarm,
                    &clients,
                    nodes,
                    &mut heal_at,
                );
            }
            next_fault = now.plus(Millis(world.rng().range(100, 2_000)));
        }
        if quiet && done(&world, &sh, &ck, options, swarm.nodes) {
            break check_end(&world, &sh, &ck, options);
        }
        if now >= deadline {
            let (s, c) = (sh.borrow(), ck.borrow());
            let parts: Vec<String> = c
                .parts
                .iter()
                .enumerate()
                .map(|(p, pc)| {
                    format!(
                        "p{p}: {} committed, applied {:?}, leading {:?}",
                        pc.ledger.len(),
                        pc.applied,
                        pc.leading
                    )
                })
                .collect();
            break Err(format!(
                "not finished {} ms after the faults stopped: producers done {}/{}, counts {:?}; {}",
                FINISH_WITHIN.0,
                s.producers_done.len(),
                options.producers,
                s.counts,
                parts.join("; ")
            ));
        }
    };
    let mut coverage = ck.borrow().cov;
    coverage.queue = sh.borrow().cov;
    let report = Report {
        seed: options.seed,
        replay: replay(options),
        hash: world.hash(),
        swarm,
        world: world.stats(),
        coverage,
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

/// The arguments of `spool sim` that run `options` again.
fn replay(options: &Options) -> String {
    let mut args = format!("--cluster --seed {}", options.seed);
    if let Some(p) = options.partitions {
        args += &format!(" --partitions {p}");
    }
    if !options.checks {
        args += " --no-checks";
    }
    let bug = match (options.queue_bug, options.bug) {
        (Some(queue::Bug::NoFence), _) => Some("no-fence"),
        (Some(queue::Bug::NoDedupKey), _) => Some("no-dedup-key"),
        (Some(queue::Bug::WrongPartition), _) => Some("wrong-partition"),
        (None, Some(Bug::ReplyBeforeCommit)) => Some("reply-before-commit"),
        (None, Some(Bug::IgnoreTermOnReply)) => Some("ignore-term-on-reply"),
        (None, Some(Bug::IgnoreOrderKey)) => Some("ignore-order-key"),
        (None, None) => None,
    };
    if let Some(bug) = bug {
        args += &format!(" --bug {bug}");
    }
    args
}

#[allow(clippy::too_many_arguments)]
fn inject(
    world: &mut World<Msg>,
    sh: &Sh,
    ck: &Ck,
    fault: Fault,
    swarm: &Swarm,
    clients: &[NodeId],
    nodes: u32,
    heal_at: &mut Option<Time>,
) {
    let mut qc = sh.borrow().cov;
    let mut cc = ck.borrow().cov;
    let now = world.now();
    let replicas = swarm.nodes;
    // Half the replica faults go to the leader of one partition (D63).
    let leader = {
        let c = ck.borrow();
        let p = world.rng().below(c.parts.len() as u64) as u16;
        c.leader(p)
    };
    let victim = |world: &mut World<Msg>| -> (NodeId, bool) {
        match leader {
            Some(l) if world.rng().chance(MILLION / 2) => (NodeId(l), true),
            _ => {
                let n = NodeId(world.rng().below(u64::from(replicas)) as u32);
                (n, Some(n.0) == leader)
            }
        }
    };
    match fault {
        Fault::Partition => {
            if heal_at.is_none() {
                let (node, is_leader) = if world.rng().chance(MILLION / 2) {
                    victim(world)
                } else {
                    (NodeId(world.rng().below(u64::from(nodes)) as u32), false)
                };
                world.isolate(node);
                *heal_at = Some(now.plus(Millis(world.rng().range(100, 3_000))));
                qc.partitions += 1;
                cc.leader_partitions += u64::from(is_leader);
            }
        }
        Fault::OneWayCut => {
            if heal_at.is_none() {
                let from = world.rng().below(u64::from(replicas)) as u32;
                let to = (from + 1 + world.rng().below(u64::from(replicas - 1)) as u32) % replicas;
                world.cut(NodeId(from), NodeId(to));
                *heal_at = Some(now.plus(Millis(world.rng().range(100, 3_000))));
                cc.one_way_cuts += 1;
            }
        }
        Fault::ClientCrash => {
            let node = *world.rng().pick(clients);
            if world.is_up(node) {
                world.crash(node);
                let after = Millis(world.rng().range(50, 2_000));
                world.restart_after(node, after);
                qc.client_crashes += 1;
            }
        }
        Fault::ReplicaCrash => {
            let (node, is_leader) = victim(world);
            if world.is_up(node) {
                world.crash(node);
                let after = if world.rng().chance(MILLION / 2) {
                    Millis(world.rng().range(10, 200))
                } else {
                    Millis(world.rng().range(200, 2_000))
                };
                world.restart_after(node, after);
                qc.server_crashes += 1;
                cc.leader_crashes += u64::from(is_leader);
            }
        }
        Fault::TornWrite => {
            let (node, _) = victim(world);
            let calls = world.rng().range(0, 5);
            world.disk(node).fail_in(calls);
            qc.torn_armed += 1;
        }
        Fault::Pause => {
            let (node, is_leader) = if world.rng().chance(MILLION / 2) {
                victim(world)
            } else {
                (NodeId(world.rng().below(u64::from(nodes)) as u32), false)
            };
            let until = now.plus(Millis(world.rng().range(100, 3 * queue::VISIBILITY.0)));
            world.pause(node, until);
            qc.pauses += 1;
            cc.leader_pauses += u64::from(is_leader);
        }
        Fault::ClockJump => {
            // As in the queue world, but on any replica: the next leader
            // stamps batches with its own clock (D68).
            let (node, _) = victim(world);
            if world.rng().chance(MILLION / 2) {
                let ms = world.rng().range(1, 2 * queue::VISIBILITY.0);
                world.shift_clock(node, ms as i64);
                qc.clock_forward += 1;
            } else {
                let ms = world.rng().range(1, 5_000);
                world.shift_clock(node, -(ms as i64));
                qc.clock_back += 1;
            }
        }
    }
    sh.borrow_mut().cov = qc;
    ck.borrow_mut().cov = cc;
}

/// Every producer is done and the queues are empty, every replica is up and
/// has applied every committed entry of its partition, and each partition has
/// a leader.
fn done(world: &World<Msg>, sh: &Sh, ck: &Ck, options: &Options, replicas: u32) -> bool {
    let c = ck.borrow();
    queue::producers_done(&sh.borrow(), options.producers)
        && c.parts.iter().all(|pc| {
            !pc.leading.is_empty()
                && (0..replicas).all(|n| {
                    world.is_up(NodeId(n))
                        && pc.applied.get(&n).copied() == Some(pc.ledger.len() as u64)
                })
        })
}

/// Rebuild every partition's queue from n0's log alone, check it reached the
/// state every replica agreed on (when they computed it), then run the end
/// checks of D53 on all of them and the ordering check on their events.
fn check_end(world: &World<Msg>, sh: &Sh, ck_cell: &Ck, options: &Options) -> Result<(), String> {
    let files = world.disk(NodeId(0)).files();
    let ck = ck_cell.borrow();
    let mut queues = Vec::new();
    let mut order = OrderCheck::default();
    for (p, pc) in ck.parts.iter().enumerate() {
        let p = p as u16;
        let disk = Prefixed {
            inner: MemStorage::from_files(files.clone()),
            prefix: format!("p{p}-"),
        };
        let (_, saved, _) =
            RaftLog::open(disk).map_err(|e| format!("p{p}: final recovery failed: {e}"))?;
        let committed = pc.ledger.len();
        if saved.log.len() < committed {
            return Err(format!(
                "p{p}: n0's log has {} entries, {committed} were committed",
                saved.log.len()
            ));
        }
        let mut q = ReferenceQueue::for_partition(p);
        // The replicas ran the planted bug; so does the replay.
        if options.bug == Some(Bug::IgnoreOrderKey) {
            q.plant_ignore_order();
        }
        let mut out = Vec::new();
        for (i, entry) in saved.log[..committed].iter().enumerate() {
            if entry.data.is_empty() {
                continue;
            }
            let (at, ops) = replica::decode_batch(&entry.data)
                .map_err(|e| format!("p{p}: entry {} does not decode: {e}", i + 1))?;
            for op in ops {
                q.apply(&Command { at, op }, &mut out);
                order.observe(&out).map_err(|e| format!("p{p}: {e}"))?;
                out.clear();
            }
        }
        let mut state = Vec::new();
        q.encode_state(&mut state);
        if options.checks && committed > 0 && super::digest(&state) != pc.ledger[committed - 1].1 {
            return Err(format!(
                "p{p}: n0's log replays to another state than the replicas reached"
            ));
        }
        queues.push(q);
    }
    drop(ck);
    ck_cell.borrow_mut().cov.ordered_completions = order.completions;
    let refs: Vec<&ReferenceQueue> = queues.iter().collect();
    queue::check_final(&refs, &sh.borrow())?;
    Ok(())
}

/// D79 judged from the applied events alone, apart from the queue and its
/// ledger: per ordering key, at most one job leased at a time, and jobs
/// completed in the order they were enqueued. A dead job leaves its key; a
/// redriven one rejoins it at the back.
#[derive(Default)]
struct OrderCheck {
    /// The partition each ordering key was seen in: only one (D76).
    partition_of: BTreeMap<(QueueName, OrderKey), u16>,
    key_of: BTreeMap<JobId, (QueueName, OrderKey)>,
    /// Per key, the jobs not yet finished, oldest first.
    waiting: BTreeMap<(QueueName, OrderKey), VecDeque<JobId>>,
    /// Per key, the job leased now.
    leased: BTreeMap<(QueueName, OrderKey), JobId>,
    finished: BTreeSet<JobId>,
    completions: u64,
}

impl OrderCheck {
    fn observe(&mut self, events: &[Event]) -> Result<(), String> {
        for e in events {
            match e {
                Event::Enqueued {
                    job,
                    queue,
                    order: Some(order),
                    ..
                } => {
                    let k = (queue.clone(), order.clone());
                    let p = *self
                        .partition_of
                        .entry(k.clone())
                        .or_insert(job.partition());
                    if p != job.partition() {
                        return Err(format!(
                            "ordering key {}/{} has jobs in partitions {p} and {}",
                            k.0,
                            k.1,
                            job.partition()
                        ));
                    }
                    self.key_of.insert(*job, k.clone());
                    self.waiting.entry(k).or_default().push_back(*job);
                }
                Event::Leased { lease, .. } => {
                    let Some(k) = self.key_of.get(&lease.job) else {
                        continue;
                    };
                    if let Some(other) = self.leased.insert(k.clone(), lease.job) {
                        return Err(format!(
                            "jobs {other} and {} of ordering key {}/{} leased at once",
                            lease.job, k.0, k.1
                        ));
                    }
                    let first = self.waiting.get(k).and_then(|w| w.front());
                    if first != Some(&lease.job) {
                        return Err(format!(
                            "job {} of ordering key {}/{} leased before job {first:?}",
                            lease.job, k.0, k.1
                        ));
                    }
                }
                Event::Released { job, .. } => {
                    if let Some(k) = self.key_of.get(job)
                        && self.leased.get(k) == Some(job)
                    {
                        self.leased.remove(k);
                    }
                }
                Event::Acked { job } | Event::Completed { job, .. } => {
                    let Some(k) = self.key_of.get(job).cloned() else {
                        continue;
                    };
                    if !self.finished.insert(*job) {
                        continue; // a repeated complete (D42)
                    }
                    self.leased.remove(&k);
                    let w = self.waiting.entry(k.clone()).or_default();
                    if w.front() != Some(job) {
                        return Err(format!(
                            "job {job} of ordering key {}/{} completed before job {:?}",
                            k.0,
                            k.1,
                            w.front()
                        ));
                    }
                    w.pop_front();
                    self.completions += 1;
                }
                Event::DeadLettered { job } => {
                    if let Some(k) = self.key_of.get(job).cloned() {
                        self.waiting
                            .entry(k.clone())
                            .or_default()
                            .retain(|j| j != job);
                        if self.leased.get(&k) == Some(job) {
                            self.leased.remove(&k);
                        }
                    }
                }
                Event::Redriven { job } => {
                    if let Some(k) = self.key_of.get(job).cloned() {
                        self.waiting.entry(k).or_default().push_back(*job);
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}
