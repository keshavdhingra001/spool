//! The M7 world (D73): the M5 producers, workers and fenced store against a
//! replicated queue of 3 or 5 [`Replica`]s, under the faults of D52 and D63.
//!
//! ```text
//! producers ──enqueue (keyed)──┐            not_leader / unknown: retry (D71, D72)
//!                              ▼
//!            n0 ◄──raft──► n1 ◄──raft──► n2      replicas: Replica on a SimDisk
//!                              ▲
//! workers ── lease / heartbeat / complete ──┘      ── write(job, token) ──> store
//! ```
//!
//! Node ids: the replicas are n0.. in the order of their Raft ids, then the
//! store, the producers and the workers. Checks, live: every replica applies
//! the same entry at each index and reaches the same queue state after it (the
//! replica itself runs both checkers after every command), and no two leaders
//! share a term. At the end, the checks of D53 run on the queue rebuilt from
//! n0's log.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::rc::Rc;

use super::queue::{self, Msg, Producer, Sh, Shared, Store, Worker};
use super::{Ctx, MILLION, Net, NodeId, Process, Rng, START, SimDisk, World, WorldStats};
use crate::command::{Command, Event, Op, ReleaseReason};
use crate::error::StoreError;
use crate::protocol::{self, Reply as WireReply, Request};
use crate::queue::{Queue, Snapshot};
use crate::raft::Id;
use crate::raft::store::RaftLog;
use crate::reference::ReferenceQueue;
use crate::replica::{self, Applied, Bug, Output, Replica, ReplicaError, Reply};
use crate::storage::MemStorage;
use crate::types::{Millis, Time};

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
            producers: 2,
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
    fn pick(rng: &mut Rng, nodes: Option<u32>) -> Swarm {
        let on = |rng: &mut Rng| rng.chance(MILLION / 2);
        let level = |rng: &mut Rng, max: u64| {
            if on(rng) { rng.range(1, max) as u32 } else { 0 }
        };
        let five = on(rng);
        Swarm {
            nodes: nodes.unwrap_or(if five { 5 } else { 3 }),
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
    }
}

#[derive(Clone, Debug)]
pub struct Report {
    pub seed: u64,
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
            "seed {} failed: {}\n  swarm: {:?}\n  replay: cargo run -- sim --cluster --seed {} --trace",
            self.report.seed, self.message, self.report.swarm, self.report.seed
        )
    }
}

/// The cluster's live checks.
struct Checks {
    members: Vec<Id>,
    /// For every committed index: a digest of the entry's data and of the
    /// queue state after it. Every replica must reach both.
    ledger: Vec<(u64, u64)>,
    /// The last index each replica applied in its current life.
    applied: BTreeMap<Id, u64>,
    /// Who won each term: election safety.
    leaders: BTreeMap<u64, Id>,
    /// Replicas that believe they lead, with their terms.
    leading: BTreeMap<Id, u64>,
    cov: Coverage,
}

type Ck = Rc<RefCell<Checks>>;

impl Checks {
    fn apply(&mut self, sh: &mut Shared, id: Id, a: &Applied) {
        let last = self.applied.insert(id, a.index).unwrap_or(0);
        if a.index != last + 1 {
            return sh.fail(format!(
                "n{id} applied entry {} right after {last}",
                a.index
            ));
        }
        let state = a.state.as_deref().map_or(0, super::digest);
        let mine = (super::digest(&a.data), state);
        match self.ledger.get(a.index as usize - 1) {
            Some(&(data, _)) if data != mine.0 => sh.fail(format!(
                "state machine safety: n{id} applied other data at {} than another replica",
                a.index
            )),
            Some(&(_, s)) if s != mine.1 => sh.fail(format!(
                "n{id} reached another queue state after entry {} than another replica",
                a.index
            )),
            Some(_) => {}
            None => {
                self.ledger.push(mine);
                self.cov.committed += 1;
            }
        }
    }
}

// ---------------------------------------------------------------- replicas

type Tag = (NodeId, u64);

/// One replica: a [`Replica`] over the node's disk. Requests that arrive
/// within a 1–5 ms window are proposed as one entry (D66), stamped with the
/// node's clock.
struct Server {
    sh: Sh,
    ck: Ck,
    id: Id,
    disk: SimDisk,
    bug: Option<Bug>,
    replica: Option<Replica<SimDisk, Tag>>,
    election_timer: u64,
    batch_timer: u64,
    pending: Vec<(Tag, Op)>,
    /// Every request proposed and not yet answered, to check that an answer
    /// is about the op it answers.
    asked: BTreeMap<Tag, Op>,
}

impl Server {
    fn drive(&mut self, ctx: &mut Ctx<'_, Msg>, out: Result<Output<Tag>, ReplicaError>) {
        let out = match out {
            Ok(out) => out,
            Err(ReplicaError::Store(StoreError::Io(_))) => {
                // The log failed: stop and recover, like the server (D38, D63).
                self.sh.borrow_mut().cov.disk_failures += 1;
                self.replica = None;
                return ctx.halt();
            }
            Err(e) => {
                self.replica = None;
                return self.sh.borrow_mut().fail(format!("n{}: {e}", self.id));
            }
        };
        let Some(replica) = &self.replica else {
            return;
        };
        for (to, m) in out.send {
            ctx.send(NodeId(to), Msg::Raft(m));
        }
        let mut sh = self.sh.borrow_mut();
        let mut ck = self.ck.borrow_mut();
        for ((to, id), reply) in out.replies {
            let op = self.asked.remove(&(to, id));
            let wire = match reply {
                Reply::Events(events) => {
                    if let Some(op) = &op
                        && !answers(op, &events)
                    {
                        sh.fail(format!(
                            "n{} answered `{}` with events of another command: {events:?}",
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
            ck.apply(&mut sh, self.id, a);
            if a.index == ck.ledger.len() as u64 {
                sh.counts = replica.queue().counts();
            }
        }
        let node = replica.node();
        if replica.leading() {
            if !ck.leading.contains_key(&self.id) {
                ck.cov.elections_won += 1;
                if let Some(other) = ck.leaders.insert(node.term(), self.id)
                    && other != self.id
                {
                    sh.fail(format!(
                        "election safety: n{other} and n{} both won term {}",
                        self.id,
                        node.term()
                    ));
                }
            }
            ck.leading.insert(self.id, node.term());
        } else {
            ck.leading.remove(&self.id);
        }
        ck.cov.max_term = ck.cov.max_term.max(node.term());
        drop((sh, ck));
        if out.reset_election_timer {
            self.election_timer = self.sh.borrow_mut().req();
            let after = ctx.rng().range(ELECTION.0.0, ELECTION.1.0);
            ctx.set_timer(Millis(after), self.election_timer);
        }
    }

    fn propose(&mut self, ctx: &mut Ctx<'_, Msg>) {
        self.batch_timer = 0;
        let Some(replica) = &mut self.replica else {
            return;
        };
        let n = self.pending.len().min(MAX_BATCH);
        let batch: Vec<(Tag, Op)> = self.pending.drain(..n).collect();
        self.asked
            .extend(batch.iter().map(|(tag, op)| (*tag, op.clone())));
        let out = replica.propose(ctx.now(), batch);
        self.drive(ctx, out);
        self.start_batch(ctx);
    }

    fn start_batch(&mut self, ctx: &mut Ctx<'_, Msg>) {
        if self.pending.is_empty() || self.batch_timer != 0 {
            return;
        }
        self.batch_timer = self.sh.borrow_mut().req();
        let after = Millis(ctx.rng().range(1, 5));
        ctx.set_timer(after, self.batch_timer);
    }
}

impl Process<Msg> for Server {
    fn start(&mut self, ctx: &mut Ctx<'_, Msg>) {
        let members = self.ck.borrow().members.clone();
        self.ck.borrow_mut().applied.insert(self.id, 0);
        self.ck.borrow_mut().leading.remove(&self.id);
        match Replica::open(self.id, &members, self.disk.clone(), true) {
            Ok((replica, opened)) => {
                let mut sh = self.sh.borrow_mut();
                sh.cov.recoveries += u64::from(opened.records > 0);
                sh.cov.torn_tails += u64::from(opened.torn);
                drop(sh);
                let mut replica = replica.with_bugs(None, self.bug);
                let out = replica.start();
                self.replica = Some(replica);
                ctx.set_timer(HEARTBEAT, HEARTBEAT_TIMER);
                self.drive(ctx, out);
            }
            Err(StoreError::Io(_)) => {
                self.sh.borrow_mut().cov.recovery_failures += 1;
                ctx.halt();
            }
            Err(e) => self
                .sh
                .borrow_mut()
                .fail(format!("n{} recovery: {e}", self.id)),
        }
    }

    fn receive(&mut self, ctx: &mut Ctx<'_, Msg>, from: NodeId, msg: Msg) {
        let Some(replica) = &mut self.replica else {
            return;
        };
        match msg {
            Msg::Raft(m) => {
                let out = replica.receive(from.0, m);
                self.drive(ctx, out);
            }
            Msg::Frame(bytes) => {
                match protocol::decode_frame(&bytes).and_then(|f| Ok((f.id, f.request()?))) {
                    Ok((id, Request::Op(op))) => {
                        self.pending.push(((from, id), op));
                        self.start_batch(ctx);
                    }
                    other => self.sh.borrow_mut().fail(format!(
                        "n{} got a bad frame from {from}: {other:?}",
                        self.id
                    )),
                }
            }
            Msg::Write { .. } | Msg::Written { .. } => {}
        }
    }

    fn timer(&mut self, ctx: &mut Ctx<'_, Msg>, id: u64) {
        let Some(replica) = &mut self.replica else {
            return;
        };
        if id == HEARTBEAT_TIMER {
            let out = replica.heartbeat();
            ctx.set_timer(HEARTBEAT, HEARTBEAT_TIMER);
            self.drive(ctx, out);
        } else if id == self.election_timer {
            let out = replica.election_timeout();
            self.drive(ctx, out);
        } else if id == self.batch_timer {
            self.propose(ctx);
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
        _ => false,
    }
}

// ---------------------------------------------------------------- the run

pub fn run(options: &Options) -> Result<Report, Failure> {
    let mut world = World::<Msg>::new(options.seed);
    if options.trace {
        world.enable_trace();
    }
    let swarm = Swarm::pick(world.rng(), options.nodes);
    let members: Vec<Id> = (0..swarm.nodes).collect();
    let servers: Vec<NodeId> = members.iter().map(|&m| NodeId(m)).collect();
    let store = NodeId(swarm.nodes);
    let sh: Sh = Rc::new(RefCell::new(Shared::new(
        options.queue_bug,
        servers.clone(),
        store,
    )));
    let ck: Ck = Rc::new(RefCell::new(Checks {
        members: members.clone(),
        ledger: Vec::new(),
        applied: BTreeMap::new(),
        leaders: BTreeMap::new(),
        leading: BTreeMap::new(),
        cov: Coverage::default(),
    }));
    for &id in &members {
        let (s, c, bug) = (sh.clone(), ck.clone(), options.bug);
        world.add(Box::new(move |_, disk| {
            Box::new(Server {
                sh: s.clone(),
                ck: c.clone(),
                id,
                disk,
                bug,
                replica: None,
                election_timer: 0,
                batch_timer: 0,
                pending: Vec::new(),
                asked: BTreeMap::new(),
            })
        }));
    }
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
            break check_end(&world, &sh, &ck);
        }
        if now >= deadline {
            let (s, c) = (sh.borrow(), ck.borrow());
            break Err(format!(
                "not finished {} ms after the faults stopped: producers done {}/{}, counts {:?}, \
                 {} committed, applied {:?}, leading {:?}",
                FINISH_WITHIN.0,
                s.producers_done.len(),
                options.producers,
                s.counts,
                c.ledger.len(),
                c.applied,
                c.leading
            ));
        }
    };
    let mut coverage = ck.borrow().cov;
    coverage.queue = sh.borrow().cov;
    let report = Report {
        seed: options.seed,
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
    // Half the replica faults go to the leader of the highest term (D63).
    let leader = {
        let c = ck.borrow();
        c.leading.iter().max_by_key(|&(_, t)| *t).map(|(&id, _)| id)
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

/// Every producer is done and the queue is empty, every replica is up and has
/// applied every committed entry, and one leads.
fn done(world: &World<Msg>, sh: &Sh, ck: &Ck, options: &Options, replicas: u32) -> bool {
    let c = ck.borrow();
    queue::producers_done(&sh.borrow(), options.producers)
        && !c.leading.is_empty()
        && (0..replicas).all(|n| {
            world.is_up(NodeId(n)) && c.applied.get(&n).copied() == Some(c.ledger.len() as u64)
        })
}

/// Rebuild n0's queue from its log alone, check it reached the state every
/// replica agreed on, then run the end checks of D53 on it.
fn check_end(world: &World<Msg>, sh: &Sh, ck: &Ck) -> Result<(), String> {
    let files = world.disk(NodeId(0)).files();
    let (_, saved, _) = RaftLog::open(MemStorage::from_files(files))
        .map_err(|e| format!("final recovery failed: {e}"))?;
    let ck = ck.borrow();
    let committed = ck.ledger.len();
    if saved.log.len() < committed {
        return Err(format!(
            "n0's log has {} entries, {committed} were committed",
            saved.log.len()
        ));
    }
    let mut q = ReferenceQueue::new();
    let mut out = Vec::new();
    for (i, entry) in saved.log[..committed].iter().enumerate() {
        if entry.data.is_empty() {
            continue;
        }
        let (at, ops) = replica::decode_batch(&entry.data)
            .map_err(|e| format!("entry {} does not decode: {e}", i + 1))?;
        for op in ops {
            q.apply(&Command { at, op }, &mut out);
            out.clear();
        }
    }
    let mut state = Vec::new();
    q.encode_state(&mut state);
    if committed > 0 && super::digest(&state) != ck.ledger[committed - 1].1 {
        return Err("n0's log replays to another state than the replicas reached".into());
    }
    queue::check_final(&q, &sh.borrow())
}
