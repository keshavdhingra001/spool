//! The M6 world (D62–D65): a Raft cluster of 3 or 5 nodes, each a
//! [`Node`] over a [`RaftLog`] on its sim disk, and clients that propose
//! numbered operations, under the swarm faults of D63, checked live as D62
//! says.
//!
//! Node ids: the Raft nodes are n0.. in the order of their Raft ids, then the
//! clients. Timer and request ids come from one counter shared by every node
//! (as in the queue world), so a timer or reply from an earlier life is never
//! taken for a current one.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::rc::Rc;

use super::{Ctx, MILLION, Message, Net, NodeId, Process, Rng, START, SimDisk, World, WorldStats};
use crate::error::StoreError;
use crate::raft::store::RaftLog;
use crate::raft::{self, AppendResult, Bug, Entry, Id, Node, NodeStats, Role};
use crate::types::{Millis, Time};

/// Faults are injected this long; then the world heals.
pub const FAULT_PHASE: Millis = Millis(30_000);
/// After the fault phase, every operation must be done within this.
pub const FINISH_WITHIN: Millis = Millis(30_000);
/// How often a leader sends appends (D59).
pub const HEARTBEAT: Millis = Millis(50);
/// Election timeouts are drawn from this range (D59).
pub const ELECTION: (Millis, Millis) = (Millis(150), Millis(300));
/// How long a client waits for an answer before trying another node.
const CLIENT_TIMEOUT: Millis = Millis(500);
/// The heartbeat timer's id; every other id comes from the shared counter.
const HEARTBEAT_TIMER: u64 = 0;

#[derive(Clone, Debug)]
pub struct Options {
    pub seed: u64,
    /// Cluster size; `None` lets the seed pick 3 or 5 (D63).
    pub nodes: Option<u32>,
    pub clients: u32,
    /// Operations each client proposes, one at a time.
    pub ops: u64,
    pub bug: Option<Bug>,
    /// Use this fault mix instead of the one the seed picks.
    pub swarm: Option<Swarm>,
    pub trace: bool,
}

impl Options {
    pub fn new(seed: u64) -> Self {
        Options {
            seed,
            nodes: None,
            clients: 3,
            ops: 20,
            bug: None,
            swarm: None,
            trace: false,
        }
    }
}

/// Which faults this seed turned on, and how strong (D52, D63).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Swarm {
    pub nodes: u32,
    pub drop_ppm: u32,
    pub dup_ppm: u32,
    pub spike_ppm: u32,
    pub partitions: bool,
    pub one_way_cuts: bool,
    pub crashes: bool,
    pub torn_writes: bool,
    pub pauses: bool,
    /// Kill a node right after its next step that synced something.
    pub crash_after_sync: bool,
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
            crashes: on(rng),
            torn_writes: on(rng),
            pauses: on(rng),
            crash_after_sync: on(rng),
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
            (self.crashes, Fault::Crash),
            (self.torn_writes, Fault::TornWrite),
            (self.pauses, Fault::Pause),
            (self.crash_after_sync, Fault::CrashAfterSync),
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
    Crash,
    TornWrite,
    Pause,
    CrashAfterSync,
}

/// What a run reached: the cases a test asserts were covered.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Coverage {
    /// Entries committed (the length of the checker's ledger).
    pub committed: u64,
    pub acked: u64,
    /// The highest term any node reached.
    pub max_term: u64,
    pub nodes: NodeStats,
    pub redirects: u64,
    pub not_leader: u64,
    pub client_timeouts: u64,
    /// Proposals whose entry was replaced by another leader's.
    pub replaced: u64,
    /// Nodes that started from a log with records in it.
    pub recoveries: u64,
    pub torn_tails: u64,
    /// Writes that failed on the disk and stopped the node (D63).
    pub disk_failures: u64,
    /// Recoveries that hit an armed disk failure and crashed again.
    pub recovery_failures: u64,
    pub crashes: u64,
    pub leader_crashes: u64,
    pub pauses: u64,
    pub leader_pauses: u64,
    pub partitions: u64,
    pub leader_partitions: u64,
    pub one_way_cuts: u64,
    pub torn_armed: u64,
    pub crash_after_sync_armed: u64,
    /// Nodes killed right after a step that synced.
    pub crashed_after_sync: u64,
}

impl Coverage {
    /// Field-wise sum, keeping maxima as maxima.
    pub fn add(&mut self, o: &Coverage) {
        macro_rules! sum {
            ($($f:ident),*) => { $(self.$f += o.$f;)* };
        }
        sum!(
            committed,
            acked,
            redirects,
            not_leader,
            client_timeouts,
            replaced,
            recoveries,
            torn_tails,
            disk_failures,
            recovery_failures,
            crashes,
            leader_crashes,
            pauses,
            leader_pauses,
            partitions,
            leader_partitions,
            one_way_cuts,
            torn_armed,
            crash_after_sync_armed,
            crashed_after_sync
        );
        self.max_term = self.max_term.max(o.max_term);
        self.nodes = add_stats(&self.nodes, &o.nodes);
    }
}

fn add_stats(a: &NodeStats, b: &NodeStats) -> NodeStats {
    NodeStats {
        prevotes_started: a.prevotes_started + b.prevotes_started,
        prevotes_refused_leader_alive: a.prevotes_refused_leader_alive
            + b.prevotes_refused_leader_alive,
        elections_started: a.elections_started + b.elections_started,
        elections_won: a.elections_won + b.elections_won,
        check_quorum_stepdowns: a.check_quorum_stepdowns + b.check_quorum_stepdowns,
        higher_term_stepdowns: a.higher_term_stepdowns + b.higher_term_stepdowns,
        stale_messages: a.stale_messages + b.stale_messages,
        truncations: a.truncations + b.truncations,
        appends_rejected: a.appends_rejected + b.appends_rejected,
        hint_jumps: a.hint_jumps + b.hint_jumps,
        largest_append: a.largest_append.max(b.largest_append),
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
    /// Simulated time from the start until every operation was done.
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
            "seed {} failed: {}\n  swarm: {:?}\n  replay: cargo run -- sim --raft --seed {} --trace",
            self.report.seed, self.message, self.report.swarm, self.report.seed
        )
    }
}

/// What travels in the Raft world.
#[derive(Clone, Hash)]
pub enum Msg {
    Raft(raft::Message),
    /// A client asks a node to append `data`.
    Propose {
        req: u64,
        data: Vec<u8>,
    },
    Proposed {
        req: u64,
        outcome: Outcome,
    },
}

#[derive(Clone, Debug, Hash)]
pub enum Outcome {
    /// Applied at `index` with the term it was proposed in.
    Committed {
        index: u64,
    },
    NotLeader {
        leader: Option<Id>,
    },
    /// Another leader's entry was applied at the proposal's index.
    Replaced,
}

impl Message for Msg {
    fn digest(&self) -> u64 {
        super::digest_of(self)
    }
}

fn text(data: &[u8]) -> String {
    if data.is_empty() {
        "no-op".into()
    } else {
        String::from_utf8_lossy(data).into_owned()
    }
}

impl fmt::Debug for Msg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use raft::Message as M;
        match self {
            Msg::Raft(M::PreVote {
                term,
                last_index,
                last_term,
            }) => write!(f, "prevote t{term} last={last_index}/t{last_term}"),
            Msg::Raft(M::PreVoteReply { term, granted }) => {
                write!(f, "prevote-reply t{term} granted={granted}")
            }
            Msg::Raft(M::Vote {
                term,
                last_index,
                last_term,
            }) => write!(f, "vote t{term} last={last_index}/t{last_term}"),
            Msg::Raft(M::VoteReply { term, granted }) => {
                write!(f, "vote-reply t{term} granted={granted}")
            }
            Msg::Raft(M::Append {
                term,
                prev_index,
                prev_term,
                entries,
                commit,
            }) => {
                let es: Vec<String> = entries
                    .iter()
                    .map(|e| format!("t{}:{}", e.term, text(&e.data)))
                    .collect();
                write!(
                    f,
                    "append t{term} prev={prev_index}/t{prev_term} commit={commit} [{}]",
                    es.join(" ")
                )
            }
            Msg::Raft(M::AppendReply { term, result }) => match result {
                AppendResult::Ok { matched } => write!(f, "append-ok t{term} matched={matched}"),
                AppendResult::Reject {
                    prev_index,
                    conflict_term,
                    first_index,
                } => write!(
                    f,
                    "append-reject t{term} prev={prev_index} conflict={conflict_term:?} first={first_index}"
                ),
            },
            Msg::Propose { req, data } => write!(f, "#{req} propose {}", text(data)),
            Msg::Proposed { req, outcome } => write!(f, "#{req} -> {outcome:?}"),
        }
    }
}

/// The checker of D62 and what the processes share with the run.
struct Shared {
    bug: Option<Bug>,
    members: Vec<Id>,
    next_id: u64,
    /// Who won each term: election safety.
    leaders: BTreeMap<u64, Id>,
    /// For every `(index, term)` any node ever persisted: a digest of the
    /// entry's data and the term of the entry before it. Log matching says
    /// these never differ between nodes.
    entries: BTreeMap<(u64, u64), (u64, u64)>,
    /// Every committed entry, by index: what some node applied there. No
    /// node may ever apply anything else at that index.
    ledger: Vec<Entry>,
    /// For each ledger entry, the term of the node that applied it first: the
    /// entry was committed in that term or before.
    committed_by: Vec<u64>,
    /// The last index each node applied in its current life.
    applied: BTreeMap<Id, u64>,
    /// The nodes that believe they lead, with their terms. Two can at once:
    /// a deposed leader that has not heard of the new term yet.
    leading: BTreeMap<Id, u64>,
    clients_done: BTreeSet<u32>,
    /// Nodes to kill right after their next step that syncs.
    crash_after_sync: BTreeSet<Id>,
    /// Node stats of finished lives, and of each node's current life.
    stats_done: NodeStats,
    stats_live: BTreeMap<Id, NodeStats>,
    failure: Option<String>,
    cov: Coverage,
}

type Sh = Rc<RefCell<Shared>>;

impl Shared {
    fn id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    fn fail(&mut self, message: String) {
        self.failure.get_or_insert(message);
    }

    /// Node `id` wrote `entries` at `index`, after an entry of `prev_term`.
    fn persisted(&mut self, id: Id, index: u64, prev_term: u64, entries: &[Entry]) {
        let mut prev = prev_term;
        for (k, e) in entries.iter().enumerate() {
            let i = index + k as u64;
            let mine = (super::digest(&e.data), prev);
            let theirs = *self.entries.entry((i, e.term)).or_insert(mine);
            if theirs != mine {
                self.fail(format!(
                    "log matching: n{id} has entry {i} of term {} ({}) after one of term {prev}, \
                     but another log has entry {i} of term {} with other contents or after one of term {}",
                    e.term,
                    text(&e.data),
                    e.term,
                    theirs.1
                ));
            }
            prev = e.term;
        }
    }

    /// Node `id`, at `term`, applied `entry` at `index`: state machine safety.
    fn apply(&mut self, id: Id, term: u64, index: u64, entry: &Entry) {
        let last = self.applied.insert(id, index).unwrap_or(0);
        if index != last + 1 {
            self.fail(format!(
                "n{id} applied entry {index} right after {last}: it lost committed entries"
            ));
        }
        match self.ledger.get(index as usize - 1) {
            Some(e) if e != entry => self.fail(format!(
                "state machine safety: n{id} applied t{}:{} at {index}, where t{}:{} was applied before",
                entry.term,
                text(&entry.data),
                e.term,
                text(&e.data)
            )),
            Some(_) => {}
            None => {
                self.ledger.push(entry.clone());
                self.committed_by.push(term);
                self.cov.committed += 1;
            }
        }
    }

    /// Node `id` became leader of `term` with `log`: election safety, and
    /// leader completeness for every entry committed in an earlier term. (A
    /// candidate held up by a pause or slow votes can win a term after a
    /// later term has committed entries; those it need not have.)
    fn elected(&mut self, id: Id, term: u64, log: &[Entry]) {
        if let Some(other) = self.leaders.insert(term, id)
            && other != id
        {
            self.fail(format!(
                "election safety: n{other} and n{id} both won term {term}"
            ));
        }
        if let Some(i) = (0..self.ledger.len())
            .find(|&i| self.committed_by[i] < term && log.get(i) != Some(&self.ledger[i]))
        {
            let e = &self.ledger[i];
            self.fail(format!(
                "leader completeness: n{id} won term {term} without committed entry {} (t{}:{})",
                i + 1,
                e.term,
                text(&e.data)
            ));
        }
        self.leading.insert(id, term);
        self.cov.max_term = self.cov.max_term.max(term);
    }
}

// ---------------------------------------------------------------- nodes

/// A Raft node: a [`Node`] over its log on the node's disk (D56, D58).
struct Server {
    sh: Sh,
    id: Id,
    disk: SimDisk,
    node: Option<Node>,
    log: Option<RaftLog<SimDisk>>,
    election_timer: u64,
    was_leader: bool,
    /// Proposals this node accepted as leader, by index: who to answer and
    /// the term the entry was proposed in.
    pending: BTreeMap<u64, (NodeId, u64, u64)>,
}

impl Server {
    /// Carry out the node's `Ready` (D56): persist and sync, then send, then
    /// apply, then restart the election timer if asked.
    fn drive(&mut self, ctx: &mut Ctx<'_, Msg>) {
        let (Some(node), Some(log)) = (&mut self.node, &mut self.log) else {
            return;
        };
        let ready = node.take_ready();
        if let Err(e) = log.write(&ready.persist) {
            let mut sh = self.sh.borrow_mut();
            sh.cov.disk_failures += 1;
            if !matches!(e, StoreError::Io(_)) {
                sh.fail(format!("n{} log write: {e}", self.id));
            }
            drop(sh);
            // Nothing that depends on the lost records may be sent (D58).
            self.node = None;
            ctx.halt();
            return;
        }
        let mut sh = self.sh.borrow_mut();
        for record in &ready.persist {
            if let raft::Record::Append { index, entries } = record {
                let prev_term = node.term_at(index - 1);
                sh.persisted(self.id, *index, prev_term, entries);
            }
        }
        for (to, msg) in ready.send {
            ctx.send(NodeId(to), Msg::Raft(msg));
        }
        for (index, entry) in &ready.apply {
            sh.apply(self.id, node.term(), *index, entry);
            if let Some((client, req, term)) = self.pending.remove(index) {
                let outcome = if entry.term == term {
                    Outcome::Committed { index: *index }
                } else {
                    Outcome::Replaced
                };
                ctx.send(client, Msg::Proposed { req, outcome });
            }
        }
        if ready.reset_election_timer {
            self.election_timer = sh.id();
            let after = ctx.rng().range(ELECTION.0.0, ELECTION.1.0);
            ctx.set_timer(Millis(after), self.election_timer);
        }
        let leader = node.role() == Role::Leader;
        if leader && !self.was_leader {
            sh.elected(self.id, node.term(), node.log());
        }
        if !leader {
            sh.leading.remove(&self.id);
        }
        self.was_leader = leader;
        sh.cov.max_term = sh.cov.max_term.max(node.term());
        sh.stats_live.insert(self.id, node.stats());
        // Dies after its messages left: what they promised must be on disk.
        if !ready.persist.is_empty() && sh.crash_after_sync.remove(&self.id) {
            sh.cov.crashed_after_sync += 1;
            ctx.halt();
        }
    }
}

impl Process<Msg> for Server {
    fn start(&mut self, ctx: &mut Ctx<'_, Msg>) {
        let mut sh = self.sh.borrow_mut();
        if let Some(stats) = sh.stats_live.remove(&self.id) {
            sh.stats_done = add_stats(&sh.stats_done, &stats);
        }
        sh.applied.insert(self.id, 0);
        sh.leading.remove(&self.id);
        match RaftLog::open(self.disk.clone()) {
            Ok((log, saved, opened)) => {
                sh.cov.recoveries += u64::from(opened.records > 0);
                sh.cov.torn_tails += u64::from(opened.torn);
                let node = Node::new(self.id, &sh.members, saved).with_bug(sh.bug);
                self.node = Some(node);
                self.log = Some(log);
            }
            Err(StoreError::Io(_)) => {
                sh.cov.recovery_failures += 1;
                ctx.halt();
                return;
            }
            Err(e) => {
                sh.fail(format!("n{} recovery: {e}", self.id));
                return;
            }
        }
        drop(sh);
        ctx.set_timer(HEARTBEAT, HEARTBEAT_TIMER);
        self.drive(ctx);
    }

    fn receive(&mut self, ctx: &mut Ctx<'_, Msg>, from: NodeId, msg: Msg) {
        let Some(node) = &mut self.node else {
            return;
        };
        match msg {
            Msg::Raft(m) => node.receive(from.0, m),
            Msg::Propose { req, data } => match node.propose(data) {
                Ok((index, term)) => {
                    self.pending.insert(index, (from, req, term));
                }
                Err(leader) => {
                    let outcome = Outcome::NotLeader { leader };
                    ctx.send(from, Msg::Proposed { req, outcome });
                }
            },
            Msg::Proposed { .. } => {}
        }
        self.drive(ctx);
    }

    fn timer(&mut self, ctx: &mut Ctx<'_, Msg>, id: u64) {
        let Some(node) = &mut self.node else {
            return;
        };
        if id == HEARTBEAT_TIMER {
            node.heartbeat();
            ctx.set_timer(HEARTBEAT, HEARTBEAT_TIMER);
        } else if id == self.election_timer {
            node.election_timeout();
        } else {
            return;
        }
        self.drive(ctx);
    }
}

/// What a client is waiting for.
#[derive(Clone, Copy, Debug)]
enum Wait {
    /// The pause before its next operation, or before a retry.
    Pause(u64),
    /// The answer to request `req`, until timer `timer`.
    Reply {
        req: u64,
        timer: u64,
    },
    Done,
}

/// Proposes its operations one at a time (D65), following redirects and
/// moving to another node on a timeout. Operations are paced over the fault
/// phase.
struct Client {
    sh: Sh,
    index: u32,
    nodes: u32,
    ops: u64,
    /// The operation in flight, from 1.
    op: u64,
    target: Id,
    wait: Wait,
}

impl Client {
    fn data(&self) -> Vec<u8> {
        format!("c{}.{}", self.index, self.op).into_bytes()
    }

    fn send(&mut self, ctx: &mut Ctx<'_, Msg>) {
        let (req, timer) = {
            let mut sh = self.sh.borrow_mut();
            (sh.id(), sh.id())
        };
        let data = self.data();
        ctx.send(NodeId(self.target), Msg::Propose { req, data });
        ctx.set_timer(CLIENT_TIMEOUT, timer);
        self.wait = Wait::Reply { req, timer };
    }

    fn pause(&mut self, ctx: &mut Ctx<'_, Msg>, max: u64) {
        let id = self.sh.borrow_mut().id();
        let after = ctx.rng().range(1, max);
        ctx.set_timer(Millis(after), id);
        self.wait = Wait::Pause(id);
    }

    fn next_node(&mut self) {
        self.target = (self.target + 1) % self.nodes;
    }
}

impl Process<Msg> for Client {
    fn start(&mut self, ctx: &mut Ctx<'_, Msg>) {
        self.target = ctx.rng().below(u64::from(self.nodes)) as Id;
        self.pause(ctx, 2_000);
    }

    fn receive(&mut self, ctx: &mut Ctx<'_, Msg>, _: NodeId, msg: Msg) {
        let Msg::Proposed { req, outcome } = msg else {
            return;
        };
        if !matches!(self.wait, Wait::Reply { req: r, .. } if r == req) {
            return; // late, or a duplicate
        }
        match outcome {
            Outcome::Committed { index } => {
                let data = self.data();
                let mut sh = self.sh.borrow_mut();
                sh.cov.acked += 1;
                match sh.ledger.get(index as usize - 1) {
                    Some(e) if e.data == data => {}
                    other => {
                        let other = other.map(|e| text(&e.data));
                        sh.fail(format!(
                            "{} acknowledged at {index}, but the ledger has {other:?} there",
                            text(&data)
                        ));
                    }
                }
                drop(sh);
                self.op += 1;
                if self.op > self.ops {
                    self.sh.borrow_mut().clients_done.insert(self.index);
                    self.wait = Wait::Done;
                } else {
                    // Paced so the operations span the fault phase.
                    self.pause(ctx, 2_000);
                }
            }
            Outcome::NotLeader { leader: Some(l) } if l != self.target => {
                self.sh.borrow_mut().cov.redirects += 1;
                self.target = l;
                self.send(ctx);
            }
            Outcome::NotLeader { .. } => {
                self.sh.borrow_mut().cov.not_leader += 1;
                self.next_node();
                self.pause(ctx, 100);
            }
            Outcome::Replaced => {
                self.sh.borrow_mut().cov.replaced += 1;
                self.send(ctx);
            }
        }
    }

    fn timer(&mut self, ctx: &mut Ctx<'_, Msg>, id: u64) {
        match self.wait {
            Wait::Pause(t) if t == id => self.send(ctx),
            Wait::Reply { timer, .. } if timer == id => {
                self.sh.borrow_mut().cov.client_timeouts += 1;
                self.next_node();
                self.send(ctx);
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------- the run

/// Run one seed: build the cluster, inject faults for `FAULT_PHASE`, heal,
/// wait until every operation is acknowledged and applied on every node, then
/// check that a leader is in charge.
pub fn run(options: &Options) -> Result<Report, Failure> {
    let mut world = World::<Msg>::new(options.seed);
    if options.trace {
        world.enable_trace();
    }
    let mut swarm = Swarm::pick(world.rng(), options.nodes);
    if let Some(fixed) = options.swarm {
        swarm = fixed;
    }
    let members: Vec<Id> = (0..swarm.nodes).collect();
    let sh: Sh = Rc::new(RefCell::new(Shared {
        bug: options.bug,
        members: members.clone(),
        next_id: 0,
        leaders: BTreeMap::new(),
        entries: BTreeMap::new(),
        ledger: Vec::new(),
        committed_by: Vec::new(),
        applied: BTreeMap::new(),
        leading: BTreeMap::new(),
        clients_done: BTreeSet::new(),
        crash_after_sync: BTreeSet::new(),
        stats_done: NodeStats::default(),
        stats_live: BTreeMap::new(),
        failure: None,
        cov: Coverage::default(),
    }));
    for &id in &members {
        let s = sh.clone();
        world.add(Box::new(move |_, disk| {
            Box::new(Server {
                sh: s.clone(),
                id,
                disk,
                node: None,
                log: None,
                election_timer: 0,
                was_leader: false,
                pending: BTreeMap::new(),
            })
        }));
    }
    for index in 0..options.clients {
        let (s, nodes, ops) = (sh.clone(), swarm.nodes, options.ops);
        world.add(Box::new(move |_, _| {
            Box::new(Client {
                sh: s.clone(),
                index,
                nodes,
                ops,
                op: 1,
                target: 0,
                wait: Wait::Done,
            })
        }));
    }

    world.net = swarm.net();
    // A halted node may come back at once, as a supervised process does.
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
                inject(&mut world, &sh, fault, swarm.nodes, &mut heal_at);
            }
            next_fault = now.plus(Millis(world.rng().range(100, 2_000)));
        }
        if quiet && done(&world, &sh, options, swarm.nodes) {
            break Ok(());
        }
        if now >= deadline {
            let s = sh.borrow();
            break Err(format!(
                "not finished {} ms after the faults stopped: clients done {}/{}, \
                 {} committed, applied {:?}, leading {:?}",
                FINISH_WITHIN.0,
                s.clients_done.len(),
                options.clients,
                s.ledger.len(),
                s.applied,
                s.leading
            ));
        }
    };
    let mut cov = sh.borrow().cov;
    {
        let s = sh.borrow();
        cov.nodes = s
            .stats_live
            .values()
            .fold(s.stats_done, |acc, st| add_stats(&acc, st));
    }
    let report = Report {
        seed: options.seed,
        hash: world.hash(),
        swarm,
        world: world.stats(),
        coverage: cov,
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

fn inject(world: &mut World<Msg>, sh: &Sh, fault: Fault, nodes: u32, heal_at: &mut Option<Time>) {
    let mut cov = sh.borrow().cov;
    let now = world.now();
    // Half the node faults go to the leader of the highest term, where they
    // hurt most (D63).
    let leader = {
        let s = sh.borrow();
        s.leading.iter().max_by_key(|&(_, t)| *t).map(|(&id, _)| id)
    };
    let victim = |world: &mut World<Msg>| -> (NodeId, bool) {
        match leader {
            Some(l) if world.rng().chance(MILLION / 2) => (NodeId(l), true),
            _ => {
                let n = NodeId(world.rng().below(u64::from(nodes)) as u32);
                (n, Some(n.0) == leader)
            }
        }
    };
    match fault {
        Fault::Partition => {
            if heal_at.is_none() {
                let (node, is_leader) = victim(world);
                world.isolate(node);
                *heal_at = Some(now.plus(Millis(world.rng().range(100, 3_000))));
                cov.partitions += 1;
                cov.leader_partitions += u64::from(is_leader);
            }
        }
        Fault::OneWayCut => {
            if heal_at.is_none() {
                let from = world.rng().below(u64::from(nodes)) as u32;
                let to = (from + 1 + world.rng().below(u64::from(nodes - 1)) as u32) % nodes;
                world.cut(NodeId(from), NodeId(to));
                *heal_at = Some(now.plus(Millis(world.rng().range(100, 3_000))));
                cov.one_way_cuts += 1;
            }
        }
        Fault::Crash => {
            let (node, is_leader) = victim(world);
            if world.is_up(node) {
                world.crash(node);
                // Half the restarts are quick, like a supervisor's, so a node
                // can be back within the election it crashed in.
                let after = if world.rng().chance(MILLION / 2) {
                    Millis(world.rng().range(10, 200))
                } else {
                    Millis(world.rng().range(200, 2_000))
                };
                world.restart_after(node, after);
                cov.crashes += 1;
                cov.leader_crashes += u64::from(is_leader);
            }
        }
        Fault::TornWrite => {
            let (node, _) = victim(world);
            let calls = world.rng().range(0, 5);
            world.disk(node).fail_in(calls);
            cov.torn_armed += 1;
        }
        Fault::CrashAfterSync => {
            let (node, _) = victim(world);
            sh.borrow_mut().crash_after_sync.insert(node.0);
            cov.crash_after_sync_armed += 1;
        }
        Fault::Pause => {
            let (node, is_leader) = victim(world);
            let until = now.plus(Millis(world.rng().range(100, 3_000)));
            world.pause(node, until);
            cov.pauses += 1;
            cov.leader_pauses += u64::from(is_leader);
        }
    }
    sh.borrow_mut().cov = cov;
}

/// Every client is done, every node is up and has applied every committed
/// entry, and some node leads.
fn done(world: &World<Msg>, sh: &Sh, options: &Options, nodes: u32) -> bool {
    let s = sh.borrow();
    s.clients_done.len() == options.clients as usize
        && !s.leading.is_empty()
        && (0..nodes).all(|n| {
            world.is_up(NodeId(n)) && s.applied.get(&n).copied() == Some(s.ledger.len() as u64)
        })
}
