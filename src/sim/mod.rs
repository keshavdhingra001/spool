//! Deterministic simulation (D47): whole systems in one thread, driven by one
//! seed.
//!
//! Every participant is a [`Process`]: it reacts to a start, a message or a
//! timer, and acts only through its [`Ctx`] (send, set a timer, halt). The
//! [`World`] keeps one queue of pending events ordered by `(time, sequence)`
//! and executes them one at a time, so the order of that queue is the whole
//! schedule. The network (D49) and the disks (D50) are the world's: it decides
//! which messages are lost, duplicated or late, and what a crash leaves on disk.
//! Faults are applied by whoever drives the world, between calls to
//! [`World::run_until`].

pub mod disk;
pub mod queue;
pub mod raft;
pub mod rng;

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};
use std::fmt;

pub use disk::SimDisk;
pub use rng::{MILLION, Rng};

use crate::types::{Millis, Time};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u32);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "n{}", self.0)
    }
}

/// What travels between processes.
pub trait Message: Clone + fmt::Debug {
    /// A hash of the message's content, folded into the trace hash (D48).
    fn digest(&self) -> u64;
}

/// One participant (D47). It has no other view of the world than its `Ctx`.
pub trait Process<M> {
    /// Called when the node starts and after every restart.
    fn start(&mut self, ctx: &mut Ctx<'_, M>);
    fn receive(&mut self, ctx: &mut Ctx<'_, M>, from: NodeId, msg: M);
    /// A timer set with [`Ctx::set_timer`] in this incarnation fired.
    fn timer(&mut self, ctx: &mut Ctx<'_, M>, id: u64);
}

/// Builds a node's process at start and at every restart, from its disk.
pub type Factory<M> = Box<dyn FnMut(NodeId, SimDisk) -> Box<dyn Process<M>>>;

/// A process's handle on the world for the duration of one call.
pub struct Ctx<'a, M> {
    me: NodeId,
    now: Time,
    rng: &'a mut Rng,
    sends: Vec<(NodeId, M)>,
    timers: Vec<(Millis, u64)>,
    halt: bool,
}

impl<M> Ctx<'_, M> {
    pub fn me(&self) -> NodeId {
        self.me
    }

    /// The node's clock: simulated time plus the node's offset (D51).
    pub fn now(&self) -> Time {
        self.now
    }

    pub fn rng(&mut self) -> &mut Rng {
        self.rng
    }

    pub fn send(&mut self, to: NodeId, msg: M) {
        self.sends.push((to, msg));
    }

    /// Call `timer(id)` after `after` of simulated time, unless the node
    /// crashes first.
    pub fn set_timer(&mut self, after: Millis, id: u64) {
        self.timers.push((after, id));
    }

    /// Stop this node as if its process died: it crashes after this call and
    /// restarts after the world's halt delay (D38, D50).
    pub fn halt(&mut self) {
        self.halt = true;
    }
}

/// The network's behaviour (D49), changed freely between steps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Net {
    pub drop_ppm: u32,
    pub dup_ppm: u32,
    pub delay_min: Millis,
    pub delay_max: Millis,
    pub spike_ppm: u32,
    pub spike_max: Millis,
}

impl Default for Net {
    /// A reliable network with 1–10 ms of delay.
    fn default() -> Self {
        Net {
            drop_ppm: 0,
            dup_ppm: 0,
            delay_min: Millis(1),
            delay_max: Millis(10),
            spike_ppm: 0,
            spike_max: Millis(0),
        }
    }
}

/// What the world did, for coverage assertions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorldStats {
    pub events: u64,
    pub sent: u64,
    pub delivered: u64,
    pub dropped: u64,
    pub duplicated: u64,
    pub spiked: u64,
    /// Lost to a cut link, at send or at delivery.
    pub cut: u64,
    /// Lost because the receiver was down.
    pub to_down: u64,
    pub crashes: u64,
    pub halts: u64,
    pub restarts: u64,
    pub pauses: u64,
    /// Events held back because their node was paused.
    pub held: u64,
    /// The largest number of crash images one crash chose from.
    pub most_images: usize,
}

enum Kind<M> {
    Deliver { from: NodeId, to: NodeId, msg: M },
    Timer { node: NodeId, life: u64, id: u64 },
    Restart { node: NodeId, life: u64 },
}

struct Event<M> {
    at: Time,
    seq: u64,
    kind: Kind<M>,
}

impl<M> PartialEq for Event<M> {
    fn eq(&self, other: &Self) -> bool {
        (self.at, self.seq) == (other.at, other.seq)
    }
}
impl<M> Eq for Event<M> {}
impl<M> PartialOrd for Event<M> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<M> Ord for Event<M> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.at, self.seq).cmp(&(other.at, other.seq))
    }
}

struct Node<M> {
    factory: Factory<M>,
    process: Option<Box<dyn Process<M>>>,
    disk: SimDisk,
    /// Bumped at every crash; timers and restarts of an older life are void.
    life: u64,
    /// Added to simulated time to give the node's clock (D51).
    offset: i64,
    paused_until: Option<Time>,
}

/// Where simulated time starts: late enough that a node's clock can be moved
/// back minutes without going below zero.
pub const START: Time = Time(1_000_000_000);

pub struct World<M> {
    now: Time,
    seq: u64,
    queue: BinaryHeap<Reverse<Event<M>>>,
    nodes: Vec<Node<M>>,
    rng: Rng,
    pub net: Net,
    cuts: BTreeSet<(NodeId, NodeId)>,
    /// How long a halted node stays down before it restarts.
    pub halt_downtime: (Millis, Millis),
    stats: WorldStats,
    hash: u64,
    trace: Option<Vec<String>>,
}

impl<M: Message> World<M> {
    pub fn new(seed: u64) -> Self {
        World {
            now: START,
            seq: 0,
            queue: BinaryHeap::new(),
            nodes: Vec::new(),
            rng: Rng::new(seed),
            net: Net::default(),
            cuts: BTreeSet::new(),
            halt_downtime: (Millis(100), Millis(1_000)),
            stats: WorldStats::default(),
            hash: FNV_OFFSET,
            trace: None,
        }
    }

    /// Record a line for every executed event (D55).
    pub fn enable_trace(&mut self) {
        self.trace = Some(Vec::new());
    }

    pub fn take_trace(&mut self) -> Vec<String> {
        self.trace.as_mut().map(std::mem::take).unwrap_or_default()
    }

    /// Add a node with an empty disk and start it.
    pub fn add(&mut self, factory: Factory<M>) -> NodeId {
        let id = NodeId(self.nodes.len() as u32);
        self.nodes.push(Node {
            factory,
            process: None,
            disk: SimDisk::new(),
            life: 0,
            offset: 0,
            paused_until: None,
        });
        self.start(id);
        id
    }

    pub fn now(&self) -> Time {
        self.now
    }

    pub fn rng(&mut self) -> &mut Rng {
        &mut self.rng
    }

    pub fn stats(&self) -> WorldStats {
        self.stats
    }

    /// The trace hash (D48): equal for two runs exactly when they executed
    /// the same events (up to hash collisions).
    pub fn hash(&self) -> u64 {
        self.hash
    }

    pub fn disk(&self, node: NodeId) -> &SimDisk {
        &self.nodes[node.0 as usize].disk
    }

    pub fn is_up(&self, node: NodeId) -> bool {
        self.nodes[node.0 as usize].process.is_some()
    }

    /// Execute every event due at or before `until`, then set the time to it.
    pub fn run_until(&mut self, until: Time) {
        while self.queue.peek().is_some_and(|Reverse(e)| e.at <= until) {
            self.step();
        }
        self.now = self.now.max(until);
    }

    /// Execute the next event. False if there is none.
    pub fn step(&mut self) -> bool {
        let Some(Reverse(event)) = self.queue.pop() else {
            return false;
        };
        self.now = event.at;
        // A paused node's events wait until it wakes, in their order.
        if let Some(node) = self.target(&event.kind)
            && let Some(until) = self.nodes[node.0 as usize].paused_until
        {
            if self.now < until {
                self.stats.held += 1;
                self.push(until, event.kind);
                return true;
            }
            self.nodes[node.0 as usize].paused_until = None;
        }
        self.stats.events += 1;
        match event.kind {
            Kind::Deliver { from, to, msg } => {
                if self.cuts.contains(&(from, to)) {
                    self.stats.cut += 1;
                    self.record(format_args!("cut {from}->{to} {msg:?}"), 1, msg.digest());
                } else if !self.is_up(to) {
                    self.stats.to_down += 1;
                    self.record(format_args!("lost {from}->{to} {msg:?}"), 2, msg.digest());
                } else {
                    self.stats.delivered += 1;
                    self.record(format_args!("recv {from}->{to} {msg:?}"), 3, msg.digest());
                    self.call(to, |p, ctx| p.receive(ctx, from, msg));
                }
            }
            Kind::Timer { node, life, id } => {
                let n = &self.nodes[node.0 as usize];
                if n.life == life && n.process.is_some() {
                    self.record(format_args!("timer {node} {id}"), 4, id);
                    self.call(node, |p, ctx| p.timer(ctx, id));
                }
            }
            Kind::Restart { node, life } => {
                let n = &self.nodes[node.0 as usize];
                if n.life == life && n.process.is_none() {
                    self.start(node);
                }
            }
        }
        true
    }

    /// Kill `node` now: its process and pending timers are gone and its disk
    /// becomes one of its crash images (D50). It stays down until restarted.
    pub fn crash(&mut self, node: NodeId) {
        let n = &mut self.nodes[node.0 as usize];
        if n.process.is_none() {
            return;
        }
        n.process = None;
        n.life += 1;
        n.paused_until = None;
        let images = n.disk.crash(&mut self.rng);
        self.stats.crashes += 1;
        self.stats.most_images = self.stats.most_images.max(images);
        self.record(
            format_args!("crash {node} ({images} images)"),
            5,
            images as u64,
        );
    }

    /// Restart `node` after `after`, if it is still down by then.
    pub fn restart_after(&mut self, node: NodeId, after: Millis) {
        let life = self.nodes[node.0 as usize].life;
        self.push(self.now.plus(after), Kind::Restart { node, life });
    }

    /// Freeze `node` until `until`: what reaches it meanwhile waits (D52).
    pub fn pause(&mut self, node: NodeId, until: Time) {
        self.stats.pauses += 1;
        self.nodes[node.0 as usize].paused_until = Some(until);
        let at = until.0 - START.0;
        self.record(format_args!("pause {node} until {at}"), 6, until.0);
    }

    /// Move `node`'s clock by `ms` (negative is back).
    pub fn shift_clock(&mut self, node: NodeId, ms: i64) {
        self.nodes[node.0 as usize].offset += ms;
        self.record(format_args!("clock {node} {ms:+}"), 7, ms as u64);
    }

    /// Cut every link between `node` and the rest, both ways.
    pub fn isolate(&mut self, node: NodeId) {
        for other in 0..self.nodes.len() as u32 {
            let other = NodeId(other);
            if other != node {
                self.cuts.insert((node, other));
                self.cuts.insert((other, node));
            }
        }
        self.record(format_args!("isolate {node}"), 8, u64::from(node.0));
    }

    /// Cut the link from `from` to `to` only: one-way loss (D63).
    pub fn cut(&mut self, from: NodeId, to: NodeId) {
        self.cuts.insert((from, to));
        self.record(
            format_args!("cut {from}->{to}"),
            11,
            u64::from(from.0) << 32 | u64::from(to.0),
        );
    }

    pub fn heal(&mut self) {
        self.cuts.clear();
        self.record(format_args!("heal"), 9, 0);
    }

    fn start(&mut self, node: NodeId) {
        let n = &mut self.nodes[node.0 as usize];
        let process = (n.factory)(node, n.disk.clone());
        n.process = Some(process);
        if self.stats.events > 0 || n.life > 0 {
            self.stats.restarts += 1;
        }
        self.record(format_args!("start {node}"), 10, u64::from(node.0));
        self.call(node, |p, ctx| p.start(ctx));
    }

    fn target(&self, kind: &Kind<M>) -> Option<NodeId> {
        match kind {
            Kind::Deliver { to, .. } => Some(*to),
            Kind::Timer { node, .. } => Some(*node),
            Kind::Restart { .. } => None,
        }
    }

    /// Run one entry point of `node`'s process, then carry out what it asked.
    fn call(&mut self, node: NodeId, f: impl FnOnce(&mut dyn Process<M>, &mut Ctx<'_, M>)) {
        let n = &mut self.nodes[node.0 as usize];
        let Some(mut process) = n.process.take() else {
            return;
        };
        let local = self.now.0.saturating_add_signed(n.offset);
        let mut ctx = Ctx {
            me: node,
            now: Time(local),
            rng: &mut self.rng,
            sends: Vec::new(),
            timers: Vec::new(),
            halt: false,
        };
        f(process.as_mut(), &mut ctx);
        let Ctx {
            sends,
            timers,
            halt,
            ..
        } = ctx;
        self.nodes[node.0 as usize].process = Some(process);
        let life = self.nodes[node.0 as usize].life;
        for (after, id) in timers {
            self.push(self.now.plus(after), Kind::Timer { node, life, id });
        }
        for (to, msg) in sends {
            self.send(node, to, msg);
        }
        if halt {
            self.stats.halts += 1;
            self.crash(node);
            let (lo, hi) = self.halt_downtime;
            let after = Millis(self.rng.range(lo.0, hi.0));
            self.restart_after(node, after);
        }
    }

    /// The network's decision for one message (D49).
    fn send(&mut self, from: NodeId, to: NodeId, msg: M) {
        self.stats.sent += 1;
        if self.cuts.contains(&(from, to)) {
            self.stats.cut += 1;
            return;
        }
        if self.rng.chance(self.net.drop_ppm) {
            self.stats.dropped += 1;
            return;
        }
        let copies = if self.rng.chance(self.net.dup_ppm) {
            self.stats.duplicated += 1;
            2
        } else {
            1
        };
        for _ in 0..copies {
            let mut delay = self.rng.range(self.net.delay_min.0, self.net.delay_max.0);
            if self.rng.chance(self.net.spike_ppm) {
                self.stats.spiked += 1;
                delay += self.rng.range(0, self.net.spike_max.0);
            }
            let msg = msg.clone();
            self.push(
                self.now.plus(Millis(delay)),
                Kind::Deliver { from, to, msg },
            );
        }
    }

    fn push(&mut self, at: Time, kind: Kind<M>) {
        self.seq += 1;
        self.queue.push(Reverse(Event {
            at,
            seq: self.seq,
            kind,
        }));
    }

    /// Fold one executed event into the hash, and the trace if it is on.
    fn record(&mut self, line: fmt::Arguments<'_>, kind: u64, detail: u64) {
        for word in [self.now.0, kind, detail] {
            self.hash = fnv(self.hash, &word.to_le_bytes());
        }
        if let Some(trace) = &mut self.trace {
            trace.push(format!("{:>8} {line}", self.now.0 - START.0));
        }
    }
}

const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;

/// FNV-1a over `bytes`, continuing from `hash`.
pub fn fnv(mut hash: u64, bytes: &[u8]) -> u64 {
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    hash
}

/// FNV-1a of `bytes` from the standard offset.
pub fn digest(bytes: &[u8]) -> u64 {
    fnv(FNV_OFFSET, bytes)
}

/// FNV-1a of any hashable value, fed through `Hash`: the same in every run,
/// unlike a randomly keyed hasher (D48).
pub fn digest_of<T: std::hash::Hash>(value: &T) -> u64 {
    struct Fnv(u64);
    impl std::hash::Hasher for Fnv {
        fn write(&mut self, bytes: &[u8]) {
            self.0 = fnv(self.0, bytes);
        }
        fn finish(&self) -> u64 {
            self.0
        }
    }
    let mut h = Fnv(FNV_OFFSET);
    value.hash(&mut h);
    std::hash::Hasher::finish(&h)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[derive(Clone, Debug)]
    struct Ping(u64);

    impl Message for Ping {
        fn digest(&self) -> u64 {
            self.0
        }
    }

    type Log = Rc<RefCell<Vec<(u64, u32, u64)>>>;

    /// Node 0 sends `n` pings to node 1 at start and on every timer; node 1
    /// logs what arrives with the time it arrived.
    struct Pinger {
        log: Log,
        next: u64,
    }

    impl Process<Ping> for Pinger {
        fn start(&mut self, ctx: &mut Ctx<'_, Ping>) {
            if ctx.me() == NodeId(0) {
                ctx.set_timer(Millis(5), 0);
            }
        }
        fn receive(&mut self, ctx: &mut Ctx<'_, Ping>, from: NodeId, msg: Ping) {
            let at = ctx.now().0 - START.0;
            self.log.borrow_mut().push((at, from.0, msg.0));
        }
        fn timer(&mut self, ctx: &mut Ctx<'_, Ping>, _: u64) {
            self.next += 1;
            ctx.send(NodeId(1), Ping(self.next));
            if self.next < 50 {
                ctx.set_timer(Millis(5), 0);
            }
        }
    }

    fn world(seed: u64, net: Net) -> (World<Ping>, Log) {
        let log: Log = Rc::default();
        let mut w = World::new(seed);
        w.net = net;
        for _ in 0..2 {
            let log = log.clone();
            w.add(Box::new(move |_, _| {
                Box::new(Pinger {
                    log: log.clone(),
                    next: 0,
                })
            }));
        }
        (w, log)
    }

    #[test]
    fn same_seed_same_run_other_seed_other_run() {
        let net = Net {
            drop_ppm: 100_000,
            dup_ppm: 100_000,
            delay_min: Millis(1),
            delay_max: Millis(40),
            spike_ppm: 50_000,
            spike_max: Millis(500),
        };
        let run = |seed| {
            let (mut w, log) = world(seed, net);
            w.run_until(START.plus(Millis(10_000)));
            let log = log.borrow().clone();
            (w.hash(), log, w.stats())
        };
        let (h1, log1, s1) = run(1);
        let (h2, log2, _) = run(1);
        assert_eq!((h1, &log1), (h2, &log2));
        let (h3, log3, _) = run(2);
        assert_ne!(h1, h3);
        assert_ne!(log1, log3);
        assert!(
            s1.dropped > 0 && s1.duplicated > 0 && s1.spiked > 0,
            "{s1:?}"
        );
        // Independent delays reorder messages.
        assert!(log1.windows(2).any(|w| w[0].2 > w[1].2), "no reordering");
    }

    #[test]
    fn cuts_drop_in_flight_messages_and_heal_restores() {
        let (mut w, log) = world(5, Net::default());
        w.run_until(START.plus(Millis(100)));
        let before = log.borrow().len();
        assert!(before > 0);
        w.isolate(NodeId(1));
        w.run_until(START.plus(Millis(150)));
        assert_eq!(log.borrow().len(), before, "nothing crosses a cut");
        assert!(w.stats().cut > 0);
        w.heal();
        w.run_until(START.plus(Millis(200)));
        assert!(log.borrow().len() > before);
    }

    #[test]
    fn a_paused_node_receives_late_in_order_and_a_crashed_one_not_at_all() {
        let (mut w, log) = world(9, Net::default());
        w.run_until(START.plus(Millis(50)));
        w.pause(NodeId(1), START.plus(Millis(120)));
        w.run_until(START.plus(Millis(119)));
        let n = log.borrow().len();
        w.run_until(START.plus(Millis(120)));
        let after: Vec<_> = log.borrow()[n..].to_vec();
        assert!(after.len() > 5, "held messages arrive at wake-up");
        assert!(after.iter().all(|e| e.0 == 120));
        assert!(w.stats().held > 0);

        w.crash(NodeId(1));
        let n = log.borrow().len();
        w.run_until(START.plus(Millis(150)));
        assert_eq!(log.borrow().len(), n);
        assert!(w.stats().to_down > 0);
        w.restart_after(NodeId(1), Millis(10));
        w.run_until(START.plus(Millis(200)));
        assert!(log.borrow().len() > n);
        assert_eq!(w.stats().restarts, 1);
    }

    #[test]
    fn timers_of_a_crashed_life_never_fire() {
        let (mut w, log) = world(4, Net::default());
        w.run_until(START.plus(Millis(12)));
        w.crash(NodeId(0));
        w.restart_after(NodeId(0), Millis(1_000));
        w.run_until(START.plus(Millis(1_000)));
        let n = log.borrow().len();
        assert!(n <= 2, "pinger stopped at its crash: {n}");
        w.run_until(START.plus(Millis(2_000)));
        assert!(log.borrow().len() > n, "and starts again after restart");
    }

    #[test]
    fn a_node_clock_is_shifted_simulated_time() {
        struct Clock(Rc<RefCell<Vec<i64>>>);
        impl Process<Ping> for Clock {
            fn start(&mut self, ctx: &mut Ctx<'_, Ping>) {
                ctx.set_timer(Millis(10), 0);
            }
            fn receive(&mut self, _: &mut Ctx<'_, Ping>, _: NodeId, _: Ping) {}
            fn timer(&mut self, ctx: &mut Ctx<'_, Ping>, _: u64) {
                self.0
                    .borrow_mut()
                    .push(ctx.now().0 as i64 - START.0 as i64);
                ctx.set_timer(Millis(10), 0);
            }
        }
        let seen = Rc::new(RefCell::new(Vec::new()));
        let mut w = World::<Ping>::new(0);
        let s = seen.clone();
        let n = w.add(Box::new(move |_, _| Box::new(Clock(s.clone()))));
        w.run_until(START.plus(Millis(10)));
        w.shift_clock(n, 1_000);
        w.run_until(START.plus(Millis(20)));
        w.shift_clock(n, -1_500);
        w.run_until(START.plus(Millis(30)));
        assert_eq!(*seen.borrow(), vec![10, 1_020, -470]);
    }
}
