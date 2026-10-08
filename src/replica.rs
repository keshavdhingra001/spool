//! The queue on Raft (M7): one replica is a [`raft::Node`], its log on a
//! [`Storage`] and the queue it applies committed entries to.
//!
//! ```text
//! requests ──propose(at, ops)──> Node ──entry──> RaftLog (synced) ──> peers
//!                                  │
//!                     committed ◄──┘ apply: decode the batch, run every
//!                                    command on the queue, answer the
//!                                    requests if the entry is ours (D70)
//! ```
//!
//! Like the node, a replica never reads a clock or draws a random number
//! (D4): the time arrives with each batch and the driver times elections. Its
//! only I/O is the log, synced before any message is handed back (D58). Every
//! input returns an [`Output`]: messages for peers, replies for requests, and
//! whether to restart the election timer. `T` is whatever the driver needs to
//! route a reply (a connection's reply channel, a simulated node and request
//! id).

use std::collections::BTreeMap;

use crate::codec::{self, DecodeError, Reader};
use crate::command::{Command, Event, Op};
use crate::error::StoreError;
use crate::ledger::Ledger;
use crate::queue::{Queue, Snapshot};
use crate::raft::store::{Opened, RaftLog};
use crate::raft::{self, Entry, Id, Message, Node, Role};
use crate::reference::ReferenceQueue;
use crate::storage::Storage;
use crate::types::Time;

/// The answer to one request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    /// The request's command applied and produced these events.
    Events(Vec<Event>),
    /// This replica does not lead; the leader it knows of, if any (D71).
    NotLeader(Option<Id>),
    /// The outcome is not known: the request's entry was replaced or the
    /// replica restarted (D70). Retrying is safe for every op (D72).
    Unknown,
}

/// A bug planted on purpose, to show the simulator finds it (D73).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bug {
    /// The leader answers as soon as the entry is in its own log, with the
    /// events of applying it to a copy of its state.
    ReplyBeforeCommit,
    /// The leader answers with whatever entry applied at the proposal's index,
    /// whatever its term.
    IgnoreTermOnReply,
}

/// What the driver must do after an input: send, deliver the replies, and
/// restart the election timer if asked. The log is already synced.
#[derive(Debug)]
pub struct Output<T> {
    pub send: Vec<(Id, Message)>,
    pub replies: Vec<(T, Reply)>,
    pub reset_election_timer: bool,
    /// Entries applied by this input, for the simulator's checks: index, term,
    /// the entry's data and, with checking on, a digest of the queue's state
    /// right after it.
    pub applied: Vec<Applied>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Applied {
    pub index: u64,
    pub term: u64,
    pub data: Vec<u8>,
    pub state: Option<Vec<u8>>,
}

impl<T> Default for Output<T> {
    fn default() -> Self {
        Output {
            send: Vec::new(),
            replies: Vec::new(),
            reset_election_timer: false,
            applied: Vec::new(),
        }
    }
}

/// Why a replica stopped: its log failed (it must restart and recover, D63), a
/// committed entry did not decode, or (with checking on) a broken invariant.
#[derive(Debug, thiserror::Error)]
pub enum ReplicaError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("committed entry {index} does not decode: {error}")]
    BadEntry { index: u64, error: DecodeError },
    #[error("invariant violated after entry {index}: {what}")]
    Invariant { index: u64, what: String },
}

/// Requests waiting for the entry they were proposed in.
struct Pending<T> {
    term: u64,
    tags: Vec<T>,
}

pub struct Replica<S: Storage, T> {
    node: Node,
    log: RaftLog<S>,
    queue: ReferenceQueue,
    /// Present when checking: both checkers after every applied entry.
    ledger: Option<Ledger>,
    applied: u64,
    /// By index: the requests in the entry this replica proposed there (D70).
    pending: BTreeMap<u64, Pending<T>>,
    bug: Option<Bug>,
}

impl<S: Storage, T> Replica<S, T> {
    /// Recover replica `id` of the cluster `members` from `storage`. Its queue
    /// starts empty and is rebuilt as entries are learned to be committed
    /// (D67). The first output asks for an election timer.
    pub fn open(
        id: Id,
        members: &[Id],
        storage: S,
        check: bool,
    ) -> Result<(Self, Opened), StoreError> {
        let (log, saved, opened) = RaftLog::open(storage)?;
        let replica = Replica {
            node: Node::new(id, members, saved),
            log,
            queue: ReferenceQueue::new(),
            ledger: check.then(Ledger::new),
            applied: 0,
            pending: BTreeMap::new(),
            bug: None,
        };
        Ok((replica, opened))
    }

    pub fn with_bugs(mut self, raft: Option<raft::Bug>, replica: Option<Bug>) -> Self {
        self.node.set_bug(raft);
        self.bug = replica;
        self
    }

    pub fn node(&self) -> &Node {
        &self.node
    }

    pub fn queue(&self) -> &ReferenceQueue {
        &self.queue
    }

    pub fn applied(&self) -> u64 {
        self.applied
    }

    pub fn storage(&self) -> &S {
        self.log.storage()
    }

    pub fn into_storage(self) -> S {
        self.log.into_storage()
    }

    /// Whatever the node asked for since it was opened (its first election
    /// timer).
    pub fn start(&mut self) -> Result<Output<T>, ReplicaError> {
        self.drive(Output::default())
    }

    /// Propose `requests` as one entry, every command at time `at` (D66). On a
    /// replica that does not lead, each gets `NotLeader`.
    pub fn propose(&mut self, at: Time, requests: Vec<(T, Op)>) -> Result<Output<T>, ReplicaError> {
        let mut out = Output::default();
        if requests.is_empty() {
            return Ok(out);
        }
        let (tags, ops): (Vec<T>, Vec<Op>) = requests.into_iter().unzip();
        let data = encode_batch(at, &ops);
        match self.node.propose(data) {
            Ok((index, term)) => {
                if self.bug == Some(Bug::ReplyBeforeCommit) {
                    let events = self.speculate(index)?;
                    out.replies.extend(
                        tags.into_iter()
                            .zip(events)
                            .map(|(t, e)| (t, Reply::Events(e))),
                    );
                } else if let Some(old) = self.pending.insert(index, Pending { term, tags }) {
                    // An earlier proposal of ours at this index was cut from
                    // the log while we did not lead: it never applies.
                    out.replies
                        .extend(old.tags.into_iter().map(|t| (t, Reply::Unknown)));
                }
            }
            Err(leader) => {
                out.replies
                    .extend(tags.into_iter().map(|t| (t, Reply::NotLeader(leader))));
            }
        }
        self.drive(out)
    }

    pub fn receive(&mut self, from: Id, msg: Message) -> Result<Output<T>, ReplicaError> {
        self.node.receive(from, msg);
        self.drive(Output::default())
    }

    pub fn election_timeout(&mut self) -> Result<Output<T>, ReplicaError> {
        self.node.election_timeout();
        self.drive(Output::default())
    }

    pub fn heartbeat(&mut self) -> Result<Output<T>, ReplicaError> {
        self.node.heartbeat();
        self.drive(Output::default())
    }

    /// Carry out the node's `Ready` (D56): sync its records, then hand back
    /// its messages, then apply what committed.
    fn drive(&mut self, mut out: Output<T>) -> Result<Output<T>, ReplicaError> {
        let ready = self.node.take_ready();
        self.log.write(&ready.persist)?;
        out.send = ready.send;
        out.reset_election_timer = ready.reset_election_timer;
        for (index, entry) in ready.apply {
            self.apply(index, entry, &mut out)?;
        }
        Ok(out)
    }

    fn apply(&mut self, index: u64, entry: Entry, out: &mut Output<T>) -> Result<(), ReplicaError> {
        let events = if entry.data.is_empty() {
            Vec::new() // the leader's no-op (D61)
        } else {
            let (at, ops) = decode_batch(&entry.data)
                .map_err(|error| ReplicaError::BadEntry { index, error })?;
            self.run(index, at, ops)?
        };
        self.applied = index;
        if let Some(p) = self.pending.remove(&index) {
            let ours = p.term == entry.term || self.bug == Some(Bug::IgnoreTermOnReply);
            if ours {
                let mut events = events.into_iter();
                out.replies.extend(
                    p.tags
                        .into_iter()
                        .map(|t| (t, events.next().map_or(Reply::Unknown, Reply::Events))),
                );
            } else {
                out.replies
                    .extend(p.tags.into_iter().map(|t| (t, Reply::Unknown)));
            }
        }
        let state = self.ledger.is_some().then(|| {
            let mut bytes = Vec::new();
            self.queue.encode_state(&mut bytes);
            bytes
        });
        out.applied.push(Applied {
            index,
            term: entry.term,
            data: entry.data,
            state,
        });
        Ok(())
    }

    /// Apply one batch's commands and return each one's events, checked if
    /// checking is on.
    fn run(&mut self, index: u64, at: Time, ops: Vec<Op>) -> Result<Vec<Vec<Event>>, ReplicaError> {
        let mut all = Vec::with_capacity(ops.len());
        for op in ops {
            let mut events = Vec::new();
            self.queue.apply(&Command { at, op }, &mut events);
            if let Some(ledger) = &mut self.ledger {
                let invariant = |what| ReplicaError::Invariant { index, what };
                self.queue.check_invariants().map_err(invariant)?;
                ledger.observe(&events).map_err(invariant)?;
                crate::check::agree(self.queue.counts(), ledger.counts()).map_err(invariant)?;
            }
            all.push(events);
        }
        Ok(all)
    }

    /// [`Bug::ReplyBeforeCommit`]: the events entry `index` would produce if
    /// every entry up to it committed.
    fn speculate(&self, index: u64) -> Result<Vec<Vec<Event>>, ReplicaError> {
        let mut queue = self.queue.clone();
        let mut last = Vec::new();
        for i in self.applied + 1..=index {
            let data = &self.node.log()[i as usize - 1].data;
            if data.is_empty() {
                continue;
            }
            let (at, ops) =
                decode_batch(data).map_err(|error| ReplicaError::BadEntry { index: i, error })?;
            last = ops
                .into_iter()
                .map(|op| {
                    let mut events = Vec::new();
                    queue.apply(&Command { at, op }, &mut events);
                    events
                })
                .collect();
        }
        Ok(last)
    }

    /// True while this replica leads.
    pub fn leading(&self) -> bool {
        self.node.role() == Role::Leader
    }
}

/// An entry's data (D66): `at:u64 count:u32 op*`.
pub fn encode_batch(at: Time, ops: &[Op]) -> Vec<u8> {
    let mut out = Vec::new();
    codec::put_u64(&mut out, at.0);
    codec::put_u32(&mut out, u32::try_from(ops.len()).expect("under 4G ops"));
    for op in ops {
        codec::encode_op(op, &mut out);
    }
    out
}

pub fn decode_batch(bytes: &[u8]) -> Result<(Time, Vec<Op>), DecodeError> {
    let mut r = Reader::new(bytes);
    let at = Time(r.u64()?);
    let count = r.u32()?;
    let mut ops = Vec::new();
    for _ in 0..count {
        ops.push(r.op()?);
    }
    r.finish()?;
    Ok((at, ops))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemStorage;
    use crate::types::{JobId, Millis, Payload, QueueName};

    type R = Replica<MemStorage, u32>;

    fn q() -> QueueName {
        QueueName::new("q").unwrap()
    }

    fn enqueue(n: u8) -> Op {
        Op::Enqueue {
            queue: q(),
            payload: Payload(vec![n]),
            delay: Millis(0),
            key: None,
        }
    }

    /// Three replicas with messages delivered by hand, in order, none lost.
    struct Cluster {
        replicas: Vec<R>,
        inbox: Vec<(Id, Id, Message)>,
        replies: Vec<(u32, Reply)>,
    }

    impl Cluster {
        fn new() -> Self {
            let members = [0, 1, 2];
            let replicas = members
                .iter()
                .map(|&id| R::open(id, &members, MemStorage::new(), true).unwrap().0)
                .collect();
            Cluster {
                replicas,
                inbox: Vec::new(),
                replies: Vec::new(),
            }
        }

        fn take(&mut self, from: Id, out: Output<u32>) {
            self.inbox
                .extend(out.send.into_iter().map(|(to, m)| (from, to, m)));
            self.replies.extend(out.replies);
        }

        fn settle(&mut self) {
            while !self.inbox.is_empty() {
                let (from, to, msg) = self.inbox.remove(0);
                let out = self.replicas[to as usize].receive(from, msg).unwrap();
                self.take(to, out);
            }
        }

        fn elect(&mut self, id: Id) {
            let out = self.replicas[id as usize].election_timeout().unwrap();
            self.take(id, out);
            self.settle();
            assert!(self.replicas[id as usize].leading());
        }

        fn propose(&mut self, id: Id, at: u64, requests: Vec<(u32, Op)>) {
            let out = self.replicas[id as usize]
                .propose(Time(at), requests)
                .unwrap();
            self.take(id, out);
        }

        fn heartbeat(&mut self, id: Id) {
            let out = self.replicas[id as usize].heartbeat().unwrap();
            self.take(id, out);
            self.settle();
        }
    }

    #[test]
    fn batches_round_trip() {
        let ops = vec![enqueue(1), Op::Tick, Op::Result { job: JobId(3) }];
        let bytes = encode_batch(Time(42), &ops);
        assert_eq!(decode_batch(&bytes).unwrap(), (Time(42), ops));
        assert_eq!(
            decode_batch(&bytes[..bytes.len() - 1]),
            Err(DecodeError::Truncated)
        );
        let mut long = bytes.clone();
        long.push(0);
        assert_eq!(decode_batch(&long), Err(DecodeError::TrailingBytes(1)));
    }

    #[test]
    fn a_batch_is_answered_once_committed_and_applied_everywhere() {
        let mut c = Cluster::new();
        c.elect(0);
        c.propose(0, 10, vec![(1, enqueue(1)), (2, enqueue(2))]);
        assert!(c.replies.is_empty(), "nothing answered before commit");
        c.settle();
        let jobs: Vec<_> = c
            .replies
            .iter()
            .map(|(t, r)| match r {
                Reply::Events(e) => (*t, e.clone()),
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(jobs.len(), 2);
        assert!(matches!(
            jobs[0].1[..],
            [Event::Enqueued { job: JobId(1), .. }]
        ));
        assert!(matches!(
            jobs[1].1[..],
            [Event::Enqueued { job: JobId(2), .. }]
        ));
        // Followers learn the commit with the next append.
        c.heartbeat(0);
        let states: Vec<Vec<u8>> = c
            .replicas
            .iter()
            .map(|r| {
                let mut b = Vec::new();
                r.queue().encode_state(&mut b);
                b
            })
            .collect();
        assert_eq!(states[0], states[1]);
        assert_eq!(states[0], states[2]);
        assert_eq!(c.replicas[2].queue().now(), Time(10));
    }

    #[test]
    fn a_follower_redirects_to_the_leader() {
        let mut c = Cluster::new();
        c.propose(1, 10, vec![(7, enqueue(1))]);
        assert_eq!(c.replies, [(7, Reply::NotLeader(None))]);
        c.replies.clear();
        c.elect(0);
        c.propose(1, 10, vec![(8, enqueue(1))]);
        assert_eq!(c.replies, [(8, Reply::NotLeader(Some(0)))]);
    }

    /// The leader of term 2 proposes at index 2 and loses the entry before it
    /// commits; the leader of term 3 commits its own entry there. The old
    /// leader applies that entry at index 2 and must not answer its client
    /// with someone else's events (D70).
    #[test]
    fn a_replaced_entry_is_answered_unknown() {
        for bug in [None, Some(Bug::IgnoreTermOnReply)] {
            let mut c = Cluster::new();
            for r in &mut c.replicas {
                let r2 =
                    std::mem::replace(r, R::open(9, &[9], MemStorage::new(), false).unwrap().0);
                *r = r2.with_bugs(None, bug);
            }
            c.elect(0);
            // n0's two proposals (indexes 2 and 3) reach nobody.
            for (tag, n) in [(1, 1), (3, 3)] {
                let out = c.replicas[0]
                    .propose(Time(10), vec![(tag, enqueue(n))])
                    .unwrap();
                assert!(out.replies.is_empty());
            }
            // n2 stops hearing n0, so it grants n1's pre-vote. n1 wins term 2
            // without n0's entries; its no-op takes index 2, its entry index 3.
            let _ = c.replicas[2].election_timeout().unwrap();
            c.elect(1);
            c.propose(1, 20, vec![(2, enqueue(2))]);
            c.settle();
            c.heartbeat(1);
            let first: Vec<_> = c.replies.iter().filter(|(t, _)| *t == 1).collect();
            assert_eq!(first, [&(1, Reply::Unknown)], "replaced by a no-op");
            let mine: Vec<_> = c.replies.iter().filter(|(t, _)| *t == 3).collect();
            match bug {
                None => assert_eq!(mine, [&(3, Reply::Unknown)]),
                Some(_) => assert!(
                    matches!(mine[..], [(3, Reply::Events(_))]),
                    "the planted bug answers with n1's events: {mine:?}"
                ),
            }
        }
    }

    #[test]
    fn reply_before_commit_answers_from_the_leaders_own_log() {
        let mut c = Cluster::new();
        c.replicas[0] = R::open(0, &[0, 1, 2], MemStorage::new(), true)
            .unwrap()
            .0
            .with_bugs(None, Some(Bug::ReplyBeforeCommit));
        c.elect(0);
        let out = c.replicas[0]
            .propose(Time(10), vec![(1, enqueue(1))])
            .unwrap();
        assert!(matches!(
            &out.replies[..],
            [(1, Reply::Events(e))] if matches!(e[..], [Event::Enqueued { job: JobId(1), .. }])
        ));
        assert_eq!(c.replicas[0].node().commit(), 1, "only the no-op committed");
    }

    #[test]
    fn a_restarted_replica_rebuilds_its_queue_from_the_log() {
        let mut c = Cluster::new();
        c.elect(0);
        c.propose(0, 10, vec![(1, enqueue(1))]);
        c.settle();
        c.heartbeat(0);
        let disk = c.replicas[2].storage().clone();
        let mut before = Vec::new();
        c.replicas[2].queue().encode_state(&mut before);
        let (mut r, opened) = R::open(2, &[0, 1, 2], disk, true).unwrap();
        assert!(opened.records > 0);
        assert_eq!(
            r.queue().counts().total(),
            0,
            "empty until commits are learned"
        );
        r.start().unwrap();
        c.replicas[2] = r;
        c.heartbeat(0);
        let mut after = Vec::new();
        c.replicas[2].queue().encode_state(&mut after);
        assert_eq!(before, after);
        assert_eq!(c.replicas[2].applied(), 2);
    }
}
