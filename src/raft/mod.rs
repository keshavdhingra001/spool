//! Raft (M6): leader election, log replication and commit, written from the
//! paper (Ongaro and Ousterhout, 2014) and the thesis (Ongaro, 2014).
//!
//! [`Node`] is a pure state machine (D56): it is told that a message arrived,
//! that its election timer fired, that it is time to heartbeat, or that a
//! client proposes an entry, and it answers with a [`Ready`]: records to make
//! durable, messages to send, committed entries to apply, and whether to
//! restart the election timer. It never reads a clock, draws a random number
//! or touches a disk (D4); the driver does all three (D59), and must make
//! `persist` durable before it sends anything in the same `Ready` (D58).
//!
//! On top of the basic algorithm the node has PreVote and CheckQuorum (D57),
//! conflict hints on rejected appends (D60), and the current-term commit rule
//! with a no-op entry at the start of every term (D61).

pub mod store;

use std::collections::{BTreeMap, BTreeSet};

/// A node's id within its cluster.
pub type Id = u32;

/// Entries sent in one append at most (D60).
pub const MAX_APPEND: usize = 64;

/// One log entry. Empty `data` is the no-op a leader appends when it is
/// elected (D61); clients propose non-empty data.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Entry {
    pub term: u64,
    pub data: Vec<u8>,
}

/// What nodes send each other.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Message {
    /// "Could I win an election at `term`?" Changes nothing on the receiver
    /// (D57): `term` is the sender's current term plus one.
    PreVote {
        term: u64,
        last_index: u64,
        last_term: u64,
    },
    /// Granted: `term` is the term asked about. Refused: the voter's term.
    PreVoteReply {
        term: u64,
        granted: bool,
    },
    Vote {
        term: u64,
        last_index: u64,
        last_term: u64,
    },
    VoteReply {
        term: u64,
        granted: bool,
    },
    /// Entries after `prev_index`, and the leader's commit index. With no
    /// entries it is a heartbeat.
    Append {
        term: u64,
        prev_index: u64,
        prev_term: u64,
        entries: Vec<Entry>,
        commit: u64,
    },
    AppendReply {
        term: u64,
        result: AppendResult,
    },
}

impl Message {
    pub fn term(&self) -> u64 {
        match *self {
            Message::PreVote { term, .. }
            | Message::PreVoteReply { term, .. }
            | Message::Vote { term, .. }
            | Message::VoteReply { term, .. }
            | Message::Append { term, .. }
            | Message::AppendReply { term, .. } => term,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AppendResult {
    /// The follower's log now matches the leader's up to `matched`.
    Ok { matched: u64 },
    /// The entry at `prev_index` did not match (D60). If the follower has an
    /// entry there, `conflict_term` is its term and `first_index` the first
    /// index of that term in the follower's log; if its log is shorter,
    /// `conflict_term` is `None` and `first_index` is its length plus one.
    Reject {
        prev_index: u64,
        conflict_term: Option<u64>,
        first_index: u64,
    },
}

/// What must be durable (D58), in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record {
    HardState {
        term: u64,
        vote: Option<Id>,
    },
    /// `entries` go at `index`, `index + 1`, ...; `index` is always one past
    /// the end of the log as the earlier records leave it.
    Append {
        index: u64,
        entries: Vec<Entry>,
    },
    /// Drop the entries from `from` on.
    Truncate {
        from: u64,
    },
}

/// What a node needs back after a restart: its last durable term and vote and
/// its log. Commit index and applied index start at zero and are learned again.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Saved {
    pub term: u64,
    pub vote: Option<Id>,
    pub log: Vec<Entry>,
}

impl Saved {
    /// Apply one record, as the node did before it emitted it. Errors on a
    /// record that does not fit the log, which a correct node never writes.
    pub fn replay(&mut self, record: Record) -> Result<(), String> {
        match record {
            Record::HardState { term, vote } => {
                if term < self.term {
                    return Err(format!("term goes back from {} to {term}", self.term));
                }
                self.term = term;
                self.vote = vote;
            }
            Record::Append { index, entries } => {
                if index != self.log.len() as u64 + 1 {
                    return Err(format!(
                        "append at {index} to a log of {} entries",
                        self.log.len()
                    ));
                }
                self.log.extend(entries);
            }
            Record::Truncate { from } => {
                if from == 0 || from > self.log.len() as u64 + 1 {
                    return Err(format!(
                        "truncate from {from} in a log of {} entries",
                        self.log.len()
                    ));
                }
                self.log.truncate(from as usize - 1);
            }
        }
        Ok(())
    }
}

/// What the driver must do after a call, in this order: make `persist`
/// durable, then send, then apply; then restart the election timer if asked.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Ready {
    pub persist: Vec<Record>,
    pub send: Vec<(Id, Message)>,
    /// Newly committed entries with their indexes, in order.
    pub apply: Vec<(u64, Entry)>,
    /// Draw a new election timeout and restart the timer (D59).
    pub reset_election_timer: bool,
}

/// A bug planted on purpose, to show the simulator finds it (D64).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bug {
    /// A granted vote is not written to disk, so after a crash the node can
    /// vote again in the same term.
    VoteNotPersisted,
    /// A leader counts replicas for entries of earlier terms too (breaks D61).
    CommitOldTerm,
    /// A follower keeps its entries that conflict with the leader's.
    NoLogTruncate,
    /// A node takes appends from a leader of an older term.
    StaleTermAccept,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

/// Counters for coverage assertions; they change nothing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NodeStats {
    pub prevotes_started: u64,
    /// Pre-votes this node refused because it still hears a leader.
    pub prevotes_refused_leader_alive: u64,
    pub elections_started: u64,
    pub elections_won: u64,
    /// A leader stepped down because it lost contact with a majority.
    pub check_quorum_stepdowns: u64,
    /// A leader or candidate stepped down on seeing a higher term.
    pub higher_term_stepdowns: u64,
    pub stale_messages: u64,
    pub truncations: u64,
    pub appends_rejected: u64,
    /// Rejections after which `next_index` moved back more than one entry.
    pub hint_jumps: u64,
    pub largest_append: u64,
}

/// What a leader knows about each follower.
#[derive(Clone, Debug)]
struct Progress {
    /// The next index to send.
    next: BTreeMap<Id, u64>,
    /// The highest index known to match.
    matched: BTreeMap<Id, u64>,
    /// Followers heard from since the last CheckQuorum (D57).
    active: BTreeSet<Id>,
}

#[derive(Clone, Debug)]
enum State {
    Follower,
    PreCandidate { granted: BTreeSet<Id> },
    Candidate { granted: BTreeSet<Id> },
    Leader(Progress),
}

pub struct Node {
    id: Id,
    /// The other members, sorted.
    peers: Vec<Id>,
    term: u64,
    vote: Option<Id>,
    /// Entry `i` (1-based) is `log[i - 1]`.
    log: Vec<Entry>,
    commit: u64,
    applied: u64,
    state: State,
    leader: Option<Id>,
    /// This node heard from the leader of its term since its election timer
    /// last fired, so it refuses pre-votes (D57).
    leader_alive: bool,
    bug: Option<Bug>,
    stats: NodeStats,
    ready: Ready,
}

impl Node {
    /// A node of the cluster `members` (which includes `id`), from what it
    /// saved before. It starts as a follower and asks for an election timer.
    pub fn new(id: Id, members: &[Id], saved: Saved) -> Node {
        let mut peers: Vec<Id> = members.iter().copied().filter(|&m| m != id).collect();
        peers.sort_unstable();
        peers.dedup();
        let mut node = Node {
            id,
            peers,
            term: saved.term,
            vote: saved.vote,
            log: saved.log,
            commit: 0,
            applied: 0,
            state: State::Follower,
            leader: None,
            leader_alive: false,
            bug: None,
            stats: NodeStats::default(),
            ready: Ready::default(),
        };
        node.ready.reset_election_timer = true;
        node
    }

    pub fn with_bug(mut self, bug: Option<Bug>) -> Node {
        self.bug = bug;
        self
    }

    pub fn id(&self) -> Id {
        self.id
    }

    pub fn term(&self) -> u64 {
        self.term
    }

    pub fn vote(&self) -> Option<Id> {
        self.vote
    }

    pub fn log(&self) -> &[Entry] {
        &self.log
    }

    pub fn commit(&self) -> u64 {
        self.commit
    }

    pub fn leader(&self) -> Option<Id> {
        self.leader
    }

    pub fn stats(&self) -> NodeStats {
        self.stats
    }

    pub fn role(&self) -> Role {
        match self.state {
            State::Follower => Role::Follower,
            State::PreCandidate { .. } => Role::PreCandidate,
            State::Candidate { .. } => Role::Candidate,
            State::Leader(_) => Role::Leader,
        }
    }

    pub fn last_index(&self) -> u64 {
        self.log.len() as u64
    }

    /// The term of entry `index`; 0 for index 0. `index` must be in the log.
    pub fn term_at(&self, index: u64) -> u64 {
        match index {
            0 => 0,
            i => self.log[i as usize - 1].term,
        }
    }

    fn last_term(&self) -> u64 {
        self.term_at(self.last_index())
    }

    /// Votes needed to win, and replicas needed to commit.
    fn quorum(&self) -> usize {
        let size = self.peers.len() + 1;
        size / 2 + 1
    }

    /// Everything the calls since the last `take_ready` asked of the driver,
    /// with the entries committed since then.
    pub fn take_ready(&mut self) -> Ready {
        if self.commit > self.applied {
            for i in self.applied + 1..=self.commit {
                let entry = self.log[i as usize - 1].clone();
                self.ready.apply.push((i, entry));
            }
            self.applied = self.commit;
        }
        std::mem::take(&mut self.ready)
    }

    // ------------------------------------------------------------ inputs

    /// The election timer fired. A leader runs CheckQuorum; anyone else
    /// stops believing in its leader and starts a pre-vote (D57).
    pub fn election_timeout(&mut self) {
        self.ready.reset_election_timer = true;
        if let State::Leader(progress) = &mut self.state {
            let heard = progress.active.len() + 1;
            progress.active.clear();
            if heard < self.quorum() {
                self.stats.check_quorum_stepdowns += 1;
                self.state = State::Follower;
                self.leader = None;
                self.leader_alive = false;
            }
            return;
        }
        self.leader = None;
        self.leader_alive = false;
        self.pre_campaign();
    }

    /// The heartbeat interval passed: a leader sends every follower an append,
    /// which is a heartbeat if the follower is up to date.
    pub fn heartbeat(&mut self) {
        if matches!(self.state, State::Leader(_)) {
            for to in self.peers.clone() {
                self.send_append(to);
            }
        }
    }

    /// Append `data` if this node is the leader: its index and term, which
    /// the entry still has when it is applied unless another leader replaced
    /// it. Otherwise the leader this node knows of, if any.
    pub fn propose(&mut self, data: Vec<u8>) -> Result<(u64, u64), Option<Id>> {
        if !matches!(self.state, State::Leader(_)) {
            return Err(self.leader);
        }
        let index = self.append_own(data);
        for to in self.peers.clone() {
            // Followers waiting for a reply get it with their next append.
            if self.progress().next[&to] == index {
                self.send_append(to);
            }
        }
        self.maybe_commit();
        Ok((index, self.term))
    }

    pub fn receive(&mut self, from: Id, msg: Message) {
        let term = msg.term();
        match msg {
            // A pre-vote changes nothing on its receiver, whatever its term.
            Message::PreVote {
                term,
                last_index,
                last_term,
            } => return self.on_pre_vote(from, term, last_index, last_term),
            // A granted pre-vote carries the term the sender would run at.
            Message::PreVoteReply { granted: true, .. } => {}
            _ if term > self.term => {
                if !matches!(self.state, State::Follower) {
                    self.stats.higher_term_stepdowns += 1;
                }
                self.become_follower(term);
            }
            Message::Append { .. }
                if term < self.term && self.bug == Some(Bug::StaleTermAccept) => {}
            _ if term < self.term => {
                // Tell a deposed leader or candidate the current term; drop
                // stale replies.
                self.stats.stale_messages += 1;
                let reply = match msg {
                    Message::Append { prev_index, .. } => Message::AppendReply {
                        term: self.term,
                        result: AppendResult::Reject {
                            prev_index,
                            conflict_term: None,
                            first_index: 0,
                        },
                    },
                    Message::Vote { .. } => Message::VoteReply {
                        term: self.term,
                        granted: false,
                    },
                    _ => return,
                };
                self.ready.send.push((from, reply));
                return;
            }
            _ => {}
        }
        match msg {
            Message::PreVote { .. } => unreachable!(),
            Message::PreVoteReply { term, granted } => self.on_pre_vote_reply(from, term, granted),
            Message::Vote {
                last_index,
                last_term,
                ..
            } => self.on_vote(from, last_index, last_term),
            Message::VoteReply { granted, .. } => self.on_vote_reply(from, granted),
            Message::Append {
                prev_index,
                prev_term,
                entries,
                commit,
                ..
            } => self.on_append(from, prev_index, prev_term, entries, commit),
            Message::AppendReply { result, .. } => self.on_append_reply(from, result),
        }
    }

    // ------------------------------------------------------------ elections

    fn pre_campaign(&mut self) {
        self.stats.prevotes_started += 1;
        self.state = State::PreCandidate {
            granted: BTreeSet::from([self.id]),
        };
        let msg = Message::PreVote {
            term: self.term + 1,
            last_index: self.last_index(),
            last_term: self.last_term(),
        };
        self.broadcast(msg);
        self.check_pre_votes();
    }

    fn campaign(&mut self) {
        self.stats.elections_started += 1;
        self.term += 1;
        self.vote = Some(self.id);
        self.save_hard_state();
        self.state = State::Candidate {
            granted: BTreeSet::from([self.id]),
        };
        self.ready.reset_election_timer = true;
        let msg = Message::Vote {
            term: self.term,
            last_index: self.last_index(),
            last_term: self.last_term(),
        };
        self.broadcast(msg);
        self.check_votes();
    }

    /// A candidate's log is at least as up to date as ours (§5.4.1).
    fn up_to_date(&self, last_index: u64, last_term: u64) -> bool {
        (last_term, last_index) >= (self.last_term(), self.last_index())
    }

    fn on_pre_vote(&mut self, from: Id, term: u64, last_index: u64, last_term: u64) {
        let leader_alive = self.leader_alive || matches!(self.state, State::Leader(_));
        let granted = term > self.term && !leader_alive && self.up_to_date(last_index, last_term);
        if term > self.term && leader_alive {
            self.stats.prevotes_refused_leader_alive += 1;
        }
        let term = if granted { term } else { self.term };
        self.ready
            .send
            .push((from, Message::PreVoteReply { term, granted }));
    }

    fn on_pre_vote_reply(&mut self, from: Id, term: u64, granted: bool) {
        if !granted || term != self.term + 1 {
            return;
        }
        if let State::PreCandidate { granted } = &mut self.state {
            granted.insert(from);
            self.check_pre_votes();
        }
    }

    fn check_pre_votes(&mut self) {
        if let State::PreCandidate { granted } = &self.state
            && granted.len() >= self.quorum()
        {
            self.campaign();
        }
    }

    fn on_vote(&mut self, from: Id, last_index: u64, last_term: u64) {
        let granted = self.vote.is_none_or(|v| v == from) && self.up_to_date(last_index, last_term);
        if granted && self.vote.is_none() {
            self.vote = Some(from);
            if self.bug != Some(Bug::VoteNotPersisted) {
                self.save_hard_state();
            }
        }
        if granted {
            self.ready.reset_election_timer = true;
        }
        let term = self.term;
        self.ready
            .send
            .push((from, Message::VoteReply { term, granted }));
    }

    fn on_vote_reply(&mut self, from: Id, granted: bool) {
        if !granted {
            return;
        }
        if let State::Candidate { granted } = &mut self.state {
            granted.insert(from);
            self.check_votes();
        }
    }

    fn check_votes(&mut self) {
        if let State::Candidate { granted } = &self.state
            && granted.len() >= self.quorum()
        {
            self.become_leader();
        }
    }

    fn become_leader(&mut self) {
        self.stats.elections_won += 1;
        let next = self.last_index() + 1;
        self.state = State::Leader(Progress {
            next: self.peers.iter().map(|&p| (p, next)).collect(),
            matched: self.peers.iter().map(|&p| (p, 0)).collect(),
            active: BTreeSet::new(),
        });
        self.leader = Some(self.id);
        self.ready.reset_election_timer = true;
        // The no-op of D61: committing it commits everything before it.
        self.append_own(Vec::new());
        for to in self.peers.clone() {
            self.send_append(to);
        }
        self.maybe_commit();
    }

    /// Move to `term` as a follower with no vote and no known leader.
    fn become_follower(&mut self, term: u64) {
        self.term = term;
        self.vote = None;
        self.save_hard_state();
        self.state = State::Follower;
        self.leader = None;
        self.leader_alive = false;
    }

    // ------------------------------------------------------------ replication

    fn on_append(
        &mut self,
        from: Id,
        prev_index: u64,
        prev_term: u64,
        entries: Vec<Entry>,
        commit: u64,
    ) {
        if matches!(self.state, State::Leader(_)) {
            // Only with a planted bug can another leader share this term.
            return;
        }
        self.state = State::Follower;
        self.leader = Some(from);
        self.leader_alive = true;
        self.ready.reset_election_timer = true;

        let result = if prev_index > self.last_index() {
            AppendResult::Reject {
                prev_index,
                conflict_term: None,
                first_index: self.last_index() + 1,
            }
        } else if self.term_at(prev_index) != prev_term {
            let conflict = self.term_at(prev_index);
            let mut first = prev_index;
            while first > 1 && self.term_at(first - 1) == conflict {
                first -= 1;
            }
            AppendResult::Reject {
                prev_index,
                conflict_term: Some(conflict),
                first_index: first,
            }
        } else {
            let matched = prev_index + entries.len() as u64;
            self.store_entries(prev_index, entries);
            if commit > self.commit {
                // Only what this append proved matches the leader's log.
                self.commit = commit.min(matched).max(self.commit);
            }
            AppendResult::Ok { matched }
        };
        if matches!(result, AppendResult::Reject { .. }) {
            self.stats.appends_rejected += 1;
        }
        let term = self.term;
        self.ready
            .send
            .push((from, Message::AppendReply { term, result }));
    }

    /// Put `entries` after `prev_index`, which matches the leader's log.
    /// Entries already there with the same term are the same entries (log
    /// matching) and are skipped; the first that differs and everything
    /// after it are replaced (§5.3). A stale or duplicated append that holds
    /// nothing new changes nothing, so it can never cut the log short.
    fn store_entries(&mut self, prev_index: u64, entries: Vec<Entry>) {
        let mut index = prev_index;
        let mut new = Vec::new();
        for entry in entries {
            index += 1;
            if new.is_empty() && index <= self.last_index() {
                if self.term_at(index) == entry.term || self.bug == Some(Bug::NoLogTruncate) {
                    continue;
                }
                self.truncate(index);
            }
            new.push(entry);
        }
        if !new.is_empty() {
            let at = self.last_index() + 1;
            self.log.extend(new.iter().cloned());
            self.ready.persist.push(Record::Append {
                index: at,
                entries: new,
            });
        }
    }

    fn truncate(&mut self, from: u64) {
        self.stats.truncations += 1;
        self.log.truncate(from as usize - 1);
        self.ready.persist.push(Record::Truncate { from });
        // A committed entry is never truncated in a correct cluster. With a
        // planted bug it can be: forget it so it is applied again, and let
        // the simulator's checker see two different entries at one index.
        self.commit = self.commit.min(from - 1);
        self.applied = self.applied.min(from - 1);
    }

    fn on_append_reply(&mut self, from: Id, result: AppendResult) {
        let State::Leader(progress) = &mut self.state else {
            return;
        };
        progress.active.insert(from);
        match result {
            AppendResult::Ok { matched } => {
                let m = progress.matched.get_mut(&from).expect("a peer");
                *m = (*m).max(matched);
                let next = progress.next.get_mut(&from).expect("a peer");
                *next = (*next).max(matched + 1);
                let behind = *next <= self.log.len() as u64;
                self.maybe_commit();
                if behind {
                    self.send_append(from);
                }
            }
            AppendResult::Reject {
                prev_index,
                conflict_term,
                first_index,
            } => {
                let next = progress.next[&from];
                if prev_index + 1 != next {
                    return; // an answer to an older append
                }
                // Skip the follower's whole conflicting term (D60): to just
                // after our last entry of that term if we have one, else to
                // where the follower's run of that term starts.
                let hinted = match conflict_term {
                    Some(t) => match self.log.iter().rposition(|e| e.term == t) {
                        Some(i) => i as u64 + 2,
                        None => first_index,
                    },
                    None => first_index,
                };
                let floor = progress.matched[&from] + 1;
                let new_next = hinted.min(prev_index).max(floor);
                if new_next + 1 < next {
                    self.stats.hint_jumps += 1;
                }
                self.progress_mut().next.insert(from, new_next);
                self.send_append(from);
            }
        }
    }

    /// Commit the highest index a majority holds, if it is of this term (D61).
    fn maybe_commit(&mut self) {
        let quorum = self.quorum();
        let State::Leader(progress) = &self.state else {
            return;
        };
        let mut n = self.last_index();
        while n > self.commit {
            if self.term_at(n) != self.term && self.bug != Some(Bug::CommitOldTerm) {
                // Entries below are of older terms too.
                return;
            }
            let holders = 1 + progress.matched.values().filter(|&&m| m >= n).count();
            if holders >= quorum {
                self.commit = n;
                return;
            }
            n -= 1;
        }
    }

    fn send_append(&mut self, to: Id) {
        let next = self.progress().next[&to];
        let prev_index = next - 1;
        let end = (prev_index as usize + MAX_APPEND).min(self.log.len());
        let entries = self.log[prev_index as usize..end].to_vec();
        self.stats.largest_append = self.stats.largest_append.max(entries.len() as u64);
        let msg = Message::Append {
            term: self.term,
            prev_index,
            prev_term: self.term_at(prev_index),
            entries,
            commit: self.commit,
        };
        self.ready.send.push((to, msg));
    }

    /// Append an entry of the current term to the leader's own log.
    fn append_own(&mut self, data: Vec<u8>) -> u64 {
        let entry = Entry {
            term: self.term,
            data,
        };
        self.log.push(entry.clone());
        let index = self.last_index();
        self.ready.persist.push(Record::Append {
            index,
            entries: vec![entry],
        });
        index
    }

    // ------------------------------------------------------------ helpers

    fn save_hard_state(&mut self) {
        self.ready.persist.push(Record::HardState {
            term: self.term,
            vote: self.vote,
        });
    }

    fn broadcast(&mut self, msg: Message) {
        for &to in &self.peers {
            self.ready.send.push((to, msg.clone()));
        }
    }

    fn progress(&self) -> &Progress {
        match &self.state {
            State::Leader(p) => p,
            _ => unreachable!("only a leader tracks progress"),
        }
    }

    fn progress_mut(&mut self) -> &mut Progress {
        match &mut self.state {
            State::Leader(p) => p,
            _ => unreachable!("only a leader tracks progress"),
        }
    }
}

#[cfg(test)]
mod tests;
