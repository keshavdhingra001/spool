//! Unit tests of the Raft node, on a hand-driven cluster: every message waits
//! in a queue until a test delivers or drops it, and every record a node asks
//! to persist is replayed into its `Saved`, which must always equal the node's
//! own term, vote and log (D58).

use std::collections::VecDeque;

use super::*;

struct Cluster {
    nodes: Vec<Node>,
    saved: Vec<Saved>,
    /// Messages sent and not yet delivered: (from, to, message).
    inflight: VecDeque<(Id, Id, Message)>,
    applied: Vec<Vec<(u64, Entry)>>,
    bug: Option<Bug>,
}

impl Cluster {
    fn new(n: u32) -> Cluster {
        Cluster::with_bug(n, None)
    }

    fn with_bug(n: u32, bug: Option<Bug>) -> Cluster {
        let members: Vec<Id> = (0..n).collect();
        let mut c = Cluster {
            nodes: members
                .iter()
                .map(|&i| Node::new(i, &members, Saved::default()).with_bug(bug))
                .collect(),
            saved: vec![Saved::default(); n as usize],
            inflight: VecDeque::new(),
            applied: vec![Vec::new(); n as usize],
            bug,
        };
        for i in 0..n {
            c.drain(i);
        }
        c
    }

    fn node(&self, i: Id) -> &Node {
        &self.nodes[i as usize]
    }

    /// Carry out node `i`'s `Ready`: persist, queue its messages, apply.
    fn drain(&mut self, i: Id) -> Ready {
        let ready = self.nodes[i as usize].take_ready();
        let saved = &mut self.saved[i as usize];
        for record in ready.persist.clone() {
            saved.replay(record).unwrap();
        }
        let node = &self.nodes[i as usize];
        if self.bug.is_none() {
            assert_eq!(
                (saved.term, saved.vote, saved.log.as_slice()),
                (node.term(), node.vote(), node.log()),
                "node {i}: what is on disk is what the node believes"
            );
        }
        for (to, msg) in ready.send.clone() {
            self.inflight.push_back((i, to, msg));
        }
        self.applied[i as usize].extend(ready.apply.clone());
        ready
    }

    fn timeout(&mut self, i: Id) -> Ready {
        self.nodes[i as usize].election_timeout();
        self.drain(i)
    }

    fn heartbeat(&mut self, i: Id) {
        self.nodes[i as usize].heartbeat();
        self.drain(i);
    }

    fn propose(&mut self, i: Id, data: &str) -> Result<(u64, u64), Option<Id>> {
        let r = self.nodes[i as usize].propose(data.as_bytes().to_vec());
        self.drain(i);
        r
    }

    /// Deliver queued messages, including the ones they cause, until none is
    /// left; messages for which `keep` is false are dropped instead.
    fn deliver_if(&mut self, keep: impl Fn(Id, Id, &Message) -> bool) -> usize {
        let mut n = 0;
        while let Some((from, to, msg)) = self.inflight.pop_front() {
            if keep(from, to, &msg) {
                n += 1;
                self.nodes[to as usize].receive(from, msg);
                self.drain(to);
            }
        }
        n
    }

    fn deliver(&mut self) -> usize {
        self.deliver_if(|_, _, _| true)
    }

    /// Make `i` leader through a full pre-vote and election.
    fn elect(&mut self, i: Id) {
        self.timeout(i);
        self.deliver();
        assert_eq!(self.node(i).role(), Role::Leader);
    }

    fn data(&self, i: Id) -> Vec<String> {
        self.applied[i as usize]
            .iter()
            .filter(|(_, e)| !e.data.is_empty())
            .map(|(_, e)| String::from_utf8(e.data.clone()).unwrap())
            .collect()
    }
}

fn entry(term: u64, data: &str) -> Entry {
    Entry {
        term,
        data: data.as_bytes().to_vec(),
    }
}

/// A node restarted from `log` at `term`, in a cluster of `n`.
fn restored(id: Id, n: u32, term: u64, log: Vec<Entry>) -> Node {
    let members: Vec<Id> = (0..n).collect();
    let saved = Saved {
        term,
        vote: None,
        log,
    };
    Node::new(id, &members, saved)
}

#[test]
fn a_timeout_elects_a_leader_through_a_pre_vote_and_commits_its_no_op() {
    let mut c = Cluster::new(3);
    let r = c.timeout(0);
    assert!(r.reset_election_timer);
    assert_eq!(c.node(0).role(), Role::PreCandidate);
    assert_eq!(c.node(0).term(), 0, "a pre-vote raises no term");
    assert!(
        r.persist.is_empty(),
        "and writes nothing: there is nothing new to remember"
    );
    c.deliver();
    assert_eq!(c.node(0).role(), Role::Leader);
    assert_eq!(c.node(0).term(), 1);
    for i in 0..3 {
        assert_eq!(c.node(i).term(), 1);
        assert_eq!(c.node(i).leader(), Some(0));
        assert_eq!(c.node(i).log(), [entry(1, "")]);
    }
    // The no-op is committed on the leader; followers learn it on the next
    // append, which is a heartbeat.
    assert_eq!(c.node(0).commit(), 1);
    c.heartbeat(0);
    c.deliver();
    for i in 0..3 {
        assert_eq!(c.node(i).commit(), 1);
        assert_eq!(c.applied[i as usize], [(1, entry(1, ""))]);
    }
    assert_eq!(c.node(1).vote(), Some(0));
}

#[test]
fn proposals_are_replicated_committed_and_applied_in_order() {
    let mut c = Cluster::new(5);
    c.elect(2);
    assert_eq!(
        c.propose(1, "x"),
        Err(Some(2)),
        "a follower names the leader"
    );
    assert_eq!(c.propose(2, "a"), Ok((2, 1)));
    assert_eq!(c.propose(2, "b"), Ok((3, 1)));
    c.deliver();
    assert_eq!(c.node(2).commit(), 3);
    assert_eq!(c.data(2), ["a", "b"]);
    c.heartbeat(2);
    c.deliver();
    for i in 0..5 {
        assert_eq!(c.data(i), ["a", "b"], "node {i}");
    }
}

#[test]
fn one_vote_per_term_and_only_for_an_up_to_date_log() {
    let mut n = restored(0, 3, 4, vec![entry(1, "a"), entry(4, "b")]);
    n.take_ready();
    let vote = |term, last_index, last_term| Message::Vote {
        term,
        last_index,
        last_term,
    };
    // A longer log of an older last term is not up to date.
    n.receive(1, vote(5, 9, 3));
    let r = n.take_ready();
    assert_eq!(
        r.send,
        [(
            1,
            Message::VoteReply {
                term: 5,
                granted: false
            }
        )]
    );
    assert_eq!(
        r.persist,
        [Record::HardState {
            term: 5,
            vote: None
        }]
    );
    // Same last term and as long: granted, and the vote is persisted.
    n.receive(2, vote(5, 2, 4));
    let r = n.take_ready();
    assert_eq!(
        r.send,
        [(
            2,
            Message::VoteReply {
                term: 5,
                granted: true
            }
        )]
    );
    assert_eq!(
        r.persist,
        [Record::HardState {
            term: 5,
            vote: Some(2)
        }]
    );
    assert!(r.reset_election_timer);
    // A second candidate in the same term is refused; the first may ask again.
    n.receive(1, vote(5, 2, 4));
    assert_eq!(
        n.take_ready().send,
        [(
            1,
            Message::VoteReply {
                term: 5,
                granted: false
            }
        )]
    );
    n.receive(2, vote(5, 2, 4));
    let r = n.take_ready();
    assert_eq!(
        r.send,
        [(
            2,
            Message::VoteReply {
                term: 5,
                granted: true
            }
        )]
    );
    assert!(r.persist.is_empty());
}

/// Nodes 0 and 1 both campaign in term 1; node 2 votes for node 0, crashes
/// right after, restarts from its disk and is asked by node 1. Returns the
/// roles of nodes 0 and 1 at the end.
fn split_vote_with_a_crash_between(bug: Option<Bug>) -> (Role, Role) {
    let mut c = Cluster::with_bug(3, bug);
    c.timeout(0);
    c.timeout(1);
    // Node 2 grants both pre-votes; nodes 0 and 1 do not hear each other.
    c.deliver_if(|from, to, m| {
        matches!(m, Message::PreVote { .. } | Message::PreVoteReply { .. })
            && (from == 2 || to == 2)
    });
    assert_eq!(c.node(0).role(), Role::Candidate);
    assert_eq!(c.node(1).role(), Role::Candidate);
    // Their first vote requests were lost; they ask again on the next tick.
    c.heartbeat(0);
    c.heartbeat(1);
    // Node 2 gets node 0's vote request first; node 1's waits.
    let late: Vec<_> = c
        .inflight
        .iter()
        .filter(|(from, to, _)| *from == 1 && *to == 2)
        .cloned()
        .collect();
    // (Node 0's first append, which would make node 2's log newer than
    // node 1's, is lost.)
    c.deliver_if(|from, to, m| {
        matches!(m, Message::Vote { .. } | Message::VoteReply { .. })
            && ((from == 0 && to == 2) || (from == 2 && to == 0))
    });
    assert_eq!(c.node(0).role(), Role::Leader);
    // Node 2 crashes and restarts from what it saved.
    let members = [0, 1, 2];
    c.nodes[2] = Node::new(2, &members, c.saved[2].clone()).with_bug(bug);
    c.drain(2);
    c.inflight.extend(late);
    c.deliver_if(|from, to, _| (from == 1 && to == 2) || (from == 2 && to == 1));
    (c.node(0).role(), c.node(1).role())
}

#[test]
fn a_vote_survives_a_crash_so_a_term_has_one_leader() {
    assert_eq!(
        split_vote_with_a_crash_between(None),
        (Role::Leader, Role::Candidate)
    );
    assert_eq!(
        split_vote_with_a_crash_between(Some(Bug::VoteNotPersisted)),
        (Role::Leader, Role::Leader),
        "the planted bug lets the restarted node vote twice in term 1"
    );
}

#[test]
fn with_the_vote_bug_a_granted_vote_is_not_written() {
    let mut n = restored(0, 3, 4, Vec::new()).with_bug(Some(Bug::VoteNotPersisted));
    n.take_ready();
    n.receive(
        1,
        Message::Vote {
            term: 4,
            last_index: 0,
            last_term: 0,
        },
    );
    let r = n.take_ready();
    assert_eq!(n.vote(), Some(1));
    assert!(r.persist.is_empty());
}

#[test]
fn a_follower_that_hears_its_leader_refuses_pre_votes() {
    let mut c = Cluster::new(3);
    c.elect(0);
    // Node 2 is cut off from the leader (one way) and times out.
    c.timeout(2);
    assert_eq!(c.node(2).role(), Role::PreCandidate);
    c.deliver_if(|from, to, _| !(from == 0 && to == 2));
    assert_eq!(c.node(2).role(), Role::PreCandidate, "nobody granted");
    assert_eq!(c.node(1).stats().prevotes_refused_leader_alive, 1);
    assert_eq!(c.node(0).stats().prevotes_refused_leader_alive, 1);
    // No term moved, so the leader keeps leading.
    for i in 0..3 {
        assert_eq!(c.node(i).term(), 1);
    }
    assert_eq!(c.node(0).role(), Role::Leader);
    // After the leader's timer fires too, pre-votes are granted again.
    c.timeout(1);
    c.deliver_if(|from, _, _| from != 0);
    assert_eq!(c.node(2).term(), 2);
    assert!(matches!(c.node(1).role(), Role::Leader | Role::Follower));
}

#[test]
fn an_isolated_node_does_not_raise_its_term_and_rejoins_quietly() {
    let mut c = Cluster::new(3);
    c.elect(0);
    for _ in 0..5 {
        c.timeout(2);
        c.deliver_if(|from, to, _| from != 2 && to != 2);
    }
    assert_eq!(c.node(2).term(), 1, "pre-votes never got answers");
    c.heartbeat(0);
    c.deliver();
    assert_eq!(c.node(2).role(), Role::Follower);
    assert_eq!(c.node(0).role(), Role::Leader);
    assert_eq!(c.node(0).term(), 1);
}

#[test]
fn check_quorum_steps_down_a_leader_that_hears_no_majority() {
    let mut c = Cluster::new(5);
    c.elect(0);
    c.heartbeat(0);
    // Every follower hears the heartbeat; only node 1's answer gets back.
    c.deliver_if(|from, _, _| from == 0 || from == 1);
    c.timeout(0);
    assert_eq!(
        c.node(0).role(),
        Role::Leader,
        "the first period had the election's replies"
    );
    c.heartbeat(0);
    c.deliver_if(|from, to, _| (from == 0 && to == 1) || (from == 1 && to == 0));
    c.timeout(0);
    assert_eq!(c.node(0).role(), Role::Follower);
    assert_eq!(c.node(0).leader(), None);
    assert_eq!(c.node(0).stats().check_quorum_stepdowns, 1);
    assert_eq!(c.propose(0, "x"), Err(None));
}

#[test]
fn a_higher_term_deposes_a_leader_and_its_appends_are_refused() {
    let mut c = Cluster::new(3);
    c.elect(0);
    c.inflight.clear();
    // Nodes 1 and 2 elect node 1 at term 2 while node 0 hears nothing.
    // Node 2's timer fires first (its own pre-vote is lost), so it no longer
    // believes in node 0 and grants node 1's pre-vote.
    c.timeout(2);
    c.inflight.clear();
    c.timeout(1);
    c.deliver_if(|from, to, _| from != 0 && to != 0);
    assert_eq!(c.node(1).role(), Role::Leader);
    assert_eq!(c.node(1).term(), 2);
    // The old leader still believes; its proposal is refused with the term.
    assert_eq!(c.propose(0, "stale"), Ok((2, 1)));
    c.deliver();
    assert_eq!(c.node(0).role(), Role::Follower);
    assert_eq!(c.node(0).term(), 2);
    assert_eq!(c.node(0).stats().higher_term_stepdowns, 1);
    // The new leader overwrites the stale entry.
    c.heartbeat(1);
    c.deliver();
    assert_eq!(c.node(0).log(), c.node(1).log());
    assert_eq!(c.node(0).log()[1], entry(2, ""));
    assert!(c.node(0).stats().truncations >= 1);
}

#[test]
fn with_the_stale_term_bug_a_deposed_leader_still_writes_to_followers() {
    let mut n =
        restored(1, 3, 2, vec![entry(1, ""), entry(2, "")]).with_bug(Some(Bug::StaleTermAccept));
    n.take_ready();
    n.receive(
        0,
        Message::Append {
            term: 1,
            prev_index: 1,
            prev_term: 1,
            entries: vec![entry(1, "stale")],
            commit: 0,
        },
    );
    assert_eq!(n.log()[1], entry(1, "stale"));
    let mut ok = restored(1, 3, 2, vec![entry(1, ""), entry(2, "")]);
    ok.take_ready();
    ok.receive(
        0,
        Message::Append {
            term: 1,
            prev_index: 1,
            prev_term: 1,
            entries: vec![entry(1, "stale")],
            commit: 0,
        },
    );
    assert_eq!(ok.log()[1], entry(2, ""));
    assert_eq!(
        ok.take_ready().send,
        [(
            0,
            Message::AppendReply {
                term: 2,
                result: AppendResult::Reject {
                    prev_index: 1,
                    conflict_term: None,
                    first_index: 0
                }
            }
        )]
    );
}

#[test]
fn conflict_hints_skip_a_whole_term_per_round_trip() {
    // The follower has 30 entries of term 2 the leader never had; the leader
    // has 30 entries of term 3 after a common first entry.
    let mut f_log = vec![entry(1, "")];
    f_log.extend((0..30).map(|i| entry(2, &format!("f{i}"))));
    let mut l_log = vec![entry(1, "")];
    l_log.extend((0..30).map(|i| entry(3, &format!("l{i}"))));
    let mut c = Cluster::new(3);
    c.nodes[0] = restored(0, 3, 3, l_log.clone());
    c.nodes[1] = restored(1, 3, 3, f_log);
    c.nodes[2] = restored(2, 3, 3, l_log.clone());
    for i in 0..3 {
        c.saved[i as usize] = Saved {
            term: 3,
            vote: None,
            log: c.node(i).log().to_vec(),
        };
        c.drain(i);
    }
    c.elect(0);
    let delivered = c.deliver();
    let mut expect = l_log;
    expect.push(entry(4, ""));
    assert_eq!(c.node(1).log(), expect.as_slice());
    assert!(
        delivered < 12,
        "{delivered} messages: one rejection, not thirty"
    );
    assert_eq!(c.node(0).stats().hint_jumps, 1);
    assert_eq!(c.node(1).stats().truncations, 1);
}

#[test]
fn a_short_follower_catches_up_in_bounded_appends() {
    let mut c = Cluster::new(3);
    c.elect(0);
    c.inflight.clear();
    for i in 0..150 {
        c.propose(0, &format!("e{i}")).unwrap();
        // Node 2 hears nothing.
        c.deliver_if(|_, to, _| to != 2);
    }
    assert_eq!(c.node(2).log().len(), 1);
    c.heartbeat(0);
    c.deliver();
    assert_eq!(c.node(2).log(), c.node(0).log());
    assert_eq!(c.node(0).stats().largest_append, MAX_APPEND as u64);
}

/// Figure 8 of the paper. Node 0 led term 2 and got entry 2 onto nodes 0 and
/// 1; node 4 led term 3 with its own entry 2. Node 0 leads again in term 4
/// and copies its entry 2 onto node 2: a majority holds it, but it is not of
/// term 4, so it must not commit until an entry of term 4 is on a majority
/// too. Otherwise node 4 could still win term 5 and overwrite it.
fn figure_8(bug: Option<Bug>) -> Node {
    let log = |terms: &[u64]| terms.iter().map(|&t| entry(t, "")).collect::<Vec<_>>();
    let mut leader = restored(0, 5, 3, log(&[1, 2])).with_bug(bug);
    leader.take_ready();
    // Votes from nodes 1 and 2 elect it at term 4 (the pre-vote is skipped by
    // answering it directly).
    leader.election_timeout();
    leader.take_ready();
    for v in [1, 2] {
        leader.receive(
            v,
            Message::PreVoteReply {
                term: 4,
                granted: true,
            },
        );
    }
    for v in [1, 2] {
        leader.receive(
            v,
            Message::VoteReply {
                term: 4,
                granted: true,
            },
        );
    }
    assert_eq!((leader.role(), leader.term()), (Role::Leader, 4));
    let no_op = bug != Some(Bug::CommitOldTerm);
    assert_eq!(
        leader.log().len(),
        if no_op { 3 } else { 2 },
        "the no-op of term 4 is entry 3"
    );
    leader.take_ready();
    // Node 1 had entry 2; node 2 now gets it but not the no-op.
    for v in [1, 2] {
        leader.receive(
            v,
            Message::AppendReply {
                term: 4,
                result: AppendResult::Ok { matched: 2 },
            },
        );
    }
    leader
}

#[test]
fn an_entry_of_an_older_term_commits_only_with_one_of_the_current_term() {
    let mut leader = figure_8(None);
    assert_eq!(
        leader.commit(),
        0,
        "entry 2 is on a majority, but of term 2"
    );
    leader.receive(
        1,
        Message::AppendReply {
            term: 4,
            result: AppendResult::Ok { matched: 3 },
        },
    );
    leader.receive(
        2,
        Message::AppendReply {
            term: 4,
            result: AppendResult::Ok { matched: 3 },
        },
    );
    assert_eq!(leader.commit(), 3, "the no-op commits, and entry 2 with it");
    let applied: Vec<u64> = leader.take_ready().apply.iter().map(|(i, _)| *i).collect();
    assert_eq!(applied, [1, 2, 3]);

    assert_eq!(
        figure_8(Some(Bug::CommitOldTerm)).commit(),
        2,
        "the planted bug commits by counting old-term replicas"
    );
}

#[test]
fn with_the_truncate_bug_a_follower_keeps_conflicting_entries() {
    let append = Message::Append {
        term: 3,
        prev_index: 1,
        prev_term: 1,
        entries: vec![entry(3, "new")],
        commit: 0,
    };
    let mut ok = restored(1, 3, 3, vec![entry(1, ""), entry(2, "old")]);
    ok.receive(0, append.clone());
    assert_eq!(ok.log(), [entry(1, ""), entry(3, "new")]);
    let mut bad =
        restored(1, 3, 3, vec![entry(1, ""), entry(2, "old")]).with_bug(Some(Bug::NoLogTruncate));
    bad.receive(0, append);
    assert_eq!(bad.log(), [entry(1, ""), entry(2, "old")]);
}

#[test]
fn a_late_duplicate_append_never_cuts_the_log() {
    let mut f = restored(1, 3, 2, vec![entry(1, "")]);
    let first = Message::Append {
        term: 2,
        prev_index: 1,
        prev_term: 1,
        entries: vec![entry(2, "a")],
        commit: 0,
    };
    let second = Message::Append {
        term: 2,
        prev_index: 2,
        prev_term: 2,
        entries: vec![entry(2, "b")],
        commit: 3,
    };
    f.receive(0, first.clone());
    f.receive(0, second);
    f.take_ready();
    f.receive(0, first);
    assert_eq!(f.log(), [entry(1, ""), entry(2, "a"), entry(2, "b")]);
    let r = f.take_ready();
    assert!(r.persist.is_empty());
    assert_eq!(
        r.send,
        [(
            0,
            Message::AppendReply {
                term: 2,
                result: AppendResult::Ok { matched: 2 }
            }
        )]
    );
    assert_eq!(f.commit(), 3, "commit never goes back");
}

#[test]
fn a_follower_commits_only_what_the_append_proved() {
    let mut f = restored(1, 3, 2, vec![entry(1, ""), entry(1, "x"), entry(1, "y")]);
    // The leader's commit is 3, but this append only proves entry 1 matches:
    // entries 2 and 3 may be about to be replaced.
    f.receive(
        0,
        Message::Append {
            term: 2,
            prev_index: 1,
            prev_term: 1,
            entries: Vec::new(),
            commit: 3,
        },
    );
    assert_eq!(f.commit(), 1);
}

#[test]
fn a_stale_rejection_does_not_move_next_index() {
    let mut c = Cluster::new(3);
    c.elect(0);
    c.deliver();
    c.propose(0, "a").unwrap();
    c.deliver();
    // An old rejection for prev_index 0 arrives late.
    c.nodes[0].receive(
        1,
        Message::AppendReply {
            term: 1,
            result: AppendResult::Reject {
                prev_index: 0,
                conflict_term: None,
                first_index: 1,
            },
        },
    );
    let r = c.drain(0);
    assert!(
        r.send.is_empty(),
        "nothing resent: it answered an older append"
    );
}

#[test]
fn candidates_ask_again_whoever_has_not_granted() {
    let mut c = Cluster::new(5);
    c.timeout(0);
    // Nodes 1 and 2 grant the pre-vote; only node 1's vote gets through.
    c.deliver_if(|from, to, m| {
        let pair = |set: &[Id]| set.contains(&from) && set.contains(&to);
        match m {
            Message::PreVote { .. } | Message::PreVoteReply { .. } => pair(&[0, 1, 2]),
            _ => pair(&[0, 1]),
        }
    });
    assert_eq!(c.node(0).role(), Role::Candidate);
    c.heartbeat(0);
    let resent: Vec<Id> = c.inflight.iter().map(|(_, to, _)| *to).collect();
    assert_eq!(resent, [2, 3, 4]);
    assert!(
        c.inflight
            .iter()
            .all(|(_, _, m)| matches!(m, Message::Vote { term: 1, .. }))
    );
    c.inflight.clear();
    c.heartbeat(1);
    assert!(
        c.inflight.is_empty(),
        "followers send nothing on a heartbeat tick"
    );
    c.heartbeat(0);
    c.deliver();
    assert_eq!(c.node(0).role(), Role::Leader);
}

#[test]
fn only_progress_starts_another_append() {
    let mut c = Cluster::new(3);
    c.elect(0);
    c.inflight.clear();
    for i in 0..100 {
        c.propose(0, &format!("e{i}")).unwrap();
    }
    c.inflight.clear();
    let ok = |matched| Message::AppendReply {
        term: 1,
        result: AppendResult::Ok { matched },
    };
    // Node 1 has the no-op (entry 1) and is behind: an ack that moves it on
    // gets the next batch...
    c.nodes[0].receive(1, ok(2));
    assert_eq!(c.drain(0).send.len(), 1);
    // ...and the same ack again, or an older one, gets nothing.
    c.nodes[0].receive(1, ok(2));
    c.nodes[0].receive(1, ok(1));
    assert!(c.drain(0).send.is_empty());
    // A rejection that cannot move next_index back sends nothing either.
    let reject = |prev_index| Message::AppendReply {
        term: 1,
        result: AppendResult::Reject {
            prev_index,
            conflict_term: None,
            first_index: 1,
        },
    };
    c.nodes[0].receive(1, reject(2));
    assert!(c.drain(0).send.is_empty());
}

#[test]
fn a_single_node_cluster_elects_itself_and_commits_alone() {
    let mut c = Cluster::new(1);
    c.timeout(0);
    assert_eq!(c.node(0).role(), Role::Leader);
    assert_eq!(c.propose(0, "solo"), Ok((2, 1)));
    assert_eq!(c.data(0), ["solo"]);
}

#[test]
fn replay_rejects_records_that_do_not_fit() {
    let mut s = Saved::default();
    assert!(
        s.replay(Record::Append {
            index: 2,
            entries: vec![entry(1, "")]
        })
        .is_err()
    );
    assert!(s.replay(Record::Truncate { from: 0 }).is_err());
    s.replay(Record::HardState {
        term: 3,
        vote: Some(1),
    })
    .unwrap();
    assert!(
        s.replay(Record::HardState {
            term: 2,
            vote: None
        })
        .is_err()
    );
    s.replay(Record::Append {
        index: 1,
        entries: vec![entry(3, "a"), entry(3, "b")],
    })
    .unwrap();
    s.replay(Record::Truncate { from: 2 }).unwrap();
    assert_eq!(s.log, [entry(3, "a")]);
    assert!(s.replay(Record::Truncate { from: 3 }).is_err());
}
