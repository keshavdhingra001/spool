//! Random command sequences against the reference queue (D19).
//!
//! Commands are generated as abstract actions and resolved against what the
//! queue has said so far: a heartbeat, ack or nack picks one of the leases ever
//! issued, so it is sometimes current, sometimes stale, expired or already acked.
//! Times mostly move forward in small steps, sometimes jump back (D9), and
//! rarely jump past the dedup window (D35), so keys are both repeated inside
//! their window and reused after it. Some enqueues carry ordering keys (D79),
//! and queue `a` sometimes gains consumer groups (D80), one of which (`a:g`)
//! is leased from like any queue.

use std::collections::BTreeSet;

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;
use spool::{
    Checked, Command, DedupKey, Event, JobId, Millis, Op, OrderKey, Payload, Queue, QueueConfig,
    QueueName, ReferenceQueue, ResultStatus, Time, Token,
};

#[derive(Clone, Debug)]
enum Action {
    Enqueue {
        queue: usize,
        delay: u64,
        key: Option<usize>,
        order: Option<usize>,
    },
    Lease {
        queue: usize,
        visibility: u64,
    },
    Heartbeat {
        pick: usize,
        visibility: u64,
    },
    Ack {
        pick: usize,
    },
    Nack {
        pick: usize,
    },
    Complete {
        pick: usize,
    },
    Result {
        job: u64,
    },
    Configure {
        queue: usize,
        max: u32,
        base: u64,
        cap: u64,
    },
    Redrive {
        queue: usize,
    },
    Subscribe {
        group: usize,
    },
    Tick,
}

#[derive(Clone, Debug)]
struct Step {
    /// Added to the previous command's time (not the clock), floored at 0.
    dt: i64,
    action: Action,
}

const QUEUES: [&str; 3] = ["a", "b", "a:g"];
const KEYS: [&str; 3] = ["k0", "k1", "k2"];
const ORDERS: [&str; 2] = ["o0", "o1"];
const GROUPS: [&str; 2] = ["g", "h"];

fn action() -> impl Strategy<Value = Action> {
    let queue = 0..QUEUES.len();
    prop_oneof![
        4 => (
            queue.clone(),
            prop_oneof![3 => Just(0u64), 1 => 1..40u64],
            prop_oneof![1 => Just(None), 1 => (0..KEYS.len()).prop_map(Some)],
            prop_oneof![1 => Just(None), 1 => (0..ORDERS.len()).prop_map(Some)],
        )
            .prop_map(|(queue, delay, key, order)| Action::Enqueue { queue, delay, key, order }),
        5 => (queue.clone(), 0..40u64).prop_map(|(queue, visibility)| Action::Lease { queue, visibility }),
        2 => (any::<usize>(), 0..40u64).prop_map(|(pick, visibility)| Action::Heartbeat { pick, visibility }),
        3 => any::<usize>().prop_map(|pick| Action::Ack { pick }),
        3 => any::<usize>().prop_map(|pick| Action::Nack { pick }),
        2 => any::<usize>().prop_map(|pick| Action::Complete { pick }),
        1 => (1..30u64).prop_map(|job| Action::Result { job }),
        1 => (queue.clone(), 0..4u32, 0..30u64, 0..40u64)
            .prop_map(|(queue, max, base, cap)| Action::Configure { queue, max, base, cap }),
        1 => queue.prop_map(|queue| Action::Redrive { queue }),
        1 => (0..GROUPS.len()).prop_map(|group| Action::Subscribe { group }),
        1 => Just(Action::Tick),
    ]
}

fn steps(max_len: usize) -> impl Strategy<Value = Vec<Step>> {
    let dt = prop_oneof![
        80 => 0..15i64,
        10 => -30..0i64,
        10 => 15..200i64,
        1 => 250_000..400_000i64,
    ];
    prop::collection::vec(
        (dt, action()).prop_map(|(dt, action)| Step { dt, action }),
        1..max_len,
    )
}

fn queue(i: usize) -> QueueName {
    QueueName::new(QUEUES[i]).unwrap()
}

/// Turns abstract steps into commands, one at a time, as the queue answers.
#[derive(Default)]
struct Resolver {
    at: u64,
    issued: Vec<(JobId, Token)>,
}

impl Resolver {
    fn command(&mut self, step: &Step) -> Command {
        self.at = self.at.saturating_add_signed(step.dt);
        let pick = |p: usize| match self.issued.len() {
            0 => (JobId(p as u64 % 5 + 1), Token(p as u64 % 7 + 1)),
            n => self.issued[p % n],
        };
        let op = match step.action {
            Action::Enqueue {
                queue: q,
                delay,
                key,
                order,
            } => Op::Enqueue {
                queue: queue(q),
                payload: Payload(format!("p{}", self.at).into_bytes()),
                delay: Millis(delay),
                key: key.map(|k| DedupKey::new(KEYS[k]).unwrap()),
                order: order.map(|k| OrderKey::new(ORDERS[k]).unwrap()),
            },
            Action::Lease {
                queue: q,
                visibility,
            } => Op::Lease {
                queue: queue(q),
                visibility: Millis(visibility),
            },
            Action::Heartbeat {
                pick: p,
                visibility,
            } => {
                let (job, token) = pick(p);
                Op::Heartbeat {
                    job,
                    token,
                    visibility: Millis(visibility),
                }
            }
            Action::Ack { pick: p } => {
                let (job, token) = pick(p);
                Op::Ack { job, token }
            }
            Action::Nack { pick: p } => {
                let (job, token) = pick(p);
                Op::Nack { job, token }
            }
            Action::Complete { pick: p } => {
                let (job, token) = pick(p);
                Op::Complete {
                    job,
                    token,
                    result: Payload(format!("r{}", token.0).into_bytes()),
                }
            }
            Action::Result { job } => Op::Result { job: JobId(job) },
            Action::Configure {
                queue: q,
                max,
                base,
                cap,
            } => Op::Configure {
                queue: queue(q),
                config: QueueConfig {
                    max_attempts: max,
                    backoff_base: Millis(base),
                    backoff_cap: Millis(cap),
                },
            },
            Action::Redrive { queue: q } => Op::Redrive { queue: queue(q) },
            Action::Subscribe { group } => Op::Subscribe {
                queue: queue(0),
                group: QueueName::new(GROUPS[group]).unwrap(),
            },
            Action::Tick => Op::Tick,
        };
        Command {
            at: Time(self.at),
            op,
        }
    }

    fn observe(&mut self, events: &[Event]) {
        for e in events {
            if let Event::Leased { lease, .. } = e {
                self.issued.push((lease.job, lease.token));
            }
        }
    }
}

/// Run `steps` with both checkers after every command; return the commands
/// as resolved and every event, or the first failure.
fn run_checked(steps: &[Step]) -> Result<(Vec<Command>, Vec<Event>), String> {
    let mut queue = Checked::new();
    let mut resolver = Resolver::default();
    let (mut commands, mut all) = (Vec::new(), Vec::new());
    for (i, step) in steps.iter().enumerate() {
        let cmd = resolver.command(step);
        let events = queue
            .apply(&cmd)
            .map_err(|e| format!("command {i} `{cmd}`: {e}"))?;
        resolver.observe(events);
        all.extend_from_slice(events);
        commands.push(cmd);
    }
    Ok((commands, all))
}

fn event_kind(e: &Event) -> String {
    match e {
        Event::Rejected { reason } => format!("rejected {reason}"),
        Event::Released { reason, .. } => format!("released {reason}"),
        Event::Result { status, .. } => match status {
            ResultStatus::Pending => "result pending".into(),
            ResultStatus::Done { .. } => "result done".into(),
            ResultStatus::Unknown => "result unknown".into(),
        },
        other => other.to_string().split(' ').next().unwrap().to_string(),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn invariants_hold_after_every_command(steps in steps(300)) {
        if let Err(e) = run_checked(&steps) {
            prop_assert!(false, "{e}");
        }
    }

    #[test]
    fn same_commands_same_events(steps in steps(200)) {
        let (commands, events) = run_checked(&steps).unwrap();
        let mut q = ReferenceQueue::new();
        let mut again = Vec::new();
        for cmd in &commands {
            q.apply(cmd, &mut again);
        }
        prop_assert_eq!(events, again);
    }

    /// Extra ticks between commands change where expiry events appear in the
    /// stream, but not the state the queue ends up in: expiry is computed from
    /// each lease's deadline, never from the time of the command that noticed it.
    #[test]
    fn final_state_does_not_depend_on_tick_schedule(
        steps in steps(200),
        ticks in prop::collection::vec(prop::option::of(any::<u64>()), 200),
    ) {
        let (commands, _) = run_checked(&steps).unwrap();
        let mut plain = ReferenceQueue::new();
        let mut ticked = Checked::new();
        let mut out = Vec::new();
        for (cmd, tick) in commands.iter().zip(&ticks) {
            if let Some(r) = tick {
                // A tick anywhere in [clock, the time `cmd` will run at] leaves
                // `cmd`'s own time unchanged.
                let now = ticked.queue.now().0;
                let runs_at = now.max(cmd.at.0);
                let at = Time(now + r % (runs_at - now + 1));
                ticked.apply(&Command { at, op: Op::Tick }).unwrap();
            }
            plain.apply(cmd, &mut out);
            ticked.apply(cmd).unwrap();
        }
        prop_assert_eq!(plain.jobs(), ticked.queue.jobs());
        prop_assert_eq!(plain.counts(), ticked.queue.counts());
    }
}

/// The generator must actually reach every event and every rejection, or the
/// properties above are weaker than they look.
#[test]
fn generator_covers_every_outcome() {
    let mut runner = TestRunner::deterministic();
    let strategy = steps(300);
    let mut seen = BTreeSet::new();
    for _ in 0..200 {
        let steps = strategy.new_tree(&mut runner).unwrap().current();
        let (_, events) = run_checked(&steps).unwrap();
        seen.extend(events.iter().map(event_kind));
        // A job completed twice: the second is a repeat by the same lease (D42).
        let mut completed = BTreeSet::new();
        for e in &events {
            if let Event::Completed { job, .. } = e
                && !completed.insert(*job)
            {
                seen.insert("completed again".to_string());
            }
        }
        // Only a fan-out fills `a:h` (D80), and a second job of an ordering
        // key leased means the key's first job went before it (D79).
        let mut orders = BTreeSet::new();
        let mut keyed = std::collections::BTreeMap::new();
        for e in &events {
            match e {
                Event::Enqueued { queue, .. } if queue.as_str() == "a:h" => {
                    seen.insert("fan-out".to_string());
                }
                _ => {}
            }
            if let Event::Enqueued {
                job,
                queue,
                order: Some(order),
                ..
            } = e
            {
                let k = (queue.clone(), order.clone());
                let second = keyed.values().any(|x| *x == k);
                keyed.insert(*job, k);
                if second {
                    orders.insert(*job);
                }
            }
            if let Event::Leased { lease, .. } = e
                && orders.contains(&lease.job)
            {
                seen.insert("later job of an ordering key leased".to_string());
            }
        }
        // A key enqueued a second time created a job: its window had ended.
        let mut keys = BTreeSet::new();
        for e in &events {
            if let Event::Enqueued {
                queue,
                key: Some(key),
                ..
            } = e
                && !keys.insert((queue.clone(), key.clone()))
            {
                seen.insert("key reused after its window".to_string());
            }
        }
    }
    let expected = [
        "acked",
        "completed",
        "completed again",
        "configured",
        "dead",
        "deduplicated",
        "empty",
        "enqueued",
        "fan-out",
        "key reused after its window",
        "later job of an ordering key leased",
        "leased",
        "redriven",
        "rejected bad_config",
        "rejected bad_group",
        "rejected not_leased",
        "rejected stale_token",
        "rejected unknown_job",
        "rejected zero_visibility",
        "released expired",
        "released nack",
        "renewed",
        "result done",
        "result pending",
        "result unknown",
        "retrying",
        "subscribed",
    ];
    let missing: Vec<_> = expected.iter().filter(|k| !seen.contains(**k)).collect();
    assert!(
        missing.is_empty(),
        "never produced: {missing:?}; saw {seen:?}"
    );
}
