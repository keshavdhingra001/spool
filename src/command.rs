//! What goes into the queue (`Command`) and what comes out (`Event`), D4.
//!
//! The queue is a deterministic state machine: the same command sequence always
//! produces the same event sequence. Recovery (M2), Raft replicas (M7) and the
//! simulator (M5) all depend on that.
//!
//! Text format (D12), one command per line, used by the REPL and scenario files.
//! Every command starts with its logical time in milliseconds (D9):
//!
//! ```text
//! @<ms> enqueue   <queue> <payload> [delay=<ms>] [key=<key>] [order=<key>]
//! @<ms> lease     <queue> <visibility_ms>
//! @<ms> heartbeat <job> <token> <visibility_ms>
//! @<ms> ack       <job> <token>
//! @<ms> complete  <job> <token> <result>
//! @<ms> result    <job>
//! @<ms> nack      <job> <token>
//! @<ms> configure <queue> <max_attempts> <backoff_base_ms> <backoff_cap_ms>
//! @<ms> redrive   <queue>
//! @<ms> subscribe <queue> <group>
//! @<ms> tick
//! ```

use std::fmt;
use std::str::FromStr;

use crate::error::ParseError;
use crate::retry::QueueConfig;
use crate::types::{DedupKey, JobId, Lease, Millis, OrderKey, Payload, QueueName, Time, Token};

/// One input to the queue: an operation stamped with the time it happens at.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Command {
    /// The caller's current time. The queue's clock becomes `max(clock, at)` (D9),
    /// and expires every lease whose deadline has passed before applying `op`.
    pub at: Time,
    pub op: Op,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    /// Add a job to `queue`, leasable from `now + delay` (D17). The queue
    /// assigns its id (D10) and creates the queue with default settings if needed.
    /// With a `key` already used in `queue` within the last 5 minutes, nothing is
    /// added and the original job's id is returned (D35). Jobs with the same
    /// `order` key are leased one at a time, in enqueue order (D79). A queue
    /// with consumer groups gets one job in each group's queue instead (D80).
    Enqueue {
        queue: QueueName,
        payload: Payload,
        delay: Millis,
        key: Option<DedupKey>,
        order: Option<OrderKey>,
    },
    /// Lease the ready job with the smallest `(ready_at, id)` in `queue` (D18), hidden from other workers until
    /// `now + visibility`.
    Lease {
        queue: QueueName,
        visibility: Millis,
    },
    /// Extend a lease: its new deadline is `now + visibility`.
    Heartbeat {
        job: JobId,
        token: Token,
        visibility: Millis,
    },
    /// The job finished; remove it.
    Ack { job: JobId, token: Token },
    /// The job finished with `result`: remove it and keep the result for 5
    /// minutes, in one command (D41). Repeating it with the same token while
    /// the result is kept changes nothing and succeeds again (D42).
    Complete {
        job: JobId,
        token: Token,
        result: Payload,
    },
    /// Ask whether `job` is still in the queue, finished with a result, or
    /// neither (D41). Changes nothing but the clock.
    Result { job: JobId },
    /// The job failed; give up the lease. It retries after a backoff (D13) or
    /// is dead-lettered if this was its last attempt (D15).
    Nack { job: JobId, token: Token },
    /// Set `queue`'s retry policy (D14), creating the queue if needed. Applies
    /// to failures from now on, including jobs already in the queue.
    Configure {
        queue: QueueName,
        config: QueueConfig,
    },
    /// Move every dead job of `queue` back to ready, with its attempts reset (D16).
    Redrive { queue: QueueName },
    /// Give `queue` the consumer group `group` (D80): from now on each enqueue
    /// to `queue` adds a job to `<queue>:<group>`. Repeating it changes nothing.
    Subscribe { queue: QueueName, group: QueueName },
    /// Only advance the clock, expiring leases that are past their deadline.
    Tick,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// A job was added, given `job` as its id, and becomes leasable at `ready_at`.
    Enqueued {
        job: JobId,
        queue: QueueName,
        ready_at: Time,
        key: Option<DedupKey>,
        order: Option<OrderKey>,
    },
    /// A keyed enqueue repeated a key still in its window (D35): nothing was
    /// added, and `job` is the job the key's first enqueue created.
    Deduplicated {
        job: JobId,
        queue: QueueName,
        key: DedupKey,
    },
    /// A worker got a job. `attempt` counts leases of this job, starting at 1.
    Leased {
        lease: Lease,
        attempt: u32,
        payload: Payload,
    },
    /// A lease found no ready job in `queue`.
    Empty { queue: QueueName },
    /// A heartbeat moved the lease's deadline.
    Renewed { lease: Lease },
    /// The job completed and is gone.
    Acked { job: JobId },
    /// The job completed with a result under the lease `token` (D41).
    Completed { job: JobId, token: Token },
    /// The answer to a `result` op.
    Result { job: JobId, status: ResultStatus },
    /// The lease ended without an ack, by nack or by reaching its deadline.
    /// Always followed by `Retrying` or `DeadLettered` for the same job.
    Released {
        job: JobId,
        token: Token,
        reason: ReleaseReason,
    },
    /// The job will be leasable again at `ready_at`, after its backoff.
    Retrying { job: JobId, ready_at: Time },
    /// The job used its last attempt and is parked until a redrive.
    DeadLettered { job: JobId },
    /// A dead job is ready again, with its attempts reset to 0.
    Redriven { job: JobId },
    /// `queue`'s retry policy is now `config`.
    Configured {
        queue: QueueName,
        config: QueueConfig,
    },
    /// `queue` has the consumer group `group` (D80).
    Subscribed { queue: QueueName, group: QueueName },
    /// The command was refused and changed nothing except the clock.
    Rejected { reason: RejectReason },
}

/// What `result` found for a job (D41).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResultStatus {
    /// The job is still in the queue: waiting, leased or dead.
    Pending,
    /// The job was completed under `token` with `payload` as its result.
    Done { token: Token, payload: Payload },
    /// No such job, a job acked without a result, or a result whose window ended.
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReleaseReason {
    Nack,
    Expired,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectReason {
    /// No job with this id exists (never enqueued, or already acked).
    UnknownJob,
    /// The job exists but is not leased right now.
    NotLeased,
    /// The job is leased under a different token: the caller's lease expired and
    /// the job was leased again (the zombie-worker case, D5).
    StaleToken,
    /// A lease or heartbeat asked for a visibility timeout of 0, which would
    /// expire at the moment it was granted.
    ZeroVisibility,
    /// `configure` with `max_attempts` 0 or a base above the cap.
    BadConfig,
    /// `subscribe` naming a group's queue, or a pair whose group queue name
    /// would be too long; or `enqueue` to a group's queue, which only an
    /// enqueue to its queue fills (D80).
    BadGroup,
}

impl FromStr for Command {
    type Err = ParseError;

    fn from_str(line: &str) -> Result<Self, ParseError> {
        let tokens: Vec<&str> = line.split_whitespace().collect();
        let (&first, rest) = tokens.split_first().ok_or(ParseError::Empty)?;
        let at = first
            .strip_prefix('@')
            .ok_or_else(|| ParseError::MissingTime(first.to_string()))?;
        let at = Time(parse_num("time", at)?);
        let (&name, args) = rest
            .split_first()
            .ok_or_else(|| ParseError::UnknownCommand(String::new()))?;
        let op = match name {
            "enqueue" => {
                expect_args("enqueue", args, 2..=5, "2 to 5")?;
                let (mut delay, mut key, mut order) = (None, None, None);
                for &arg in &args[2..] {
                    if let Some(ms) = arg.strip_prefix("delay=") {
                        if delay.replace(Millis(parse_num("delay", ms)?)).is_some() {
                            return Err(ParseError::DuplicateOption("delay".into()));
                        }
                    } else if let Some(k) = arg.strip_prefix("key=") {
                        if key.replace(DedupKey::new(k)?).is_some() {
                            return Err(ParseError::DuplicateOption("key".into()));
                        }
                    } else if let Some(k) = arg.strip_prefix("order=") {
                        if order.replace(OrderKey::new(k)?).is_some() {
                            return Err(ParseError::DuplicateOption("order".into()));
                        }
                    } else {
                        return Err(ParseError::BadOption(arg.to_string()));
                    }
                }
                Op::Enqueue {
                    queue: QueueName::new(args[0])?,
                    payload: Payload::parse(args[1])?,
                    delay: delay.unwrap_or_default(),
                    key,
                    order,
                }
            }
            "lease" => {
                expect_args("lease", args, 2..=2, "2")?;
                Op::Lease {
                    queue: QueueName::new(args[0])?,
                    visibility: Millis(parse_num("visibility", args[1])?),
                }
            }
            "heartbeat" => {
                expect_args("heartbeat", args, 3..=3, "3")?;
                Op::Heartbeat {
                    job: JobId(parse_num("job", args[0])?),
                    token: Token(parse_num("token", args[1])?),
                    visibility: Millis(parse_num("visibility", args[2])?),
                }
            }
            "ack" => {
                expect_args("ack", args, 2..=2, "2")?;
                Op::Ack {
                    job: JobId(parse_num("job", args[0])?),
                    token: Token(parse_num("token", args[1])?),
                }
            }
            "nack" => {
                expect_args("nack", args, 2..=2, "2")?;
                Op::Nack {
                    job: JobId(parse_num("job", args[0])?),
                    token: Token(parse_num("token", args[1])?),
                }
            }
            "complete" => {
                expect_args("complete", args, 3..=3, "3")?;
                Op::Complete {
                    job: JobId(parse_num("job", args[0])?),
                    token: Token(parse_num("token", args[1])?),
                    result: Payload::parse(args[2])?,
                }
            }
            "result" => {
                expect_args("result", args, 1..=1, "1")?;
                Op::Result {
                    job: JobId(parse_num("job", args[0])?),
                }
            }
            "configure" => {
                expect_args("configure", args, 4..=4, "4")?;
                Op::Configure {
                    queue: QueueName::new(args[0])?,
                    config: QueueConfig {
                        max_attempts: parse_num("max_attempts", args[1])?,
                        backoff_base: Millis(parse_num("backoff_base", args[2])?),
                        backoff_cap: Millis(parse_num("backoff_cap", args[3])?),
                    },
                }
            }
            "redrive" => {
                expect_args("redrive", args, 1..=1, "1")?;
                Op::Redrive {
                    queue: QueueName::new(args[0])?,
                }
            }
            "subscribe" => {
                expect_args("subscribe", args, 2..=2, "2")?;
                Op::Subscribe {
                    queue: QueueName::new(args[0])?,
                    group: QueueName::new(args[1])?,
                }
            }
            "tick" => {
                expect_args("tick", args, 0..=0, "0")?;
                Op::Tick
            }
            other => return Err(ParseError::UnknownCommand(other.to_string())),
        };
        Ok(Command { at, op })
    }
}

fn expect_args(
    command: &'static str,
    args: &[&str],
    allowed: std::ops::RangeInclusive<usize>,
    expected: &'static str,
) -> Result<(), ParseError> {
    if allowed.contains(&args.len()) {
        Ok(())
    } else {
        Err(ParseError::WrongArgCount {
            command,
            expected,
            got: args.len(),
        })
    }
}

/// Parse a non-negative decimal integer. Rejects a leading `+`, which `str::parse`
/// would accept, so every accepted number has exactly one spelling and the text
/// format round-trips in both directions.
fn parse_num<T: FromStr>(field: &'static str, s: &str) -> Result<T, ParseError> {
    let bad = || ParseError::BadNumber {
        field,
        value: s.to_string(),
    };
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad());
    }
    s.parse().map_err(|_| bad())
}

/// Prints the same format `from_str` parses, so `cmd.to_string().parse() == Ok(cmd)`.
impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "@{} ", self.at)?;
        match &self.op {
            Op::Enqueue {
                queue,
                payload,
                delay,
                key,
                order,
            } => {
                write!(f, "enqueue {queue} {payload}")?;
                // Delay 0 is the default and is left out, so each command has one line form.
                if delay.0 > 0 {
                    write!(f, " delay={delay}")?;
                }
                if let Some(key) = key {
                    write!(f, " key={key}")?;
                }
                if let Some(order) = order {
                    write!(f, " order={order}")?;
                }
                Ok(())
            }
            Op::Lease { queue, visibility } => write!(f, "lease {queue} {visibility}"),
            Op::Heartbeat {
                job,
                token,
                visibility,
            } => write!(f, "heartbeat {job} {token} {visibility}"),
            Op::Ack { job, token } => write!(f, "ack {job} {token}"),
            Op::Nack { job, token } => write!(f, "nack {job} {token}"),
            Op::Complete { job, token, result } => write!(f, "complete {job} {token} {result}"),
            Op::Result { job } => write!(f, "result {job}"),
            Op::Configure { queue, config } => write!(f, "configure {queue} {config}"),
            Op::Redrive { queue } => write!(f, "redrive {queue}"),
            Op::Subscribe { queue, group } => write!(f, "subscribe {queue} {group}"),
            Op::Tick => write!(f, "tick"),
        }
    }
}

/// One line per event, `name key=value ...`, for scenario files to compare against.
impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Event::Enqueued {
                job,
                queue,
                ready_at,
                key,
                order,
            } => {
                write!(f, "enqueued job={job} queue={queue} ready_at={ready_at}")?;
                if let Some(key) = key {
                    write!(f, " key={key}")?;
                }
                if let Some(order) = order {
                    write!(f, " order={order}")?;
                }
                Ok(())
            }
            Event::Deduplicated { job, queue, key } => {
                write!(f, "deduplicated job={job} queue={queue} key={key}")
            }
            Event::Leased {
                lease,
                attempt,
                payload,
            } => write!(
                f,
                "leased job={} token={} deadline={} attempt={attempt} payload={payload}",
                lease.job, lease.token, lease.deadline
            ),
            Event::Empty { queue } => write!(f, "empty queue={queue}"),
            Event::Renewed { lease } => write!(
                f,
                "renewed job={} token={} deadline={}",
                lease.job, lease.token, lease.deadline
            ),
            Event::Acked { job } => write!(f, "acked job={job}"),
            Event::Completed { job, token } => write!(f, "completed job={job} token={token}"),
            Event::Result { job, status } => match status {
                ResultStatus::Pending => write!(f, "result job={job} pending"),
                ResultStatus::Done { token, payload } => {
                    write!(f, "result job={job} done token={token} payload={payload}")
                }
                ResultStatus::Unknown => write!(f, "result job={job} unknown"),
            },
            Event::Released { job, token, reason } => {
                write!(f, "released job={job} token={token} reason={reason}")
            }
            Event::Retrying { job, ready_at } => {
                write!(f, "retrying job={job} ready_at={ready_at}")
            }
            Event::DeadLettered { job } => write!(f, "dead job={job}"),
            Event::Redriven { job } => write!(f, "redriven job={job}"),
            Event::Configured { queue, config } => write!(
                f,
                "configured queue={queue} max_attempts={} backoff_base={} backoff_cap={}",
                config.max_attempts, config.backoff_base, config.backoff_cap
            ),
            Event::Subscribed { queue, group } => {
                write!(f, "subscribed queue={queue} group={group}")
            }
            Event::Rejected { reason } => write!(f, "rejected reason={reason}"),
        }
    }
}

impl fmt::Display for ReleaseReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ReleaseReason::Nack => "nack",
            ReleaseReason::Expired => "expired",
        })
    }
}

impl fmt::Display for RejectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            RejectReason::UnknownJob => "unknown_job",
            RejectReason::NotLeased => "not_leased",
            RejectReason::StaleToken => "stale_token",
            RejectReason::ZeroVisibility => "zero_visibility",
            RejectReason::BadConfig => "bad_config",
            RejectReason::BadGroup => "bad_group",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(name: &str) -> QueueName {
        QueueName::new(name).unwrap()
    }

    fn cmd(at: u64, op: Op) -> Command {
        Command { at: Time(at), op }
    }

    #[test]
    fn parses_every_command() {
        let cases = [
            (
                "@0 enqueue emails send:42",
                cmd(
                    0,
                    Op::Enqueue {
                        queue: q("emails"),
                        payload: Payload(b"send:42".to_vec()),
                        delay: Millis(0),
                        key: None,
                        order: None,
                    },
                ),
            ),
            (
                "@0 enqueue emails - delay=500",
                cmd(
                    0,
                    Op::Enqueue {
                        queue: q("emails"),
                        payload: Payload(Vec::new()),
                        delay: Millis(500),
                        key: None,
                        order: None,
                    },
                ),
            ),
            (
                "@0 enqueue emails x delay=5 key=order-7",
                cmd(
                    0,
                    Op::Enqueue {
                        queue: q("emails"),
                        payload: Payload(b"x".to_vec()),
                        delay: Millis(5),
                        key: Some(DedupKey::new("order-7").unwrap()),
                        order: None,
                    },
                ),
            ),
            (
                "@1 configure emails 3 100 2000",
                cmd(
                    1,
                    Op::Configure {
                        queue: q("emails"),
                        config: QueueConfig {
                            max_attempts: 3,
                            backoff_base: Millis(100),
                            backoff_cap: Millis(2000),
                        },
                    },
                ),
            ),
            (
                "@2 redrive emails",
                cmd(2, Op::Redrive { queue: q("emails") }),
            ),
            (
                "@5 lease emails 30000",
                cmd(
                    5,
                    Op::Lease {
                        queue: q("emails"),
                        visibility: Millis(30000),
                    },
                ),
            ),
            (
                "@6 heartbeat 1 7 30000",
                cmd(
                    6,
                    Op::Heartbeat {
                        job: JobId(1),
                        token: Token(7),
                        visibility: Millis(30000),
                    },
                ),
            ),
            (
                "@7 ack 1 7",
                cmd(
                    7,
                    Op::Ack {
                        job: JobId(1),
                        token: Token(7),
                    },
                ),
            ),
            (
                "@8 nack 1 7",
                cmd(
                    8,
                    Op::Nack {
                        job: JobId(1),
                        token: Token(7),
                    },
                ),
            ),
            ("@9 tick", cmd(9, Op::Tick)),
            (
                "@10 complete 1 7 ok%20done",
                cmd(
                    10,
                    Op::Complete {
                        job: JobId(1),
                        token: Token(7),
                        result: Payload(b"ok done".to_vec()),
                    },
                ),
            ),
            ("@11 result 4", cmd(11, Op::Result { job: JobId(4) })),
        ];
        for (text, expected) in cases {
            assert_eq!(text.parse::<Command>(), Ok(expected.clone()), "{text}");
            assert_eq!(expected.to_string(), text);
        }
    }

    #[test]
    fn whitespace_is_flexible_on_input() {
        assert_eq!(
            "  @3   ack\t1  2 ".parse::<Command>(),
            Ok(cmd(
                3,
                Op::Ack {
                    job: JobId(1),
                    token: Token(2)
                }
            ))
        );
    }

    #[test]
    fn parse_errors() {
        use ParseError::*;
        let wrong = |command, expected, got| WrongArgCount {
            command,
            expected,
            got,
        };
        let num = |field, value: &str| BadNumber {
            field,
            value: value.to_string(),
        };
        let cases = [
            ("", Empty),
            ("   ", Empty),
            ("enqueue emails x", MissingTime("enqueue".into())),
            ("@", num("time", "")),
            ("@-1 tick", num("time", "-1")),
            ("@+1 tick", num("time", "+1")),
            (
                "@18446744073709551616 tick",
                num("time", "18446744073709551616"),
            ),
            ("@1", UnknownCommand(String::new())),
            ("@1 push q x", UnknownCommand("push".into())),
            ("@1 enqueue q", wrong("enqueue", "2 to 5", 1)),
            (
                "@1 enqueue q a delay=1 key=k order=o b",
                wrong("enqueue", "2 to 5", 6),
            ),
            (
                "@1 enqueue q a order=x order=y",
                DuplicateOption("order".into()),
            ),
            ("@1 enqueue q a order=", BadOrderKey(String::new())),
            (
                "@1 enqueue q a delay=1 delay=2",
                DuplicateOption("delay".into()),
            ),
            ("@1 enqueue q a key=x key=y", DuplicateOption("key".into())),
            ("@1 enqueue q a key=", BadKey(String::new())),
            ("@1 enqueue q a b", BadOption("b".into())),
            ("@1 enqueue q a delay=", num("delay", "")),
            ("@1 enqueue q a delay=-5", num("delay", "-5")),
            ("@1 tick now", wrong("tick", "0", 1)),
            ("@1 ack 1", wrong("ack", "2", 1)),
            ("@1 complete 1 2", wrong("complete", "3", 2)),
            ("@1 complete 1 2 %G", BadPayload("%G".into())),
            ("@1 result", wrong("result", "1", 0)),
            ("@1 result x", num("job", "x")),
            ("@1 configure q 1 2", wrong("configure", "4", 3)),
            ("@1 configure q x 1 2", num("max_attempts", "x")),
            ("@1 configure q 1 2 c", num("backoff_cap", "c")),
            (
                "@1 configure q 4294967296 1 2",
                num("max_attempts", "4294967296"),
            ),
            ("@1 redrive", wrong("redrive", "1", 0)),
            ("@1 redrive a/b", BadQueueName("a/b".into())),
            ("@1 lease q/x 10", BadQueueName("q/x".into())),
            ("@1 lease q 1.5", num("visibility", "1.5")),
            ("@1 heartbeat x 1 1", num("job", "x")),
            ("@1 nack 1 -2", num("token", "-2")),
            ("@1 enqueue q %G0", BadPayload("%G0".into())),
        ];
        for (text, expected) in cases {
            assert_eq!(text.parse::<Command>(), Err(expected), "{text:?}");
        }
    }

    #[test]
    fn event_lines() {
        let lease = Lease {
            job: JobId(3),
            token: Token(9),
            deadline: Time(30_005),
        };
        let cases = [
            (
                Event::Enqueued {
                    job: JobId(3),
                    queue: q("emails"),
                    ready_at: Time(500),
                    key: None,
                    order: None,
                },
                "enqueued job=3 queue=emails ready_at=500",
            ),
            (
                Event::Enqueued {
                    job: JobId(3),
                    queue: q("emails"),
                    ready_at: Time(500),
                    key: Some(DedupKey::new("k1").unwrap()),
                    order: None,
                },
                "enqueued job=3 queue=emails ready_at=500 key=k1",
            ),
            (
                Event::Deduplicated {
                    job: JobId(3),
                    queue: q("emails"),
                    key: DedupKey::new("k1").unwrap(),
                },
                "deduplicated job=3 queue=emails key=k1",
            ),
            (
                Event::Leased {
                    lease,
                    attempt: 2,
                    payload: Payload(b"hi there".to_vec()),
                },
                "leased job=3 token=9 deadline=30005 attempt=2 payload=hi%20there",
            ),
            (Event::Empty { queue: q("emails") }, "empty queue=emails"),
            (
                Event::Renewed { lease },
                "renewed job=3 token=9 deadline=30005",
            ),
            (Event::Acked { job: JobId(3) }, "acked job=3"),
            (
                Event::Completed {
                    job: JobId(3),
                    token: Token(9),
                },
                "completed job=3 token=9",
            ),
            (
                Event::Result {
                    job: JobId(3),
                    status: ResultStatus::Pending,
                },
                "result job=3 pending",
            ),
            (
                Event::Result {
                    job: JobId(3),
                    status: ResultStatus::Done {
                        token: Token(9),
                        payload: Payload(b"a b".to_vec()),
                    },
                },
                "result job=3 done token=9 payload=a%20b",
            ),
            (
                Event::Result {
                    job: JobId(3),
                    status: ResultStatus::Unknown,
                },
                "result job=3 unknown",
            ),
            (
                Event::Released {
                    job: JobId(3),
                    token: Token(9),
                    reason: ReleaseReason::Expired,
                },
                "released job=3 token=9 reason=expired",
            ),
            (
                Event::Retrying {
                    job: JobId(3),
                    ready_at: Time(31_000),
                },
                "retrying job=3 ready_at=31000",
            ),
            (Event::DeadLettered { job: JobId(3) }, "dead job=3"),
            (Event::Redriven { job: JobId(3) }, "redriven job=3"),
            (
                Event::Configured {
                    queue: q("emails"),
                    config: QueueConfig::default(),
                },
                "configured queue=emails max_attempts=5 backoff_base=1000 backoff_cap=300000",
            ),
            (
                Event::Rejected {
                    reason: RejectReason::StaleToken,
                },
                "rejected reason=stale_token",
            ),
            (
                Event::Rejected {
                    reason: RejectReason::BadConfig,
                },
                "rejected reason=bad_config",
            ),
        ];
        for (event, text) in cases {
            assert_eq!(event.to_string(), text);
        }
    }
}
