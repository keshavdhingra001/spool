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
//! @<ms> enqueue   <queue> <payload>
//! @<ms> lease     <queue> <visibility_ms>
//! @<ms> heartbeat <job> <token> <visibility_ms>
//! @<ms> ack       <job> <token>
//! @<ms> nack      <job> <token>
//! @<ms> tick
//! ```

use std::fmt;
use std::str::FromStr;

use crate::error::ParseError;
use crate::types::{JobId, Lease, Millis, Payload, QueueName, Time, Token};

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
    /// Add a job to `queue`. The queue assigns its id (D10).
    Enqueue { queue: QueueName, payload: Payload },
    /// Lease the oldest ready job in `queue`, hidden from other workers until
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
    /// The job failed; give up the lease. What happens next (retry, backoff,
    /// dead-letter) is M1's policy.
    Nack { job: JobId, token: Token },
    /// Only advance the clock, expiring leases that are past their deadline.
    Tick,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// A job was added and given `job` as its id.
    Enqueued { job: JobId, queue: QueueName },
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
    /// The lease ended without an ack, by nack or by reaching its deadline.
    Released {
        job: JobId,
        token: Token,
        reason: ReleaseReason,
    },
    /// The command was refused and changed nothing except the clock.
    Rejected { reason: RejectReason },
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
                expect_args("enqueue", args, 2)?;
                Op::Enqueue {
                    queue: QueueName::new(args[0])?,
                    payload: Payload::parse(args[1])?,
                }
            }
            "lease" => {
                expect_args("lease", args, 2)?;
                Op::Lease {
                    queue: QueueName::new(args[0])?,
                    visibility: Millis(parse_num("visibility", args[1])?),
                }
            }
            "heartbeat" => {
                expect_args("heartbeat", args, 3)?;
                Op::Heartbeat {
                    job: JobId(parse_num("job", args[0])?),
                    token: Token(parse_num("token", args[1])?),
                    visibility: Millis(parse_num("visibility", args[2])?),
                }
            }
            "ack" => {
                expect_args("ack", args, 2)?;
                Op::Ack {
                    job: JobId(parse_num("job", args[0])?),
                    token: Token(parse_num("token", args[1])?),
                }
            }
            "nack" => {
                expect_args("nack", args, 2)?;
                Op::Nack {
                    job: JobId(parse_num("job", args[0])?),
                    token: Token(parse_num("token", args[1])?),
                }
            }
            "tick" => {
                expect_args("tick", args, 0)?;
                Op::Tick
            }
            other => return Err(ParseError::UnknownCommand(other.to_string())),
        };
        Ok(Command { at, op })
    }
}

fn expect_args(command: &'static str, args: &[&str], expected: usize) -> Result<(), ParseError> {
    if args.len() == expected {
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
            Op::Enqueue { queue, payload } => write!(f, "enqueue {queue} {payload}"),
            Op::Lease { queue, visibility } => write!(f, "lease {queue} {visibility}"),
            Op::Heartbeat {
                job,
                token,
                visibility,
            } => write!(f, "heartbeat {job} {token} {visibility}"),
            Op::Ack { job, token } => write!(f, "ack {job} {token}"),
            Op::Nack { job, token } => write!(f, "nack {job} {token}"),
            Op::Tick => write!(f, "tick"),
        }
    }
}

/// One line per event, `name key=value ...`, for scenario files to compare against.
impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Event::Enqueued { job, queue } => write!(f, "enqueued job={job} queue={queue}"),
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
            Event::Released { job, token, reason } => {
                write!(f, "released job={job} token={token} reason={reason}")
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
                    },
                ),
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
            ("@1 enqueue q", wrong("enqueue", 2, 1)),
            ("@1 enqueue q a b", wrong("enqueue", 2, 3)),
            ("@1 tick now", wrong("tick", 0, 1)),
            ("@1 ack 1", wrong("ack", 2, 1)),
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
                },
                "enqueued job=3 queue=emails",
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
                Event::Released {
                    job: JobId(3),
                    token: Token(9),
                    reason: ReleaseReason::Expired,
                },
                "released job=3 token=9 reason=expired",
            ),
            (
                Event::Rejected {
                    reason: RejectReason::StaleToken,
                },
                "rejected reason=stale_token",
            ),
        ];
        for (event, text) in cases {
            assert_eq!(event.to_string(), text);
        }
    }
}
