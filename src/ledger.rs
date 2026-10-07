//! A second, independent check (D19): rebuild every job's state from the event
//! stream alone and reject any event that is impossible in that state.
//!
//! The invariant checker looks at the queue's internals; the ledger looks only at
//! what the queue said. A bug that corrupts both the state and the checker's view
//! of it (or emits events that disagree with the state) is caught here.

use std::collections::BTreeMap;

use crate::command::Event;
use crate::reference::Counts;
use crate::types::{JobId, Payload, Token};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Waiting,
    Leased(Token),
    /// Between `Released` and the `Retrying`/`DeadLettered` that must follow it
    /// within the same command.
    Released,
    Dead,
}

#[derive(Clone, Debug)]
struct Entry {
    state: State,
    attempts: u32,
    /// Learned from the first lease; every later lease must carry the same bytes.
    payload: Option<Payload>,
}

#[derive(Clone, Debug, Default)]
pub struct Ledger {
    jobs: BTreeMap<JobId, Entry>,
    last_job: u64,
    last_token: u64,
    acked: u64,
}

impl Ledger {
    pub fn new() -> Self {
        Self::default()
    }

    /// Check the events one command produced, in order, and apply them.
    pub fn observe(&mut self, events: &[Event]) -> Result<(), String> {
        for event in events {
            self.observe_one(event)
                .map_err(|e| format!("event `{event}`: {e}"))?;
        }
        match self.jobs.iter().find(|(_, e)| e.state == State::Released) {
            Some((id, _)) => Err(format!("job {id} released without a retry or dead-letter")),
            None => Ok(()),
        }
    }

    fn observe_one(&mut self, event: &Event) -> Result<(), String> {
        match event {
            Event::Enqueued { job, .. } => {
                if job.0 != self.last_job + 1 {
                    return Err(format!("expected id {}", self.last_job + 1));
                }
                self.last_job = job.0;
                self.jobs.insert(
                    *job,
                    Entry {
                        state: State::Waiting,
                        attempts: 0,
                        payload: None,
                    },
                );
            }
            Event::Leased {
                lease,
                attempt,
                payload,
            } => {
                if lease.token.0 <= self.last_token {
                    return Err(format!("token not above {}", self.last_token));
                }
                self.last_token = lease.token.0;
                let e = self.entry(lease.job, State::Waiting)?;
                if *attempt != e.attempts + 1 {
                    return Err(format!("attempt should be {}", e.attempts + 1));
                }
                if e.payload.as_ref().is_some_and(|p| p != payload) {
                    return Err("payload changed between leases".into());
                }
                e.attempts = *attempt;
                e.payload = Some(payload.clone());
                e.state = State::Leased(lease.token);
            }
            Event::Renewed { lease } => {
                self.entry(lease.job, State::Leased(lease.token))?;
            }
            Event::Acked { job } => {
                let state = self.jobs.get(job).map(|e| e.state);
                if !matches!(state, Some(State::Leased(_))) {
                    return Err(format!("acked in state {state:?}"));
                }
                self.jobs.remove(job);
                self.acked += 1;
            }
            Event::Released { job, token, .. } => {
                self.entry(*job, State::Leased(*token))?.state = State::Released;
            }
            Event::Retrying { job, .. } => {
                self.entry(*job, State::Released)?.state = State::Waiting;
            }
            Event::DeadLettered { job } => {
                self.entry(*job, State::Released)?.state = State::Dead;
            }
            Event::Redriven { job } => {
                let e = self.entry(*job, State::Dead)?;
                e.state = State::Waiting;
                e.attempts = 0;
            }
            Event::Empty { .. } | Event::Configured { .. } | Event::Rejected { .. } => {}
        }
        Ok(())
    }

    /// The job's entry, which must be in state `expected`.
    fn entry(&mut self, job: JobId, expected: State) -> Result<&mut Entry, String> {
        let e = self.jobs.get_mut(&job).ok_or("unknown job")?;
        if e.state != expected {
            return Err(format!("job in state {:?}, expected {expected:?}", e.state));
        }
        Ok(e)
    }

    pub fn counts(&self) -> Counts {
        let mut c = Counts {
            acked: self.acked,
            ..Counts::default()
        };
        for e in self.jobs.values() {
            match e.state {
                State::Waiting => c.waiting += 1,
                State::Leased(_) => c.leased += 1,
                State::Dead => c.dead += 1,
                State::Released => {}
            }
        }
        c
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::ReleaseReason;
    use crate::types::{Lease, QueueName, Time};

    fn enqueued(job: u64) -> Event {
        Event::Enqueued {
            job: JobId(job),
            queue: QueueName::new("a").unwrap(),
            ready_at: Time(0),
        }
    }

    fn leased(job: u64, token: u64, attempt: u32, payload: &[u8]) -> Event {
        Event::Leased {
            lease: Lease {
                job: JobId(job),
                token: Token(token),
                deadline: Time(10),
            },
            attempt,
            payload: Payload(payload.to_vec()),
        }
    }

    fn released(job: u64, token: u64) -> Event {
        Event::Released {
            job: JobId(job),
            token: Token(token),
            reason: ReleaseReason::Nack,
        }
    }

    fn retrying(job: u64) -> Event {
        Event::Retrying {
            job: JobId(job),
            ready_at: Time(0),
        }
    }

    #[test]
    fn accepts_a_valid_history() {
        let mut l = Ledger::new();
        l.observe(&[enqueued(1), enqueued(2)]).unwrap();
        l.observe(&[leased(1, 1, 1, b"x")]).unwrap();
        l.observe(&[released(1, 1), retrying(1)]).unwrap();
        l.observe(&[leased(1, 2, 2, b"x")]).unwrap();
        l.observe(&[Event::Acked { job: JobId(1) }]).unwrap();
        let c = l.counts();
        assert_eq!((c.waiting, c.leased, c.dead, c.acked), (1, 0, 0, 1));
    }

    #[test]
    fn rejects_impossible_histories() {
        let bad: [(&str, Vec<Vec<Event>>); 8] = [
            ("skipped id", vec![vec![enqueued(2)]]),
            ("lease unknown", vec![vec![leased(1, 1, 1, b"")]]),
            (
                "double lease",
                vec![
                    vec![enqueued(1)],
                    vec![leased(1, 1, 1, b""), leased(1, 2, 2, b"")],
                ],
            ),
            (
                "token reuse",
                vec![
                    vec![enqueued(1), enqueued(2)],
                    vec![leased(1, 5, 1, b""), leased(2, 5, 1, b"")],
                ],
            ),
            (
                "wrong attempt",
                vec![vec![enqueued(1)], vec![leased(1, 1, 2, b"")]],
            ),
            (
                "release with wrong token",
                vec![
                    vec![enqueued(1)],
                    vec![leased(1, 1, 1, b"")],
                    vec![released(1, 2), retrying(1)],
                ],
            ),
            (
                "release not resolved",
                vec![
                    vec![enqueued(1)],
                    vec![leased(1, 1, 1, b"")],
                    vec![released(1, 1)],
                ],
            ),
            (
                "payload changed",
                vec![
                    vec![enqueued(1)],
                    vec![leased(1, 1, 1, b"x")],
                    vec![released(1, 1), retrying(1)],
                    vec![leased(1, 2, 2, b"y")],
                ],
            ),
        ];
        for (name, commands) in bad {
            let mut l = Ledger::new();
            let result = commands.iter().try_for_each(|events| l.observe(events));
            assert!(result.is_err(), "{name} accepted");
        }
    }
}
