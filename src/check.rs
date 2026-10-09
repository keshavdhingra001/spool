//! The reference queue with both checks (D19) run after every command: the
//! structural invariant checker and the event-only ledger, plus agreement
//! between the two on how many jobs are in each state. Used by scenario files,
//! random tests and the REPL.

use crate::command::{Command, Event};
use crate::ledger::Ledger;
use crate::queue::Queue;
use crate::reference::{Counts, ReferenceQueue};

#[derive(Clone, Debug, Default)]
pub struct Checked {
    pub queue: ReferenceQueue,
    ledger: Ledger,
    out: Vec<Event>,
}

impl Checked {
    pub fn new() -> Self {
        Self::default()
    }

    /// The queue of `partition` (D77), checked.
    pub fn for_partition(partition: u16) -> Self {
        Checked {
            queue: ReferenceQueue::for_partition(partition),
            ledger: Ledger::for_partition(partition),
            out: Vec::new(),
        }
    }

    /// Apply `cmd` and return its events, or the first violated invariant.
    pub fn apply(&mut self, cmd: &Command) -> Result<&[Event], String> {
        self.out.clear();
        self.queue.apply(cmd, &mut self.out);
        self.queue.check_invariants()?;
        self.ledger.observe(&self.out)?;
        agree(self.queue.counts(), self.ledger.counts())?;
        Ok(&self.out)
    }
}

/// The queue and the ledger must agree on every state's count, not just the
/// total: a job the queue thinks is dead but the events say is waiting has the
/// same total.
pub(crate) fn agree(state: Counts, events: Counts) -> Result<(), String> {
    if state != events {
        return Err(format!(
            "queue counts {state:?} disagree with the event ledger {events:?}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_must_agree_per_state() {
        let c = Counts {
            waiting: 2,
            leased: 1,
            dead: 0,
            acked: 3,
        };
        assert_eq!(agree(c, c), Ok(()));
        let moved = Counts {
            waiting: 1,
            dead: 1,
            ..c
        };
        assert_eq!(moved.total(), c.total());
        assert!(agree(c, moved).is_err());
    }
}
