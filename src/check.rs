//! The reference queue with both checks (D19) run after every command: the
//! structural invariant checker and the event-only ledger, plus agreement
//! between the two on how many jobs are in each state. Used by scenario files,
//! random tests and the REPL.

use crate::command::{Command, Event};
use crate::ledger::Ledger;
use crate::queue::Queue;
use crate::reference::ReferenceQueue;

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

    /// Apply `cmd` and return its events, or the first violated invariant.
    pub fn apply(&mut self, cmd: &Command) -> Result<&[Event], String> {
        self.out.clear();
        self.queue.apply(cmd, &mut self.out);
        self.queue.check_invariants()?;
        self.ledger.observe(&self.out)?;
        let (state, events) = (self.queue.counts(), self.ledger.counts());
        if state != events {
            return Err(format!(
                "queue counts {state:?} disagree with the event ledger {events:?}"
            ));
        }
        Ok(&self.out)
    }
}
