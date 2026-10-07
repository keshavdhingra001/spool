//! The interface every queue implementation provides. M1 adds the reference queue
//! (simple, obviously correct, kept forever as the oracle); later milestones wrap
//! it in a log (M2) and in Raft (M7) without changing this trait.

use crate::command::{Command, Event};
use crate::types::Time;

pub trait Queue {
    /// Apply one command and append the events it causes to `out`, in order.
    ///
    /// `out` is owned by the caller and not cleared, so a hot loop can reuse one
    /// buffer instead of allocating per command (D11).
    fn apply(&mut self, cmd: &Command, out: &mut Vec<Event>);

    /// The queue's logical clock: the largest command time seen so far (D9).
    fn now(&self) -> Time;
}

/// The queue's logical clock (D9). It only moves forward: a command stamped
/// earlier than the clock runs at the clock's time instead of being rejected.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Clock {
    now: Time,
}

impl Clock {
    pub fn now(&self) -> Time {
        self.now
    }

    /// Move to `max(now, at)` and return the time the command runs at.
    pub fn advance(&mut self, at: Time) -> Time {
        self.now = self.now.max(at);
        self.now
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_starts_at_zero_and_never_goes_back() {
        let mut c = Clock::default();
        assert_eq!(c.now(), Time(0));
        assert_eq!(c.advance(Time(10)), Time(10));
        assert_eq!(c.advance(Time(10)), Time(10));
        assert_eq!(
            c.advance(Time(4)),
            Time(10),
            "late command runs at the clock"
        );
        assert_eq!(c.now(), Time(10));
        assert_eq!(c.advance(Time(11)), Time(11));
        assert_eq!(c.advance(Time(u64::MAX)), Time(u64::MAX));
    }
}
