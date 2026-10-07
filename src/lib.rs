//! spool: a distributed task queue.
//!
//! The core is a pure state machine (D4): commands in, events out, with logical
//! time carried inside each command. Networking, storage and replication wrap it
//! from the outside in later milestones.

#![forbid(unsafe_code)]
