//! spool: a distributed task queue.
//!
//! The core is a pure state machine (D4): [`Command`]s in, [`Event`]s out, with
//! logical time carried inside each command. Networking, storage and replication
//! wrap it from the outside in later milestones.

#![forbid(unsafe_code)]

pub mod command;
pub mod error;
pub mod queue;
pub mod retry;
pub mod types;

pub use command::{Command, Event, Op, RejectReason, ReleaseReason};
pub use error::ParseError;
pub use queue::{Clock, Queue};
pub use retry::QueueConfig;
pub use types::{JobId, Lease, Millis, Payload, QueueName, Time, Token};
