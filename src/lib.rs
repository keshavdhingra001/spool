//! spool: a distributed task queue.
//!
//! The core is a pure state machine (D4): [`Command`]s in, [`Event`]s out, with
//! logical time carried inside each command. Networking, storage and replication
//! wrap it from the outside in later milestones.

#![forbid(unsafe_code)]

pub mod check;
pub mod client;
pub mod cluster;
pub mod codec;
pub mod command;
pub mod durable;
pub mod error;
pub mod fence;
pub mod ledger;
pub mod protocol;
pub mod queue;
pub mod raft;
pub mod reference;
pub mod replica;
pub mod retry;
pub mod scenario;
pub mod server;
pub mod sim;
pub mod storage;
pub mod types;
pub mod wal;
pub mod worker;

pub use check::Checked;
pub use command::{Command, Event, Op, RejectReason, ReleaseReason, ResultStatus};
pub use durable::{Durable, Options, Recovery};
pub use error::{ParseError, StoreError};
pub use queue::{Clock, Queue, Snapshot};
pub use reference::ReferenceQueue;
pub use retry::QueueConfig;
pub use types::{DedupKey, JobId, Lease, Millis, Payload, QueueName, Time, Token};
