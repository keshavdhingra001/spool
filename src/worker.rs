//! The worker loop (D34, D36): lease, run the handler while heartbeating,
//! ack on success, nack on failure, and drop the handler when the lease is
//! lost.

use std::future::Future;
use std::hash::{BuildHasher, RandomState};
use std::time::Duration;

use crate::client::{Client, ClientError, Leased};
use crate::command::RejectReason;
use crate::types::{JobId, Millis, QueueName};

#[derive(Clone, Copy, Debug)]
pub struct WorkerOptions {
    /// Visibility timeout of every lease; heartbeats go every third of it.
    pub visibility: Millis,
    /// First wait after an empty lease; doubles up to `idle_max` (D34).
    pub idle_min: Duration,
    pub idle_max: Duration,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        WorkerOptions {
            visibility: Millis(30_000),
            idle_min: Duration::from_millis(10),
            idle_max: Duration::from_secs(1),
        }
    }
}

/// What one turn of the loop did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The queue had no ready job.
    Idle,
    /// The handler succeeded and the ack was accepted.
    Acked(JobId),
    /// The handler failed and the job was given back.
    Nacked(JobId),
    /// The lease was lost while the handler ran (its heartbeat was rejected,
    /// and the handler was dropped) or by the time of the ack or nack.
    Lost(JobId, RejectReason),
}

pub struct Worker {
    client: Client,
    queue: QueueName,
    options: WorkerOptions,
}

impl Worker {
    pub fn new(client: Client, queue: QueueName, options: WorkerOptions) -> Self {
        Worker {
            client,
            queue,
            options,
        }
    }

    /// Process jobs until a call to the server fails. Stop it by dropping or
    /// aborting the task that runs it.
    pub async fn run<F, Fut, E>(&self, mut handler: F) -> Result<(), ClientError>
    where
        F: FnMut(Leased) -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        let mut idle = self.options.idle_min;
        let seed = RandomState::new();
        let mut turn = 0u64;
        loop {
            match self.step(&mut handler).await? {
                Outcome::Idle => {
                    // Equal jitter, as the queue's retries (D13), so idle
                    // workers do not poll in lockstep.
                    turn += 1;
                    let half = idle / 2;
                    let spread = (idle - half).as_micros() as u64 + 1;
                    let extra = Duration::from_micros(seed.hash_one(turn) % spread);
                    tokio::time::sleep(half + extra).await;
                    idle = (idle * 2).min(self.options.idle_max);
                }
                _ => idle = self.options.idle_min,
            }
        }
    }

    /// One turn: lease a job and see it through, or report the queue empty.
    pub async fn step<F, Fut, E>(&self, handler: &mut F) -> Result<Outcome, ClientError>
    where
        F: FnMut(Leased) -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        let visibility = self.options.visibility;
        let Some(job) = self.client.lease(&self.queue, visibility).await? else {
            return Ok(Outcome::Idle);
        };
        let lease = job.lease;
        let period = Duration::from_millis((visibility.0 / 3).max(1));
        let mut beat = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        let work = handler(job);
        tokio::pin!(work);
        let result = loop {
            tokio::select! {
                result = &mut work => break result,
                _ = beat.tick() => {
                    match self.client.heartbeat(lease.job, lease.token, visibility).await {
                        Ok(_) => {}
                        // The lease is gone: drop the handler's future, ack nothing.
                        Err(ClientError::Rejected(reason)) => {
                            return Ok(Outcome::Lost(lease.job, reason));
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        };
        let done = match result {
            Ok(()) => self.client.ack(lease.job, lease.token).await,
            Err(_) => self.client.nack(lease.job, lease.token).await,
        };
        match done {
            Ok(()) if result.is_ok() => Ok(Outcome::Acked(lease.job)),
            Ok(()) => Ok(Outcome::Nacked(lease.job)),
            Err(ClientError::Rejected(reason)) => Ok(Outcome::Lost(lease.job, reason)),
            Err(e) => Err(e),
        }
    }
}
