//! The worker loop (D34, D36, D44): lease, run the handler while
//! heartbeating, ack or complete on success, nack on failure, and drop the
//! handler when the lease is lost.

use std::future::Future;
use std::hash::{BuildHasher, RandomState};
use std::time::Duration;

use crate::client::{Client, ClientError, Leased};
use crate::command::RejectReason;
use crate::types::{JobId, Millis, Payload, QueueName};

/// How a handler's success is reported (D44): `()` acks, a `Payload`
/// completes the job with that result (D41).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Completion {
    Ack,
    Result(Payload),
}

impl From<()> for Completion {
    fn from((): ()) -> Self {
        Completion::Ack
    }
}

impl From<Payload> for Completion {
    fn from(p: Payload) -> Self {
        Completion::Result(p)
    }
}

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
    /// The handler succeeded with a result and the complete was accepted.
    Completed(JobId),
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
    /// Where to reconnect if a complete's reply is lost (D44).
    addr: Option<String>,
}

impl Worker {
    pub fn new(client: Client, queue: QueueName, options: WorkerOptions) -> Self {
        Worker {
            client,
            queue,
            options,
            addr: None,
        }
    }

    /// Connect to `addr`; if a complete's reply is lost later, reconnect there
    /// and retry it once (D42).
    pub async fn connect(
        addr: &str,
        queue: QueueName,
        options: WorkerOptions,
    ) -> Result<Self, ClientError> {
        Ok(Worker {
            client: Client::connect(addr).await?,
            queue,
            options,
            addr: Some(addr.to_string()),
        })
    }

    /// Process jobs until a call to the server fails. Stop it by dropping or
    /// aborting the task that runs it.
    pub async fn run<F, Fut, T, E>(&mut self, mut handler: F) -> Result<(), ClientError>
    where
        F: FnMut(Leased) -> Fut,
        Fut: Future<Output = Result<T, E>>,
        T: Into<Completion>,
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
    pub async fn step<F, Fut, T, E>(&mut self, handler: &mut F) -> Result<Outcome, ClientError>
    where
        F: FnMut(Leased) -> Fut,
        Fut: Future<Output = Result<T, E>>,
        T: Into<Completion>,
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
        let (done, outcome) = match result.map(Into::into) {
            Ok(Completion::Ack) => (
                self.client.ack(lease.job, lease.token).await,
                Outcome::Acked(lease.job),
            ),
            Ok(Completion::Result(payload)) => (
                self.complete(lease.job, lease.token, payload).await,
                Outcome::Completed(lease.job),
            ),
            Err(_) => (
                self.client.nack(lease.job, lease.token).await,
                Outcome::Nacked(lease.job),
            ),
        };
        match done {
            Ok(()) => Ok(outcome),
            Err(ClientError::Rejected(reason)) => Ok(Outcome::Lost(lease.job, reason)),
            Err(e) => Err(e),
        }
    }

    /// Complete, and if the connection dies before the reply, reconnect and
    /// send the same complete again: it lands at most once either way (D42).
    async fn complete(
        &mut self,
        job: JobId,
        token: crate::types::Token,
        payload: Payload,
    ) -> Result<(), ClientError> {
        match self.client.complete(job, token, payload.clone()).await {
            Err(ClientError::Closed | ClientError::Io(_)) if self.addr.is_some() => {
                let addr = self.addr.as_deref().expect("checked");
                self.client = Client::connect(addr).await?;
                self.client.complete(job, token, payload).await
            }
            other => other,
        }
    }
}
