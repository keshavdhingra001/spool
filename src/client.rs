//! The async client (D36): one connection, shared by every clone.
//!
//! A writer task gives each request the next id, records it in `pending` and
//! writes it; a reader task takes replies off the socket and hands each to the
//! oldest pending request, checking the id (D32). Calls from many tasks
//! therefore pipeline over one connection, which is what lets a worker
//! heartbeat while its handler is still waiting on another call.

use std::collections::VecDeque;
use std::io;
use std::sync::{Arc, Mutex};

use thiserror::Error;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, oneshot};

use crate::command::{Event, Op, RejectReason, ResultStatus};
use crate::protocol::{self, Reply, Request};
use crate::types::{DedupKey, JobId, Lease, Millis, Payload, QueueName, Token};

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("connection: {0}")]
    Io(#[from] io::Error),
    #[error("connection closed")]
    Closed,
    /// The server refused the connection or a frame (and has closed it).
    #[error("server error: {0}")]
    Server(String),
    /// The reply did not follow the protocol.
    #[error("protocol: {0}")]
    Protocol(String),
    /// The queue refused the command.
    #[error("rejected: {0}")]
    Rejected(RejectReason),
}

type ReplyTx = oneshot::Sender<Result<Vec<Event>, ClientError>>;

#[derive(Default)]
struct Pending {
    /// Requests written and not yet answered, oldest first.
    waiting: VecDeque<(u64, ReplyTx)>,
    /// Set by the reader when the connection ends; nothing is added after.
    closed: bool,
}

#[derive(Clone)]
pub struct Client {
    requests: mpsc::Sender<(Op, ReplyTx)>,
}

/// The result of an enqueue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Enqueued {
    pub job: JobId,
    /// True if the key was already used and no job was added (D35).
    pub deduplicated: bool,
}

/// A leased job, as the worker sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Leased {
    pub lease: Lease,
    pub attempt: u32,
    pub payload: Payload,
}

impl Client {
    pub async fn connect(addr: impl ToSocketAddrs) -> Result<Client, ClientError> {
        let mut stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let mut hello = Vec::new();
        let version = protocol::VERSION;
        protocol::encode_request(0, &Request::Hello { version }, &mut hello);
        protocol::write_all(&mut stream, &hello).await?;
        match read_reply(&mut stream).await? {
            // A server may answer with an older version it shares with us.
            Reply::HelloOk { version: v } if protocol::speaks(v) && v <= version => {}
            Reply::Error { message, .. } => return Err(ClientError::Server(message)),
            other => {
                return Err(ClientError::Protocol(format!(
                    "expected hello_ok, got {other:?}"
                )));
            }
        }

        let (mut rd, mut wr) = stream.into_split();
        let pending = Arc::new(Mutex::new(Pending::default()));
        let (requests, mut rx) = mpsc::channel::<(Op, ReplyTx)>(256);

        let writer_pending = pending.clone();
        tokio::spawn(async move {
            let mut next_id = 1u64;
            let mut buf = Vec::new();
            while let Some(first) = rx.recv().await {
                buf.clear();
                let mut next = Some(first);
                // Everything already queued goes out in one write.
                while let Some((op, reply)) = next.take().or_else(|| rx.try_recv().ok()) {
                    let id = next_id;
                    next_id += 1;
                    {
                        let mut p = writer_pending.lock().unwrap();
                        if p.closed {
                            // Dropping `reply` tells the caller the connection is gone.
                            return;
                        }
                        p.waiting.push_back((id, reply));
                    }
                    protocol::encode_request(id, &Request::Op(op), &mut buf);
                }
                if protocol::write_all(&mut wr, &buf).await.is_err() {
                    return;
                }
            }
            // Every clone is gone: dropping `wr` closes our side, the server
            // writes what is pending and closes, and the reader ends.
        });

        tokio::spawn(async move {
            let failure = loop {
                let frame = match protocol::read_frame(&mut rd).await {
                    Ok(Some(frame)) => frame,
                    Ok(None) => break None,
                    Err(e) => break Some(ClientError::Protocol(e.to_string())),
                };
                let Some((id, reply)) = pending.lock().unwrap().waiting.pop_front() else {
                    break Some(ClientError::Protocol("reply with no request".into()));
                };
                let result = match frame.reply() {
                    Ok(_) if frame.id != id => {
                        let e = format!("reply to request {}, expected {id}", frame.id);
                        let _ = reply.send(Err(ClientError::Protocol(e.clone())));
                        break Some(ClientError::Protocol(e));
                    }
                    Ok(Reply::Events(events)) => Ok(events),
                    Ok(Reply::Error { message, .. }) => Err(ClientError::Server(message)),
                    Ok(other) => Err(ClientError::Protocol(format!("unexpected {other:?}"))),
                    Err(e) => Err(ClientError::Protocol(e.to_string())),
                };
                let fatal = result.is_err();
                let _ = reply.send(result);
                if fatal {
                    break None;
                }
            };
            let mut p = pending.lock().unwrap();
            p.closed = true;
            // Everyone still waiting learns the connection is gone.
            for (_, reply) in p.waiting.drain(..) {
                let _ = reply.send(Err(failure.as_ref().map_or(ClientError::Closed, |e| {
                    ClientError::Protocol(e.to_string())
                })));
            }
        });
        Ok(Client { requests })
    }

    /// Send one op and return every event its command produced (D33),
    /// including expiry events of other jobs.
    pub async fn request(&self, op: Op) -> Result<Vec<Event>, ClientError> {
        let (tx, rx) = oneshot::channel();
        self.requests
            .send((op, tx))
            .await
            .map_err(|_| ClientError::Closed)?;
        rx.await.map_err(|_| ClientError::Closed)?
    }

    /// The event about the op itself: always the last one, after any expiry
    /// the command triggered first. A rejection becomes an error.
    async fn outcome(&self, op: Op) -> Result<Event, ClientError> {
        match self.request(op).await?.pop() {
            Some(Event::Rejected { reason }) => Err(ClientError::Rejected(reason)),
            Some(e) => Ok(e),
            None => Err(ClientError::Protocol("no events for the op".into())),
        }
    }

    pub async fn enqueue(
        &self,
        queue: &QueueName,
        payload: Payload,
        delay: Millis,
        key: Option<DedupKey>,
    ) -> Result<Enqueued, ClientError> {
        let op = Op::Enqueue {
            queue: queue.clone(),
            payload,
            delay,
            key,
        };
        match self.outcome(op).await? {
            Event::Enqueued { job, .. } => Ok(Enqueued {
                job,
                deduplicated: false,
            }),
            Event::Deduplicated { job, .. } => Ok(Enqueued {
                job,
                deduplicated: true,
            }),
            other => Err(unexpected(other)),
        }
    }

    /// Lease the next ready job, or `None` if there is none (D34).
    pub async fn lease(
        &self,
        queue: &QueueName,
        visibility: Millis,
    ) -> Result<Option<Leased>, ClientError> {
        let op = Op::Lease {
            queue: queue.clone(),
            visibility,
        };
        match self.outcome(op).await? {
            Event::Leased {
                lease,
                attempt,
                payload,
            } => Ok(Some(Leased {
                lease,
                attempt,
                payload,
            })),
            Event::Empty { .. } => Ok(None),
            other => Err(unexpected(other)),
        }
    }

    pub async fn heartbeat(
        &self,
        job: JobId,
        token: Token,
        visibility: Millis,
    ) -> Result<Lease, ClientError> {
        let op = Op::Heartbeat {
            job,
            token,
            visibility,
        };
        match self.outcome(op).await? {
            Event::Renewed { lease } => Ok(lease),
            other => Err(unexpected(other)),
        }
    }

    pub async fn ack(&self, job: JobId, token: Token) -> Result<(), ClientError> {
        match self.outcome(Op::Ack { job, token }).await? {
            Event::Acked { .. } => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    /// Finish the job with `result`: ack and store the result in one command
    /// (D41). Safe to repeat with the same token if the reply was lost (D42).
    pub async fn complete(
        &self,
        job: JobId,
        token: Token,
        result: Payload,
    ) -> Result<(), ClientError> {
        match self.outcome(Op::Complete { job, token, result }).await? {
            Event::Completed { .. } => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    /// Whether `job` is still queued, finished with a result, or neither (D41).
    pub async fn result(&self, job: JobId) -> Result<ResultStatus, ClientError> {
        match self.outcome(Op::Result { job }).await? {
            Event::Result { status, .. } => Ok(status),
            other => Err(unexpected(other)),
        }
    }

    /// Give the job back: it retries after a backoff or is dead-lettered.
    pub async fn nack(&self, job: JobId, token: Token) -> Result<(), ClientError> {
        match self.outcome(Op::Nack { job, token }).await? {
            Event::Retrying { .. } | Event::DeadLettered { .. } => Ok(()),
            other => Err(unexpected(other)),
        }
    }
}

fn unexpected(e: Event) -> ClientError {
    ClientError::Protocol(format!("unexpected event `{e}`"))
}

async fn read_reply(stream: &mut TcpStream) -> Result<Reply, ClientError> {
    let frame = protocol::read_frame(stream)
        .await
        .map_err(|e| ClientError::Protocol(e.to_string()))?
        .ok_or(ClientError::Closed)?;
    frame
        .reply()
        .map_err(|e| ClientError::Protocol(e.to_string()))
}
