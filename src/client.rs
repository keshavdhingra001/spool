//! The async client (D36): one connection, shared by every clone.
//!
//! A writer task gives each request the next id, records it in `pending` and
//! writes it; a reader task takes replies off the socket and hands each to the
//! oldest pending request, checking the id (D32). Calls from many tasks
//! therefore pipeline over one connection, which is what lets a worker
//! heartbeat while its handler is still waiting on another call.
//!
//! [`Client::cluster`] talks to a replicated queue (M7) of one or more
//! partitions (M8). It routes each op to its partition (D76, [`route`]),
//! keeps one connection per replica, and per partition the replica it
//! believes leads: it follows `not_leader` hints, moves to the next replica
//! when there is no hint or the connection breaks, and resends after
//! `unknown` (D71, D72). Every enqueue without a key gets a fresh one, so a
//! resent enqueue never adds a second job and always hashes to the same
//! partition (D35, D72, D76). An attempt that gets no answer within
//! [`ATTEMPT`] counts as a broken connection: a leader cut off from its
//! quorum never answers.

use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use thiserror::Error;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, oneshot};

use crate::command::{Event, Op, RejectReason, ResultStatus};
use crate::protocol::{self, Reply, Request};
use crate::route::{Route, route};
use crate::types::{DedupKey, JobId, Lease, Millis, OrderKey, Payload, QueueName, Token};

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
    /// The node is a replica that does not lead (D71).
    #[error("not the leader (leader: {0:?})")]
    NotLeader(Option<u32>),
    /// The request may or may not have applied (D70).
    #[error("outcome unknown")]
    Unknown,
}

type ReplyTx = oneshot::Sender<Result<Vec<Event>, ClientError>>;

/// How long a cluster client waits for one attempt's answer.
pub const ATTEMPT: Duration = Duration::from_secs(1);

#[derive(Default)]
struct Pending {
    /// Requests written and not yet answered, oldest first.
    waiting: VecDeque<(u64, ReplyTx)>,
    /// Set by the reader when the connection ends; nothing is added after.
    closed: bool,
}

#[derive(Clone)]
pub struct Client {
    mode: Mode,
}

#[derive(Clone)]
enum Mode {
    One(Conn),
    Cluster(Arc<Router>),
}

/// One connection's request queue: each op with its partition.
#[derive(Clone)]
struct Conn {
    requests: mpsc::Sender<(u16, Op, ReplyTx)>,
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
    /// One connection to one server. Errors are returned as they happen.
    pub async fn connect(addr: impl ToSocketAddrs) -> Result<Client, ClientError> {
        Ok(Client {
            mode: Mode::One(Conn::connect(addr).await?),
        })
    }

    /// A client of the replicated queue whose replicas are `members`, by
    /// Raft id, with `partitions` partitions (D74; the count every replica
    /// was started with). It connects on first use and retries as the
    /// module says for up to `retry_for`; the last error is returned after
    /// that.
    pub fn cluster(
        members: BTreeMap<u32, SocketAddr>,
        partitions: u16,
        retry_for: Duration,
    ) -> Client {
        assert!(!members.is_empty(), "a cluster has members");
        assert!(partitions > 0, "a cluster has partitions");
        let first = *members.keys().next().unwrap();
        let targets = (0..partitions)
            .map(|_| Target {
                node: first,
                generation: 0,
            })
            .collect();
        Client {
            mode: Mode::Cluster(Arc::new(Router {
                members,
                partitions,
                retry_for,
                targets: Mutex::new(targets),
                conns: Mutex::new(BTreeMap::new()),
                last_lease: AtomicU64::new(u64::from(partitions) - 1),
                keys: key_prefix(),
                next_key: AtomicU64::new(0),
            })),
        }
    }
}

impl Conn {
    async fn connect(addr: impl ToSocketAddrs) -> Result<Conn, ClientError> {
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
        let (requests, mut rx) = mpsc::channel::<(u16, Op, ReplyTx)>(256);

        let writer_pending = pending.clone();
        tokio::spawn(async move {
            let mut next_id = 1u64;
            let mut buf = Vec::new();
            while let Some(first) = rx.recv().await {
                buf.clear();
                let mut next = Some(first);
                // Everything already queued goes out in one write.
                while let Some((partition, op, reply)) = next.take().or_else(|| rx.try_recv().ok())
                {
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
                    // Partition 0 as a plain request, which every version
                    // speaks; others need version 3 (D76).
                    let req = match partition {
                        0 => Request::Op(op),
                        partition => Request::Routed { partition, op },
                    };
                    protocol::encode_request(id, &req, &mut buf);
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
                    Ok(Reply::NotLeader { leader }) => Err(ClientError::NotLeader(leader)),
                    Ok(Reply::Unknown) => Err(ClientError::Unknown),
                    Ok(Reply::Error { message, .. }) => Err(ClientError::Server(message)),
                    Ok(other) => Err(ClientError::Protocol(format!("unexpected {other:?}"))),
                    Err(e) => Err(ClientError::Protocol(e.to_string())),
                };
                let fatal = matches!(
                    result,
                    Err(ClientError::Server(_) | ClientError::Protocol(_))
                );
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
        Ok(Conn { requests })
    }

    async fn request(&self, partition: u16, op: Op) -> Result<Vec<Event>, ClientError> {
        let (tx, rx) = oneshot::channel();
        self.requests
            .send((partition, op, tx))
            .await
            .map_err(|_| ClientError::Closed)?;
        rx.await.map_err(|_| ClientError::Closed)?
    }
}

/// The cluster client's view: per partition, the replica it sends to; per
/// replica, the connection, shared by every partition that targets it.
struct Router {
    members: BTreeMap<u32, SocketAddr>,
    partitions: u16,
    retry_for: Duration,
    targets: Mutex<Vec<Target>>,
    conns: Mutex<BTreeMap<u32, Conn>>,
    /// The partition of the last lease that found a job (D78).
    last_lease: AtomicU64,
    keys: String,
    next_key: AtomicU64,
}

/// Where one partition's requests go. `generation` counts changes of
/// target, so that of several callers that saw the same failure, only the
/// first moves on.
#[derive(Clone, Copy)]
struct Target {
    node: u32,
    generation: u64,
}

/// A prefix no other client shares: the clock, the process and a counter of
/// clients in this process.
fn key_prefix() -> String {
    static CLIENTS: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let n = CLIENTS.fetch_add(1, Ordering::Relaxed);
    format!("auto-{nanos:x}-{:x}-{n:x}", std::process::id())
}

impl Router {
    /// Give an unkeyed enqueue its own key (D72).
    fn keyed(&self, op: Op) -> Op {
        match op {
            Op::Enqueue {
                queue,
                payload,
                delay,
                key: None,
                order,
            } => {
                let n = self.next_key.fetch_add(1, Ordering::Relaxed);
                let key = DedupKey::new(&format!("{}-{n:x}", self.keys)).expect("a valid key");
                Op::Enqueue {
                    queue,
                    payload,
                    delay,
                    key: Some(key),
                    order,
                }
            }
            op => op,
        }
    }

    /// Partition `p`'s target and the connection to it, dialed if there is
    /// none.
    async fn conn(&self, p: u16) -> Result<(Target, Conn), (Target, ClientError)> {
        let target = self.targets.lock().unwrap()[usize::from(p)];
        if let Some(conn) = self.conns.lock().unwrap().get(&target.node) {
            return Ok((target, conn.clone()));
        }
        let conn = Conn::connect(self.members[&target.node])
            .await
            .map_err(|e| (target, e))?;
        // Another caller may have dialed it meanwhile; keep one of the two.
        let conn = self
            .conns
            .lock()
            .unwrap()
            .entry(target.node)
            .or_insert(conn)
            .clone();
        Ok((target, conn))
    }

    /// Forget a connection that failed, unless it was replaced already.
    fn drop_conn(&self, node: u32, conn: &Conn) {
        let mut conns = self.conns.lock().unwrap();
        if conns
            .get(&node)
            .is_some_and(|c| c.requests.same_channel(&conn.requests))
        {
            conns.remove(&node);
        }
    }

    /// Move partition `p` on from `seen`: to `leader` if it is a member,
    /// else to the next replica.
    fn retarget(&self, p: u16, seen: Target, leader: Option<u32>) {
        let mut targets = self.targets.lock().unwrap();
        let t = &mut targets[usize::from(p)];
        if t.generation != seen.generation {
            return; // someone else moved on already
        }
        t.node = match leader.filter(|l| self.members.contains_key(l)) {
            Some(l) => l,
            None => self
                .members
                .range(t.node + 1..)
                .next()
                .or_else(|| self.members.iter().next())
                .map(|(&id, _)| id)
                .unwrap(),
        };
        t.generation += 1;
    }

    /// Send `op` to partition `p`'s leader, following redirects and retrying
    /// for up to `retry_for` from `started`.
    async fn to_partition(
        &self,
        p: u16,
        op: &Op,
        started: Instant,
    ) -> Result<Vec<Event>, ClientError> {
        let mut backoff = Duration::from_millis(10);
        loop {
            let error = match self.conn(p).await {
                Ok((target, conn)) => match tokio::time::timeout(
                    ATTEMPT.min(self.retry_for),
                    conn.request(p, op.clone()),
                )
                .await
                .unwrap_or(Err(ClientError::Closed))
                {
                    Ok(events) => return Ok(events),
                    Err(ClientError::NotLeader(Some(l))) if l != target.node => {
                        // A named leader: go there now.
                        self.retarget(p, target, Some(l));
                        continue;
                    }
                    Err(ClientError::NotLeader(leader)) => {
                        self.retarget(p, target, leader.filter(|&l| l != target.node));
                        ClientError::NotLeader(leader)
                    }
                    Err(e @ (ClientError::Closed | ClientError::Io(_))) => {
                        self.drop_conn(target.node, &conn);
                        self.retarget(p, target, None);
                        e
                    }
                    // Same replica, after a pause: it may know more by then.
                    Err(ClientError::Unknown) => ClientError::Unknown,
                    Err(e) => return Err(e),
                },
                Err((target, e)) => {
                    self.retarget(p, target, None);
                    e
                }
            };
            if started.elapsed() >= self.retry_for {
                return Err(error);
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_millis(500));
        }
    }

    async fn request(&self, op: Op) -> Result<Vec<Event>, ClientError> {
        let op = self.keyed(op);
        let started = Instant::now();
        match route(&op, self.partitions) {
            Route::One(p) if p >= self.partitions => Err(ClientError::Protocol(format!(
                "partition {p}, but the cluster has {}",
                self.partitions
            ))),
            Route::One(p) => self.to_partition(p, &op, started).await,
            // Each partition in turn, every event kept: the last event is
            // the last partition's answer.
            Route::All => {
                let mut events = Vec::new();
                for p in 0..self.partitions {
                    events.extend(self.to_partition(p, &op, started).await?);
                }
                Ok(events)
            }
            // From the partition after the last one that had a job, until
            // one has a job; `empty` only once every partition said so.
            Route::Rotate => {
                let n = u64::from(self.partitions);
                let first = (self.last_lease.load(Ordering::Relaxed) + 1) % n;
                let mut events = Vec::new();
                for i in 0..n {
                    let p = ((first + i) % n) as u16;
                    events = self.to_partition(p, &op, started).await?;
                    if !matches!(events.last(), Some(Event::Empty { .. })) {
                        if matches!(op, Op::Lease { .. }) {
                            self.last_lease.store(u64::from(p), Ordering::Relaxed);
                        }
                        break;
                    }
                }
                Ok(events)
            }
        }
    }
}

impl Client {
    /// Send one op and return every event its command produced (D33),
    /// including expiry events of other jobs. A cluster client retries as
    /// the module says.
    pub async fn request(&self, op: Op) -> Result<Vec<Event>, ClientError> {
        match &self.mode {
            Mode::One(conn) => conn.request(0, op).await,
            Mode::Cluster(router) => router.request(op).await,
        }
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
        self.enqueue_ordered(queue, payload, delay, key, None).await
    }

    /// Enqueue with an ordering key: jobs of `queue` with the same `order`
    /// are leased one at a time, in enqueue order (D79). With consumer
    /// groups, the job of the first group is returned (D80).
    pub async fn enqueue_ordered(
        &self,
        queue: &QueueName,
        payload: Payload,
        delay: Millis,
        key: Option<DedupKey>,
        order: Option<OrderKey>,
    ) -> Result<Enqueued, ClientError> {
        // A cluster client keys every enqueue; only a caller's key can make
        // an enqueue a duplicate in the caller's eyes.
        let keyed = key.is_some();
        let op = Op::Enqueue {
            queue: queue.clone(),
            payload,
            delay,
            key,
            order,
        };
        // Expiry events of other jobs come first; then the op's own: one
        // `enqueued` per consumer group (D80), or one other event.
        let events = self.request(op).await?;
        let first = events.iter().find(|e| {
            matches!(
                e,
                Event::Enqueued { .. } | Event::Deduplicated { .. } | Event::Rejected { .. }
            )
        });
        match first.cloned() {
            Some(Event::Rejected { reason }) => Err(ClientError::Rejected(reason)),
            Some(Event::Enqueued { job, .. }) => Ok(Enqueued {
                job,
                deduplicated: false,
            }),
            Some(Event::Deduplicated { job, .. }) => Ok(Enqueued {
                job,
                deduplicated: keyed,
            }),
            Some(other) => Err(unexpected(other)),
            None => Err(ClientError::Protocol("no events for the op".into())),
        }
    }

    /// Give `queue` the consumer group `group` (D80), in every partition.
    pub async fn subscribe(&self, queue: &QueueName, group: &QueueName) -> Result<(), ClientError> {
        let op = Op::Subscribe {
            queue: queue.clone(),
            group: group.clone(),
        };
        match self.outcome(op).await? {
            Event::Subscribed { .. } => Ok(()),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cluster_client_keys_every_unkeyed_enqueue_once() {
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let c = Client::cluster(BTreeMap::from([(0, addr)]), 1, Duration::from_secs(1));
        let Mode::Cluster(router) = &c.mode else {
            panic!("a cluster client");
        };
        let enqueue = |key: Option<&str>| Op::Enqueue {
            queue: QueueName::new("q").unwrap(),
            payload: Payload(vec![]),
            delay: Millis(0),
            key: key.map(|k| DedupKey::new(k).unwrap()),
            order: None,
        };
        let key = |op: Op| match op {
            Op::Enqueue { key, .. } => key,
            other => panic!("{other:?}"),
        };
        let a = key(router.keyed(enqueue(None))).expect("a key");
        let b = key(router.keyed(enqueue(None))).expect("a key");
        assert_ne!(a, b, "every enqueue its own key");
        assert_eq!(
            key(router.keyed(enqueue(Some("mine")))),
            DedupKey::new("mine").ok()
        );
        assert_eq!(router.keyed(Op::Tick), Op::Tick);
        let other = Client::cluster(BTreeMap::from([(0, addr)]), 1, Duration::from_secs(1));
        let Mode::Cluster(other) = &other.mode else {
            panic!()
        };
        assert_ne!(
            key(other.keyed(enqueue(None))).unwrap(),
            a,
            "clients never share keys"
        );
    }
}
