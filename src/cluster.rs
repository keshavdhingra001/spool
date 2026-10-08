//! A replicated queue server (M7, D66–D73): the TCP front of the single-node
//! server (D30) in front of a [`Replica`], with peers over TCP.
//!
//! ```text
//! clients ──(op, reply)──┐
//!                        ├─> core channel ──> core thread: Replica (node, log, queue)
//! peers' connections ────┘   (Input)            drains requests into one entry (D66),
//!   (Raft messages in)                          heartbeats and elections on timers
//!                                                    │
//! dial tasks, one per peer  <──(Raft messages out)───┘
//! ```
//!
//! The core thread is the only owner of the replica, as in D30. Everything a
//! replica learns arrives on one channel: client requests and the Raft
//! messages its peers send on the connections they dialed. What it sends goes
//! out on connections it dialed itself, one task per peer, which redial when
//! a connection breaks. A full outgoing queue drops the message: Raft
//! resends what matters (D57, D60).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::codec;
use crate::protocol::{self, Reply, Request};
use crate::raft::{Id, Message};
use crate::replica::{self, Output, Replica, ReplicaError};
use crate::server::{Clock, Input, ServerError, Stats, Stopped, accept, system_clock};
use crate::storage::Storage;

#[derive(Clone)]
pub struct ClusterOptions {
    /// This replica's id: a key of `members`.
    pub id: Id,
    /// Every replica of the cluster, this one included, by id.
    pub members: BTreeMap<Id, SocketAddr>,
    pub clock: Clock,
    /// Run both checkers after every applied command (tests).
    pub check: bool,
    /// Requests in one entry at most (D30, D66)...
    pub max_batch: usize,
    /// ...and bytes of encoded ops, past the first (D66).
    pub max_batch_bytes: usize,
    pub core_queue: usize,
    pub in_flight: usize,
    /// D59: how often a leader heartbeats, and the election timeout range.
    pub heartbeat: Duration,
    pub election: (Duration, Duration),
}

impl ClusterOptions {
    pub fn new(id: Id, members: BTreeMap<Id, SocketAddr>) -> Self {
        ClusterOptions {
            id,
            members,
            clock: system_clock(),
            check: false,
            max_batch: 256,
            max_batch_bytes: 1 << 20,
            core_queue: 1024,
            in_flight: 128,
            heartbeat: Duration::from_millis(50),
            election: (Duration::from_millis(150), Duration::from_millis(300)),
        }
    }
}

/// Messages waiting for one peer's connection, at most.
const PEER_QUEUE: usize = 1024;
/// Wait between attempts to dial a peer.
const REDIAL: Duration = Duration::from_millis(100);

pub struct ClusterServer<S: Storage> {
    addr: SocketAddr,
    shutdown: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
    done: oneshot::Receiver<Stopped<S>>,
}

impl<S: Storage + Send + 'static> ClusterServer<S> {
    /// Recover the replica from `storage` (D67), start its core thread, dial
    /// the peers and serve on `listener`. Must run inside a tokio runtime.
    pub async fn start(
        storage: S,
        listener: TcpListener,
        options: ClusterOptions,
    ) -> Result<Self, ServerError> {
        let ids: Vec<Id> = options.members.keys().copied().collect();
        if !options.members.contains_key(&options.id) {
            return Err(ServerError::Invariant(format!(
                "replica {} is not one of the members {ids:?}",
                options.id
            )));
        }
        let (replica, _) = Replica::open(options.id, &ids, storage, options.check)?;
        let addr = listener.local_addr()?;
        let (shutdown, shutdown_rx) = watch::channel(false);
        let mut tasks = Vec::new();
        let mut peers = BTreeMap::new();
        for (&id, &peer) in &options.members {
            if id != options.id {
                let (tx, rx) = mpsc::channel(PEER_QUEUE);
                peers.insert(id, tx);
                tasks.push(tokio::spawn(dial(
                    options.id,
                    peer,
                    rx,
                    shutdown_rx.clone(),
                )));
            }
        }
        let (core_tx, core_rx) = mpsc::channel(options.core_queue);
        let (done_tx, done) = oneshot::channel();
        let core_options = options.clone();
        let core_shutdown = shutdown_rx.clone();
        std::thread::Builder::new()
            .name(format!("spool-core-{}", options.id))
            .spawn(move || {
                let stopped = core(replica, core_rx, peers, core_shutdown, &core_options);
                let _ = done_tx.send(stopped);
            })?;
        tasks.push(tokio::spawn(accept(
            listener,
            core_tx,
            options.in_flight,
            true,
            shutdown_rx,
        )));
        Ok(ClusterServer {
            addr,
            shutdown,
            tasks,
            done,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Wait until the core thread stops by itself: a log error or (with
    /// `check`) a broken invariant (D38).
    pub async fn stopped(&mut self) -> Stopped<S> {
        (&mut self.done)
            .await
            .expect("core thread reports how it ended")
    }

    /// Stop serving and dialing, and wait for the core thread to finish.
    pub async fn shutdown(self) -> Stopped<S> {
        let _ = self.shutdown.send(true);
        for task in self.tasks {
            let _ = task.await;
        }
        self.done.await.expect("core thread reports how it ended")
    }
}

/// A reply channel: where the core thread answers a request.
type Tag = oneshot::Sender<Reply>;

/// The core thread: owns the replica, batches requests into entries, runs
/// the timers, until shutdown or until the replica fails. Requests still
/// waiting for their entry are dropped with the replica, which closes their
/// connections without an answer: their clients cannot know the outcome
/// (D70). Unlike the single node's core (D30), this one cannot wait for every
/// pending reply, since without a quorum some never come.
fn core<S: Storage>(
    mut replica: Replica<S, Tag>,
    mut rx: mpsc::Receiver<Input>,
    peers: BTreeMap<Id, mpsc::Sender<Message>>,
    shutdown: watch::Receiver<bool>,
    options: &ClusterOptions,
) -> Stopped<S> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("a timer runtime");
    let mut timers = Timers::new(options);
    let mut stats = Stats::default();
    let mut scratch = Vec::new();
    let result = (|| -> Result<(), ServerError> {
        let out = replica.start().map_err(failed)?;
        timers.handle(out, &peers);
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let deadline = timers.heartbeat_at.min(timers.election_at);
            let first =
                rt.block_on(async { tokio::time::timeout_at(deadline.into(), rx.recv()).await });
            match first {
                Ok(None) => return Ok(()),
                Ok(Some(input)) => {
                    let mut requests = Vec::new();
                    let mut bytes = 0;
                    let mut next = Some(input);
                    while let Some(input) = next.take().or_else(|| rx.try_recv().ok()) {
                        match input {
                            Input::Raft(from, msg) => {
                                let out = replica.receive(from, msg).map_err(failed)?;
                                timers.handle(out, &peers);
                            }
                            Input::Request(job) => {
                                scratch.clear();
                                codec::encode_op(&job.op, &mut scratch);
                                bytes += scratch.len();
                                requests.push((job.reply, job.op));
                                if requests.len() >= options.max_batch
                                    || bytes >= options.max_batch_bytes
                                {
                                    break;
                                }
                            }
                        }
                    }
                    if !requests.is_empty() {
                        stats.batches += 1;
                        stats.commands += requests.len() as u64;
                        stats.largest_batch = stats.largest_batch.max(requests.len());
                        // One clock read per entry (D31, D66).
                        let at = (options.clock)();
                        let out = replica.propose(at, requests).map_err(failed)?;
                        timers.handle(out, &peers);
                    }
                }
                Err(_) => {}
            }
            let now = Instant::now();
            if now >= timers.heartbeat_at {
                timers.heartbeat_at = now + options.heartbeat;
                let out = replica.heartbeat().map_err(failed)?;
                timers.handle(out, &peers);
            }
            if now >= timers.election_at {
                let out = replica.election_timeout().map_err(failed)?;
                timers.handle(out, &peers);
            }
        }
    })();
    drop(rx);
    if options.check {
        stats.checked = stats.batches;
    }
    Stopped {
        storage: replica.into_storage(),
        stats,
        result,
    }
}

fn failed(e: ReplicaError) -> ServerError {
    match e {
        ReplicaError::Store(e) => ServerError::Store(e),
        other => ServerError::Invariant(other.to_string()),
    }
}

/// The core thread's timers and the generator for election timeouts: the
/// driver's randomness (D59), never the replica's.
struct Timers {
    heartbeat_at: Instant,
    election_at: Instant,
    election: (Duration, Duration),
    rng: u64,
}

impl Timers {
    fn new(options: &ClusterOptions) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64);
        let now = Instant::now();
        Timers {
            heartbeat_at: now + options.heartbeat,
            election_at: now + options.election.1,
            election: options.election,
            rng: (nanos ^ (u64::from(options.id) << 32)) | 1,
        }
    }

    /// Send, answer, and restart the election timer if the replica asks.
    fn handle(&mut self, out: Output<Tag>, peers: &BTreeMap<Id, mpsc::Sender<Message>>) {
        for (to, msg) in out.send {
            if let Some(peer) = peers.get(&to) {
                // Full or closed: the message is lost, as on a network.
                let _ = peer.try_send(msg);
            }
        }
        for (reply, r) in out.replies {
            let wire = match r {
                replica::Reply::Events(events) => Reply::Events(events),
                replica::Reply::NotLeader(leader) => Reply::NotLeader { leader },
                replica::Reply::Unknown => Reply::Unknown,
            };
            // The client may have gone.
            let _ = reply.send(wire);
        }
        if out.reset_election_timer {
            // xorshift64: uniform enough to break split votes.
            self.rng ^= self.rng << 13;
            self.rng ^= self.rng >> 7;
            self.rng ^= self.rng << 17;
            let (lo, hi) = self.election;
            let span = (hi - lo).as_millis().max(1) as u64;
            self.election_at = Instant::now() + lo + Duration::from_millis(self.rng % span);
        }
    }
}

/// Keep a connection to one peer and write the core thread's messages to it,
/// redialing whenever it breaks, until shutdown.
async fn dial(
    me: Id,
    addr: SocketAddr,
    mut rx: mpsc::Receiver<Message>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut buf = Vec::new();
    loop {
        let stream = tokio::select! {
            _ = shutdown.changed() => return,
            s = TcpStream::connect(addr) => s,
        };
        let Ok(mut stream) = stream else {
            tokio::select! {
                _ = shutdown.changed() => return,
                _ = tokio::time::sleep(REDIAL) => continue,
            }
        };
        let _ = stream.set_nodelay(true);
        buf.clear();
        let hello = Request::Peer {
            version: protocol::VERSION,
            from: me,
        };
        protocol::encode_request(0, &hello, &mut buf);
        if protocol::write_all(&mut stream, &buf).await.is_err() {
            continue;
        }
        loop {
            let msg = tokio::select! {
                _ = shutdown.changed() => return,
                m = rx.recv() => m,
            };
            let Some(msg) = msg else { return };
            buf.clear();
            let mut next = Some(msg);
            // Everything already queued goes out in one write.
            while let Some(m) = next.take().or_else(|| rx.try_recv().ok()) {
                protocol::encode_request(0, &Request::Raft(m), &mut buf);
            }
            if protocol::write_all(&mut stream, &buf).await.is_err() {
                break;
            }
        }
    }
}
