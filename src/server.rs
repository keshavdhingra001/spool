//! The TCP server (D30): tokio tasks own the sockets, one OS thread owns the
//! durable queue.
//!
//! ```text
//! connection reader ──(op, oneshot)──> core channel (1024) ──> core thread
//!        │                                                     drains up to 256,
//!        └──(oneshot, in request order)──> connection writer   stamps the time once,
//!                                          awaits each reply   apply_batch: one sync
//! ```
//!
//! Each connection has a reader task and a writer task joined by a channel of
//! pending replies, in request order (D32). That channel holds at most 128
//! entries, so a client with that many requests unanswered stops being read
//! (D37), as does every client while the core channel is full.

use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::check::agree;
use crate::command::{Command, Event, Op};
use crate::durable::{Durable, Options, Recovery};
use crate::error::StoreError;
use crate::ledger::Ledger;
use crate::protocol::{self, ErrorCode, FrameError, Reply, Request};
use crate::raft;
use crate::storage::{FileStorage, Storage};
use crate::types::Time;

/// Where command times come from (D31).
pub type Clock = Arc<dyn Fn() -> Time + Send + Sync>;

/// Wall-clock milliseconds since the Unix epoch: the only clock read in spool
/// (D31). A clock set before 1970 reads as 0, and D9 clamps any step back.
pub fn system_clock() -> Clock {
    Arc::new(|| {
        let ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis());
        Time(u64::try_from(ms).unwrap_or(u64::MAX))
    })
}

/// A clock the test moves by hand.
#[derive(Clone, Debug, Default)]
pub struct ManualClock(Arc<AtomicU64>);

impl ManualClock {
    pub fn new(at: u64) -> Self {
        ManualClock(Arc::new(AtomicU64::new(at)))
    }

    pub fn set(&self, at: u64) {
        self.0.store(at, Ordering::SeqCst);
    }

    pub fn advance(&self, ms: u64) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }

    pub fn now(&self) -> Time {
        Time(self.0.load(Ordering::SeqCst))
    }

    pub fn clock(&self) -> Clock {
        let me = self.clone();
        Arc::new(move || me.now())
    }
}

#[derive(Clone)]
pub struct ServerOptions {
    pub durable: Options,
    pub clock: Clock,
    /// Run the invariant checker and the event ledger after every batch (D39).
    /// Linear in the number of jobs per batch: for tests.
    pub check: bool,
    /// Requests applied with one sync, at most (D30).
    pub max_batch: usize,
    /// Requests waiting for the core thread, across all connections (D37).
    pub core_queue: usize,
    /// Requests sent and not yet answered, per connection (D37).
    pub in_flight: usize,
}

impl Default for ServerOptions {
    fn default() -> Self {
        ServerOptions {
            durable: Options::default(),
            clock: system_clock(),
            check: false,
            max_batch: 256,
            core_queue: 1024,
            in_flight: 128,
        }
    }
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("network: {0}")]
    Io(#[from] io::Error),
    #[error("invariant violated: {0}")]
    Invariant(String),
}

/// What the core thread did, for tests and the group-commit numbers (D30).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub batches: u64,
    pub commands: u64,
    pub largest_batch: usize,
    /// Batches after which both checkers ran (all of them with `check`).
    pub checked: u64,
}

/// How the core thread ended: the storage it owned (so a test can restart on
/// it), what it did, and the error that stopped it, if any (D38).
pub struct Stopped<S> {
    pub storage: S,
    pub stats: Stats,
    pub result: Result<(), ServerError>,
}

/// One request on its way to the core thread.
pub(crate) struct Job {
    pub(crate) op: Op,
    pub(crate) reply: oneshot::Sender<Reply>,
}

/// What reaches a core thread: a client's request, or (in a cluster, M7) a
/// message from another replica.
pub(crate) enum Input {
    Request(Job),
    Raft(raft::Id, raft::Message),
}

pub struct Server<S: Storage = FileStorage> {
    addr: SocketAddr,
    shutdown: watch::Sender<bool>,
    accept: JoinHandle<()>,
    done: oneshot::Receiver<Stopped<S>>,
}

impl Server<FileStorage> {
    /// Recover the data directory `dir` and serve it on `listen`.
    pub async fn start_dir(
        dir: &Path,
        listen: SocketAddr,
        options: ServerOptions,
    ) -> Result<Self, ServerError> {
        Self::start(FileStorage::open(dir)?, listen, options).await
    }
}

impl<S: Storage + Send + 'static> Server<S> {
    /// Recover the queue from `storage` (D29), start the core thread and
    /// accept connections on `listen`. Must run inside a tokio runtime.
    pub async fn start(
        storage: S,
        listen: SocketAddr,
        options: ServerOptions,
    ) -> Result<Self, ServerError> {
        let (core_state, _) = Core::open(storage, options.durable, options.check)?;
        let listener = TcpListener::bind(listen).await?;
        let addr = listener.local_addr()?;
        let (core_tx, core_rx) = mpsc::channel(options.core_queue);
        let (done_tx, done) = oneshot::channel();
        let core_options = options.clone();
        std::thread::Builder::new()
            .name("spool-core".into())
            .spawn(move || {
                let _ = done_tx.send(core(core_state, core_rx, &core_options));
            })?;
        let (shutdown, shutdown_rx) = watch::channel(false);
        let accept = tokio::spawn(accept(
            listener,
            core_tx,
            options.in_flight,
            false,
            shutdown_rx,
        ));
        Ok(Server {
            addr,
            shutdown,
            accept,
            done,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Wait until the core thread stops by itself, which only a storage error
    /// or (with `check`) a broken invariant makes it do (D38).
    pub async fn stopped(&mut self) -> Stopped<S> {
        (&mut self.done)
            .await
            .expect("core thread reports how it ended")
    }

    /// Stop accepting, close every connection once its pending replies are
    /// written, and wait for the core thread to finish.
    pub async fn shutdown(self) -> Stopped<S> {
        let _ = self.shutdown.send(true);
        let _ = self.accept.await;
        self.done.await.expect("core thread reports how it ended")
    }
}

/// The server's batch logic (D30): apply a batch of ops at one time with one
/// sync, then (with `check`) run both checkers (D39). The core thread and the
/// simulated server (D47) both drive it.
pub struct Core<S: Storage> {
    durable: Durable<S>,
    /// Present when checking: resumed from the recovered state, not from nothing.
    ledger: Option<Ledger>,
    stats: Stats,
    cmds: Vec<Command>,
}

impl<S: Storage> Core<S> {
    /// Recover the queue from `storage` (D29).
    pub fn open(storage: S, options: Options, check: bool) -> Result<(Self, Recovery), StoreError> {
        let (durable, recovery) = Durable::open(storage, options)?;
        let ledger = check.then(|| Ledger::resume(durable.queue()));
        let core = Core {
            durable,
            ledger,
            stats: Stats::default(),
            cmds: Vec::new(),
        };
        Ok((core, recovery))
    }

    /// Apply `ops` as one batch, every command at time `at` (D31), and return
    /// each one's events once all of them are durable.
    pub fn apply(
        &mut self,
        at: Time,
        ops: impl IntoIterator<Item = Op>,
    ) -> Result<Vec<Vec<Event>>, ServerError> {
        self.cmds.clear();
        self.cmds
            .extend(ops.into_iter().map(|op| Command { at, op }));
        let events = self.durable.apply_batch(&self.cmds)?;
        let n = self.cmds.len();
        self.stats.batches += 1;
        self.stats.commands += n as u64;
        self.stats.largest_batch = self.stats.largest_batch.max(n);
        if let Some(ledger) = &mut self.ledger {
            check(&self.durable, ledger, &events).map_err(ServerError::Invariant)?;
            self.stats.checked += 1;
        }
        Ok(events)
    }

    pub fn durable(&self) -> &Durable<S> {
        &self.durable
    }

    pub fn stats(&self) -> Stats {
        self.stats
    }

    pub fn into_storage(self) -> S {
        self.durable.into_storage()
    }
}

/// The core thread (D30): apply batches until every sender is gone or the
/// queue fails. Dropping the receiver and the pending reply senders on the way
/// out is what tells the connections to close.
fn core<S: Storage>(
    mut core: Core<S>,
    mut rx: mpsc::Receiver<Input>,
    options: &ServerOptions,
) -> Stopped<S> {
    let mut batch = Vec::with_capacity(options.max_batch);
    let result = loop {
        // A single node accepts no peers, so only requests arrive.
        let Some(Input::Request(first)) = rx.blocking_recv() else {
            break Ok(());
        };
        batch.push(first);
        while batch.len() < options.max_batch {
            match rx.try_recv() {
                Ok(Input::Request(job)) => batch.push(job),
                _ => break,
            }
        }
        // One clock read per batch (D31): every command in it runs at one time.
        let at = (options.clock)();
        let events = match core.apply(at, batch.iter().map(|j: &Job| j.op.clone())) {
            Ok(events) => events,
            Err(e) => break Err(e),
        };
        for (job, events) in batch.drain(..).zip(events) {
            // The client may have gone; its reply has nowhere to go.
            let _ = job.reply.send(Reply::Events(events));
        }
    };
    drop(rx);
    let stats = core.stats();
    Stopped {
        storage: core.into_storage(),
        stats,
        result,
    }
}

fn check<S: Storage>(
    durable: &Durable<S>,
    ledger: &mut Ledger,
    events: &[Vec<Event>],
) -> Result<(), String> {
    durable.queue().check_invariants()?;
    for e in events {
        ledger.observe(e)?;
    }
    agree(durable.queue().counts(), ledger.counts())
}

/// Accept connections until shutdown. With `cluster`, a connection may also
/// be a peer replica's (M7).
pub(crate) async fn accept(
    listener: TcpListener,
    core: mpsc::Sender<Input>,
    in_flight: usize,
    cluster: bool,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let _ = stream.set_nodelay(true);
                    connections.spawn(connection(
                        stream,
                        core.clone(),
                        in_flight,
                        cluster,
                        shutdown.clone(),
                    ));
                }
                // Out of file descriptors and the like: keep serving the
                // connections that exist.
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
            },
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
    drop(core);
    while connections.join_next().await.is_some() {}
}

/// A reply the writer sends: ready now (handshake, errors) or once the core
/// thread has applied the request.
enum Pending {
    Now(u64, Reply),
    Later(u64, oneshot::Receiver<Reply>),
}

/// How a connection introduced itself.
enum Greeted {
    No,
    Client,
    Peer(raft::Id),
}

async fn connection(
    stream: TcpStream,
    core: mpsc::Sender<Input>,
    in_flight: usize,
    cluster: bool,
    mut shutdown: watch::Receiver<bool>,
) {
    let (mut rd, mut wr) = stream.into_split();
    let (pending, mut replies) = mpsc::channel::<Pending>(in_flight);
    let writer = tokio::spawn(async move {
        let mut buf = Vec::new();
        while let Some(p) = replies.recv().await {
            let (id, reply) = match p {
                Pending::Now(id, reply) => (id, reply),
                Pending::Later(id, rx) => match rx.await {
                    Ok(reply) => (id, reply),
                    // The core thread stopped (D38): close without a reply.
                    Err(_) => break,
                },
            };
            buf.clear();
            protocol::encode_reply(id, &reply, &mut buf);
            // After an error reply the reader stops and drops `pending`, so
            // this loop ends and the socket closes once the error is written.
            if protocol::write_all(&mut wr, &buf).await.is_err() {
                break;
            }
        }
    });
    let error = |id, code, message: String| Pending::Now(id, Reply::Error { code, message });

    // The handshake: the first frame must be a hello for a version we speak,
    // or, in a cluster, a peer's introduction.
    let first = tokio::select! {
        _ = shutdown.changed() => None,
        f = protocol::read_frame(&mut rd) => Some(f),
    };
    let greeted = match first {
        Some(Ok(Some(frame))) => match frame.request() {
            Ok(Request::Hello { version }) if protocol::speaks(version) => {
                let hello_ok = Pending::Now(frame.id, Reply::HelloOk { version });
                if pending.send(hello_ok).await.is_ok() {
                    Greeted::Client
                } else {
                    Greeted::No
                }
            }
            Ok(Request::Peer { version, from }) if cluster && protocol::speaks(version) => {
                Greeted::Peer(from)
            }
            Ok(Request::Hello { version }) => {
                let message = format!(
                    "protocol version {version}, server speaks 1 to {}",
                    protocol::VERSION
                );
                let _ = pending
                    .send(error(frame.id, ErrorCode::UnsupportedVersion, message))
                    .await;
                Greeted::No
            }
            Ok(Request::Peer { version, .. }) if cluster => {
                let message = format!("peer protocol version {version}");
                let _ = pending
                    .send(error(frame.id, ErrorCode::UnsupportedVersion, message))
                    .await;
                Greeted::No
            }
            Ok(_) => {
                let message = "the first frame must be hello".to_string();
                let _ = pending
                    .send(error(frame.id, ErrorCode::Protocol, message))
                    .await;
                Greeted::No
            }
            Err(e) => {
                let _ = pending
                    .send(error(frame.id, ErrorCode::Protocol, e.to_string()))
                    .await;
                Greeted::No
            }
        },
        Some(Err(e)) => {
            refuse(&pending, e).await;
            Greeted::No
        }
        _ => Greeted::No,
    };

    match greeted {
        Greeted::Client => requests(&mut rd, &pending, &core, &mut shutdown).await,
        Greeted::Peer(from) => peer(&mut rd, from, &core, &mut shutdown).await,
        Greeted::No => {}
    }
    drop(core);
    drop(pending);
    let _ = writer.await;
}

/// Read requests after the handshake and hand each to the core thread, until
/// the client closes, breaks the protocol, or the server shuts down.
async fn requests(
    rd: &mut tokio::net::tcp::OwnedReadHalf,
    pending: &mpsc::Sender<Pending>,
    core: &mpsc::Sender<Input>,
    shutdown: &mut watch::Receiver<bool>,
) {
    let error = |id, code, message: String| Pending::Now(id, Reply::Error { code, message });
    loop {
        let frame = tokio::select! {
            _ = shutdown.changed() => break,
            f = protocol::read_frame(rd) => f,
        };
        let frame = match frame {
            Ok(Some(frame)) => frame,
            Ok(None) => break,
            Err(e) => {
                refuse(pending, e).await;
                break;
            }
        };
        let op = match frame.request() {
            Ok(Request::Op(op)) => op,
            Ok(_) => {
                let message = "only requests after the hello".to_string();
                let _ = pending
                    .send(error(frame.id, ErrorCode::Protocol, message))
                    .await;
                break;
            }
            Err(e) => {
                let _ = pending
                    .send(error(frame.id, ErrorCode::Protocol, e.to_string()))
                    .await;
                break;
            }
        };
        // Take an in-flight slot first, then a place in the core channel:
        // either wait is the backpressure of D37.
        let Ok(slot) = pending.reserve().await else {
            break;
        };
        let (reply, rx) = oneshot::channel();
        if core.send(Input::Request(Job { op, reply })).await.is_err() {
            break; // The core thread stopped (D38).
        }
        slot.send(Pending::Later(frame.id, rx));
    }
}

/// Read Raft messages from a peer that dialed us and hand them to the core
/// thread. Nothing is answered on this connection: replies go out on the one
/// this replica dialed. Anything unexpected closes it; the peer redials.
async fn peer(
    rd: &mut tokio::net::tcp::OwnedReadHalf,
    from: raft::Id,
    core: &mpsc::Sender<Input>,
    shutdown: &mut watch::Receiver<bool>,
) {
    loop {
        let frame = tokio::select! {
            _ = shutdown.changed() => break,
            f = protocol::read_frame_max(rd, protocol::MAX_PEER_FRAME) => f,
        };
        let Ok(Some(frame)) = frame else { break };
        let Ok(Request::Raft(msg)) = frame.request() else {
            break;
        };
        if core.send(Input::Raft(from, msg)).await.is_err() {
            break;
        }
    }
}

/// Answer a frame that could not be read with an error, unless the connection
/// itself failed.
async fn refuse(pending: &mpsc::Sender<Pending>, e: FrameError) {
    if !matches!(e, FrameError::Io(_)) {
        let reply = Reply::Error {
            code: ErrorCode::Protocol,
            message: e.to_string(),
        };
        let _ = pending.send(Pending::Now(0, reply)).await;
    }
}
