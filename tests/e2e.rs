//! End to end (D39): the client and worker library against an in-process
//! server that runs both checkers after every batch, so every test here is
//! also an invariant test. The server's clock is moved by hand.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use spool::client::{Client, ClientError};
use spool::server::{ManualClock, Server, ServerOptions, Stopped};
use spool::storage::MemStorage;
use spool::worker::{Outcome, Worker, WorkerOptions};
use spool::{
    DedupKey, Durable, JobId, Millis, Op, Options, Payload, QueueConfig, QueueName, RejectReason,
};
use tokio::sync::Notify;

fn any_port() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

fn options(clock: &ManualClock) -> ServerOptions {
    ServerOptions {
        clock: clock.clock(),
        check: true,
        ..ServerOptions::default()
    }
}

async fn start(clock: &ManualClock) -> Server<MemStorage> {
    Server::start(MemStorage::new(), any_port(), options(clock))
        .await
        .unwrap()
}

fn q(name: &str) -> QueueName {
    QueueName::new(name).unwrap()
}

fn key(k: &str) -> Option<DedupKey> {
    Some(DedupKey::new(k).unwrap())
}

fn p(s: &str) -> Payload {
    Payload(s.as_bytes().to_vec())
}

/// Stop the server, require that it never failed, and return its final state.
async fn finish(server: Server<MemStorage>) -> spool::ReferenceQueue {
    let Stopped {
        storage, result, ..
    } = server.shutdown().await;
    result.unwrap();
    let (d, _) = Durable::<MemStorage>::open(storage, Options::default()).unwrap();
    d.queue().clone()
}

#[tokio::test]
async fn client_calls() {
    let clock = ManualClock::new(1_000);
    let server = start(&clock).await;
    let c = Client::connect(server.local_addr()).await.unwrap();
    let queue = q("emails");

    let first = c
        .enqueue(&queue, p("a"), Millis(0), key("k"))
        .await
        .unwrap();
    assert_eq!((first.job, first.deduplicated), (JobId(1), false));
    let again = c
        .enqueue(&queue, p("other"), Millis(0), key("k"))
        .await
        .unwrap();
    assert_eq!((again.job, again.deduplicated), (JobId(1), true));
    c.enqueue(&queue, p("b"), Millis(500), None).await.unwrap();

    let job = c.lease(&queue, Millis(1_000)).await.unwrap().unwrap();
    assert_eq!(
        (job.lease.job, job.attempt, job.payload),
        (JobId(1), 1, p("a"))
    );
    assert_eq!(job.lease.deadline.0, 2_000);
    assert_eq!(
        c.lease(&queue, Millis(1_000)).await.unwrap(),
        None,
        "b is delayed"
    );
    clock.advance(500);
    let renewed = c
        .heartbeat(JobId(1), job.lease.token, Millis(5_000))
        .await
        .unwrap();
    assert_eq!(renewed.deadline.0, 6_500);

    let stale = c.ack(JobId(1), spool::Token(99)).await;
    assert!(matches!(
        stale,
        Err(ClientError::Rejected(RejectReason::StaleToken))
    ));
    let zero = c.lease(&queue, Millis(0)).await;
    assert!(matches!(
        zero,
        Err(ClientError::Rejected(RejectReason::ZeroVisibility))
    ));
    c.ack(JobId(1), job.lease.token).await.unwrap();

    let b = c.lease(&queue, Millis(1_000)).await.unwrap().unwrap();
    c.nack(b.lease.job, b.lease.token).await.unwrap();
    let gone = c.nack(b.lease.job, b.lease.token).await;
    assert!(matches!(
        gone,
        Err(ClientError::Rejected(RejectReason::NotLeased))
    ));

    drop(c);
    let state = finish(server).await;
    assert_eq!((state.counts().acked, state.counts().waiting), (1, 1));
}

#[tokio::test]
async fn one_client_pipelines_from_many_tasks() {
    let clock = ManualClock::new(0);
    let server = start(&clock).await;
    let c = Client::connect(server.local_addr()).await.unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..500 {
        let c = c.clone();
        tasks.spawn(async move {
            c.enqueue(&q("a"), p(&format!("{i}")), Millis(0), None)
                .await
                .unwrap()
                .job
        });
    }
    let mut ids: Vec<u64> = tasks.join_all().await.into_iter().map(|j| j.0).collect();
    ids.sort();
    assert_eq!(ids, (1..=500).collect::<Vec<_>>());
    drop(c);
    let stopped = server.shutdown().await;
    stopped.result.unwrap();
    // With 500 requests in flight at once, the core thread found more than
    // one waiting at least once: group commit (D30).
    assert!(stopped.stats.largest_batch > 1, "{:?}", stopped.stats);
    assert!(stopped.stats.batches < 500, "{:?}", stopped.stats);
}

#[tokio::test]
async fn backpressure_does_not_deadlock() {
    let clock = ManualClock::new(0);
    let server = Server::start(
        MemStorage::new(),
        any_port(),
        ServerOptions {
            in_flight: 2,
            core_queue: 3,
            // Below what the channel can hand over at once (1 + 3), so the
            // cap itself is what keeps batches small.
            max_batch: 2,
            ..options(&clock)
        },
    )
    .await
    .unwrap();
    // Eight connections, so more requests wait for the core than one
    // connection's in-flight cap allows.
    let mut clients = Vec::new();
    for _ in 0..8 {
        clients.push(Client::connect(server.local_addr()).await.unwrap());
    }
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..2_000 {
        let c = clients[i % clients.len()].clone();
        // Large payloads, so socket buffers fill on both sides as well.
        tasks.spawn(async move {
            c.enqueue(&q("a"), Payload(vec![i as u8; 4_000]), Millis(0), None)
                .await
                .unwrap()
        });
    }
    let done = tokio::time::timeout(Duration::from_secs(60), tasks.join_all()).await;
    assert_eq!(done.expect("no deadlock").len(), 2_000);
    drop(clients);
    let stopped = server.shutdown().await;
    stopped.result.unwrap();
    assert!(stopped.stats.largest_batch <= 2, "{:?}", stopped.stats);
}

#[tokio::test]
async fn lease_expiry_follows_the_server_clock() {
    let clock = ManualClock::new(0);
    let server = start(&clock).await;
    let a = Client::connect(server.local_addr()).await.unwrap();
    let b = Client::connect(server.local_addr()).await.unwrap();
    a.enqueue(&q("a"), p("x"), Millis(0), None).await.unwrap();
    let first = a.lease(&q("a"), Millis(1_000)).await.unwrap().unwrap();
    clock.advance(999);
    assert_eq!(b.lease(&q("a"), Millis(1_000)).await.unwrap(), None);
    clock.advance(1);
    // The default backoff after attempt 1 is 500..=1000 ms.
    clock.advance(1_000);
    let second = b.lease(&q("a"), Millis(1_000)).await.unwrap().unwrap();
    assert_eq!((second.lease.job, second.attempt), (first.lease.job, 2));
    assert!(second.lease.token > first.lease.token);
    let zombie = a.ack(first.lease.job, first.lease.token).await;
    assert!(matches!(
        zombie,
        Err(ClientError::Rejected(RejectReason::StaleToken))
    ));
    b.ack(second.lease.job, second.lease.token).await.unwrap();
    drop((a, b));
    assert_eq!(finish(server).await.counts().acked, 1);
}

#[tokio::test]
async fn concurrent_workers_process_every_job_once() {
    let clock = ManualClock::new(0);
    let server = start(&clock).await;
    let admin = Client::connect(server.local_addr()).await.unwrap();
    // Retry at once, so nacked jobs come back without moving the clock.
    let configure = Op::Configure {
        queue: q("work"),
        config: QueueConfig {
            max_attempts: 5,
            backoff_base: Millis(0),
            backoff_cap: Millis(0),
        },
    };
    admin.request(configure).await.unwrap();
    const JOBS: usize = 300;
    for i in 0..JOBS {
        admin
            .enqueue(&q("work"), p(&format!("{i}")), Millis(0), None)
            .await
            .unwrap();
    }
    // payload -> (attempts seen, successes)
    let seen: Arc<Mutex<BTreeMap<String, (u32, u32)>>> = Arc::default();
    let all_done = Arc::new(Notify::new());
    let mut workers = tokio::task::JoinSet::new();
    for w in 0..6 {
        let client = if w % 2 == 0 {
            admin.clone()
        } else {
            Client::connect(server.local_addr()).await.unwrap()
        };
        let (seen, all_done) = (seen.clone(), all_done.clone());
        workers.spawn(async move {
            let mut worker = Worker::new(client, q("work"), WorkerOptions::default());
            worker
                .run(|job| {
                    let (seen, all_done) = (seen.clone(), all_done.clone());
                    async move {
                        let name = String::from_utf8(job.payload.0).unwrap();
                        // Every seventh job fails its first attempt.
                        let fail = name.parse::<usize>().unwrap() % 7 == 0 && job.attempt == 1;
                        tokio::task::yield_now().await;
                        let mut s = seen.lock().unwrap();
                        let e = s.entry(name).or_default();
                        e.0 += 1;
                        if fail {
                            return Err("first attempt fails");
                        }
                        e.1 += 1;
                        if s.values().filter(|e| e.1 > 0).count() == JOBS {
                            all_done.notify_one();
                        }
                        Ok(())
                    }
                })
                .await
        });
    }
    tokio::time::timeout(Duration::from_secs(30), all_done.notified())
        .await
        .expect("every job processed");
    // Let the last acks land, then stop the workers.
    tokio::time::sleep(Duration::from_millis(50)).await;
    workers.abort_all();
    while workers.join_next().await.is_some() {}
    drop(admin);

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), JOBS);
    for (name, (attempts, successes)) in &seen {
        let expected = if name.parse::<usize>().unwrap() % 7 == 0 {
            2
        } else {
            1
        };
        assert_eq!((*attempts, *successes), (expected, 1), "job {name}");
    }
    // Leases never expired (the clock did not move), so no job was leased
    // twice at once; the server's checkers ran after every batch.
    let state = finish(server).await;
    let c = state.counts();
    assert_eq!(
        (c.acked, c.waiting, c.leased, c.dead),
        (JOBS as u64, 0, 0, 0)
    );
}

#[tokio::test]
async fn a_lost_lease_stops_the_handler() {
    let clock = ManualClock::new(0);
    let server = start(&clock).await;
    let c = Client::connect(server.local_addr()).await.unwrap();
    let other = Client::connect(server.local_addr()).await.unwrap();
    c.enqueue(&q("a"), p("slow"), Millis(0), None)
        .await
        .unwrap();
    // Retry at once, so the other worker can lease the job right away.
    other
        .request(Op::Configure {
            queue: q("a"),
            config: QueueConfig {
                max_attempts: 5,
                backoff_base: Millis(0),
                backoff_cap: Millis(0),
            },
        })
        .await
        .unwrap();

    struct Dropped(Arc<Mutex<bool>>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            *self.0.lock().unwrap() = true;
        }
    }
    let dropped = Arc::new(Mutex::new(false));
    let started = Arc::new(Notify::new());
    let mut worker = Worker::new(
        c,
        q("a"),
        WorkerOptions {
            visibility: Millis(300),
            ..WorkerOptions::default()
        },
    );
    let (d, s) = (dropped.clone(), started.clone());
    let mut handler = move |_job| {
        let guard = Dropped(d.clone());
        let s = s.clone();
        async move {
            let _guard = guard;
            s.notify_one();
            // A handler that would run forever: it stalls like a zombie.
            std::future::pending::<()>().await;
            Ok::<(), ()>(())
        }
    };
    let step = tokio::spawn(async move { worker.step(&mut handler).await });
    started.notified().await;
    // The lease runs out, and another worker takes the job (token 2).
    clock.advance(301);
    let taken = other.lease(&q("a"), Millis(10_000)).await.unwrap().unwrap();
    assert_eq!((taken.lease.job, taken.attempt), (JobId(1), 2));
    // The zombie's next heartbeat (every 100 ms of real time) is refused.
    let outcome = tokio::time::timeout(Duration::from_secs(5), step)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(outcome, Outcome::Lost(JobId(1), RejectReason::StaleToken));
    assert!(*dropped.lock().unwrap(), "the handler's future was dropped");
    other.ack(JobId(1), taken.lease.token).await.unwrap();
    drop(other);
    assert_eq!(finish(server).await.counts().acked, 1);
}

#[tokio::test]
async fn dedup_survives_a_restart_on_disk() {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("e2e_dedup_restart");
    let _ = std::fs::remove_dir_all(&dir);
    let clock = ManualClock::new(1_000);
    let server = Server::start_dir(&dir, any_port(), options(&clock))
        .await
        .unwrap();
    let c = Client::connect(server.local_addr()).await.unwrap();
    let first = c
        .enqueue(&q("orders"), p("charge"), Millis(0), key("order-9"))
        .await
        .unwrap();
    drop(c);
    server.shutdown().await.result.unwrap();

    // A new process on the same directory: the key is in the recovered state.
    clock.advance(60_000);
    let server = Server::start_dir(&dir, any_port(), options(&clock))
        .await
        .unwrap();
    let c = Client::connect(server.local_addr()).await.unwrap();
    let retry = c
        .enqueue(&q("orders"), p("charge"), Millis(0), key("order-9"))
        .await
        .unwrap();
    assert_eq!((retry.job, retry.deduplicated), (first.job, true));
    // After the window, the key makes a new job.
    clock.advance(300_000);
    let later = c
        .enqueue(&q("orders"), p("charge"), Millis(0), key("order-9"))
        .await
        .unwrap();
    assert_eq!((later.job, later.deduplicated), (JobId(2), false));
    drop(c);
    server.shutdown().await.result.unwrap();
}

#[tokio::test]
async fn calls_fail_once_the_server_is_gone() {
    let clock = ManualClock::new(0);
    let server = start(&clock).await;
    let c = Client::connect(server.local_addr()).await.unwrap();
    c.enqueue(&q("a"), p("x"), Millis(0), None).await.unwrap();
    server.shutdown().await.result.unwrap();
    let err = c.enqueue(&q("a"), p("y"), Millis(0), None).await;
    assert!(matches!(err, Err(ClientError::Closed)), "{err:?}");
}

#[tokio::test]
async fn a_reply_with_the_wrong_id_is_a_protocol_error() {
    use spool::protocol::{self, Reply};
    // A broken server: answers the hello, then replies to request 1 as if it
    // were request 2 (D32).
    let listener = tokio::net::TcpListener::bind(any_port()).await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let hello = protocol::read_frame(&mut s).await.unwrap().unwrap();
        let mut out = Vec::new();
        protocol::encode_reply(hello.id, &Reply::HelloOk { version: 1 }, &mut out);
        protocol::write_all(&mut s, &out).await.unwrap();
        let req = protocol::read_frame(&mut s).await.unwrap().unwrap();
        out.clear();
        protocol::encode_reply(req.id + 1, &Reply::Events(vec![]), &mut out);
        protocol::write_all(&mut s, &out).await.unwrap();
        // Keep the socket open: the client must not wait for more.
        let _ = protocol::read_frame(&mut s).await;
    });
    let c = Client::connect(addr).await.unwrap();
    let err = c.request(Op::Tick).await;
    assert!(
        matches!(&err, Err(ClientError::Protocol(m)) if m.contains("expected 1")),
        "{err:?}"
    );
    // The connection is unusable afterwards; later calls fail at once.
    let later = tokio::time::timeout(Duration::from_secs(5), c.request(Op::Tick)).await;
    assert!(matches!(later, Ok(Err(_))), "{later:?}");
}

#[tokio::test]
async fn a_worker_completes_with_a_result() {
    let clock = ManualClock::new(0);
    let server = start(&clock).await;
    let c = Client::connect(server.local_addr()).await.unwrap();
    let job = c
        .enqueue(&q("a"), p("21"), Millis(0), None)
        .await
        .unwrap()
        .job;
    assert_eq!(c.result(job).await.unwrap(), spool::ResultStatus::Pending);
    let mut worker = Worker::new(c.clone(), q("a"), WorkerOptions::default());
    let mut double = |job: spool::client::Leased| async move {
        let n: u32 = String::from_utf8(job.payload.0).unwrap().parse().unwrap();
        Ok::<_, ()>(Payload((n * 2).to_string().into_bytes()))
    };
    assert_eq!(
        worker.step(&mut double).await.unwrap(),
        Outcome::Completed(job)
    );
    assert_eq!(
        c.result(job).await.unwrap(),
        spool::ResultStatus::Done {
            token: spool::Token(1),
            payload: p("42")
        }
    );
    drop((c, worker));
    let state = finish(server).await;
    assert_eq!((state.counts().acked, state.results().len()), (1, 1));
}

/// A proxy that forwards the first connection until the server's `n`-th reply
/// frame, then drops that reply and both sockets. Later connections pass
/// through untouched.
async fn lossy_proxy(server: SocketAddr, n: usize) -> SocketAddr {
    use spool::protocol::read_frame;
    use tokio::io::AsyncWriteExt;
    let listener = tokio::net::TcpListener::bind(any_port()).await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut first = true;
        loop {
            let (mut client, _) = listener.accept().await.unwrap();
            let mut upstream = tokio::net::TcpStream::connect(server).await.unwrap();
            if !std::mem::take(&mut first) {
                tokio::spawn(async move {
                    let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
                });
                continue;
            }
            tokio::spawn(async move {
                let (mut cr, mut cw) = client.into_split();
                let (mut ur, mut uw) = upstream.into_split();
                let up = tokio::spawn(async move {
                    let _ = tokio::io::copy(&mut cr, &mut uw).await;
                });
                for _ in 1..n {
                    let Ok(Some(f)) = read_frame(&mut ur).await else {
                        return;
                    };
                    let mut bytes = ((9 + f.body.len()) as u32).to_le_bytes().to_vec();
                    bytes.push(f.kind);
                    bytes.extend_from_slice(&f.id.to_le_bytes());
                    bytes.extend_from_slice(&f.body);
                    cw.write_all(&bytes).await.unwrap();
                }
                // The n-th reply: read it, so the request certainly landed, then lose it.
                let _ = read_frame(&mut ur).await;
                up.abort();
            });
        }
    });
    addr
}

#[tokio::test]
async fn a_lost_complete_reply_is_retried_on_a_new_connection() {
    let clock = ManualClock::new(0);
    let server = start(&clock).await;
    let c = Client::connect(server.local_addr()).await.unwrap();
    let job = c
        .enqueue(&q("a"), p("x"), Millis(0), None)
        .await
        .unwrap()
        .job;
    // Replies on the worker's first connection: hello_ok, leased, completed (lost).
    let proxy = lossy_proxy(server.local_addr(), 3).await;
    let mut worker = Worker::connect(&proxy.to_string(), q("a"), WorkerOptions::default())
        .await
        .unwrap();
    let mut handler = |_| async { Ok::<_, ()>(p("done")) };
    // The first complete landed; the retry on a new connection is a repeat
    // by the same lease, and succeeds (D42).
    assert_eq!(
        worker.step(&mut handler).await.unwrap(),
        Outcome::Completed(job)
    );
    assert_eq!(
        c.result(job).await.unwrap(),
        spool::ResultStatus::Done {
            token: spool::Token(1),
            payload: p("done")
        }
    );
    drop((c, worker));
    assert_eq!(finish(server).await.counts().acked, 1);
}
