//! The server over raw sockets (D30–D32, D37, D38): handshake, protocol errors,
//! pipelining, group commit, disk errors and restarts. Every server here runs
//! both checkers after every batch (D39).

use std::net::SocketAddr;

use spool::protocol::{self, ErrorCode, Frame, Reply, Request};
use spool::server::{ManualClock, Server, ServerError, ServerOptions};
use spool::storage::MemStorage;
use spool::{Op, StoreError};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

fn options(clock: &ManualClock) -> ServerOptions {
    ServerOptions {
        clock: clock.clock(),
        check: true,
        ..ServerOptions::default()
    }
}

async fn start(storage: MemStorage, clock: &ManualClock) -> Server<MemStorage> {
    let any: SocketAddr = "127.0.0.1:0".parse().unwrap();
    Server::start(storage, any, options(clock)).await.unwrap()
}

fn op(line: &str) -> Op {
    format!("@0 {line}").parse::<spool::Command>().unwrap().op
}

fn frames(reqs: &[(u64, Request)]) -> Vec<u8> {
    let mut out = Vec::new();
    for (id, r) in reqs {
        protocol::encode_request(*id, r, &mut out);
    }
    out
}

async fn read(s: &mut TcpStream) -> Option<Frame> {
    protocol::read_frame(s).await.unwrap()
}

async fn connect(addr: SocketAddr) -> TcpStream {
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(&frames(&[(0, Request::Hello { version: 1 })]))
        .await
        .unwrap();
    assert_eq!(
        read(&mut s).await.unwrap().reply().unwrap(),
        Reply::HelloOk { version: 1 }
    );
    s
}

fn events(f: Frame) -> Vec<String> {
    match f.reply().unwrap() {
        Reply::Events(e) => e.iter().map(ToString::to_string).collect(),
        other => panic!("expected events, got {other:?}"),
    }
}

#[tokio::test]
async fn handshake_and_protocol_errors() {
    let clock = ManualClock::new(1_000);
    let server = start(MemStorage::new(), &clock).await;
    let addr = server.local_addr();

    let refused = async |bytes: Vec<u8>, code: ErrorCode| {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(&bytes).await.unwrap();
        let first = tokio::time::timeout(std::time::Duration::from_secs(5), read(&mut s))
            .await
            .expect("an answer");
        match first.unwrap().reply().unwrap() {
            Reply::Error { code: c, message } => assert_eq!(c, code, "{message}"),
            other => panic!("expected an error, got {other:?}"),
        }
        assert!(read(&mut s).await.is_none(), "closed after the error");
    };
    refused(
        frames(&[(1, Request::Hello { version: 4 })]),
        ErrorCode::UnsupportedVersion,
    )
    .await;
    refused(frames(&[(1, Request::Op(op("tick")))]), ErrorCode::Protocol).await;
    // Only a cluster replica takes Raft peers (M7).
    refused(
        frames(&[(
            1,
            Request::Peer {
                version: 2,
                from: 0,
            },
        )]),
        ErrorCode::Protocol,
    )
    .await;
    // A length over the cap, refused before reading the body.
    refused(
        (protocol::MAX_FRAME + 1).to_le_bytes().to_vec(),
        ErrorCode::Protocol,
    )
    .await;
    // A second hello, and an op that does not decode.
    let mut hello_twice = frames(&[(0, Request::Hello { version: 1 })]);
    let mut s = TcpStream::connect(addr).await.unwrap();
    hello_twice.extend(frames(&[(1, Request::Hello { version: 1 })]));
    s.write_all(&hello_twice).await.unwrap();
    assert!(matches!(
        read(&mut s).await.unwrap().reply(),
        Ok(Reply::HelloOk { .. })
    ));
    assert!(matches!(
        read(&mut s).await.unwrap().reply(),
        Ok(Reply::Error { .. })
    ));
    assert!(read(&mut s).await.is_none());
    let mut s = connect(addr).await;
    let mut bad = frames(&[(5, Request::Op(op("tick")))]);
    bad[13] = 77; // op tag
    s.write_all(&bad).await.unwrap();
    let f = read(&mut s).await.unwrap();
    assert_eq!(f.id, 5);
    assert!(matches!(
        f.reply(),
        Ok(Reply::Error {
            code: ErrorCode::Protocol,
            ..
        })
    ));

    let stopped = server.shutdown().await;
    stopped.result.unwrap();
    assert_eq!(stopped.stats.commands, 0, "nothing reached the queue");
}

#[tokio::test]
async fn a_single_node_is_partition_0_of_one() {
    let clock = ManualClock::new(1_000);
    let server = start(MemStorage::new(), &clock).await;
    let mut s = connect(server.local_addr()).await;
    let routed = |partition, line| Request::Routed {
        partition,
        op: op(line),
    };
    s.write_all(&frames(&[(1, routed(0, "enqueue q a"))]))
        .await
        .unwrap();
    assert_eq!(
        events(read(&mut s).await.unwrap()),
        ["enqueued job=1 queue=q ready_at=1000"]
    );
    // There is no partition 1: the connection is refused, as for any frame
    // the server cannot serve.
    s.write_all(&frames(&[(2, routed(1, "lease q 10"))]))
        .await
        .unwrap();
    let f = read(&mut s).await.unwrap();
    assert_eq!(f.id, 2);
    match f.reply().unwrap() {
        Reply::Error { code, message } => {
            assert_eq!(code, ErrorCode::Protocol);
            assert!(message.contains("partition 1"), "{message}");
        }
        other => panic!("{other:?}"),
    }
    assert!(read(&mut s).await.is_none());
    server.shutdown().await.result.unwrap();
}

#[tokio::test]
async fn pipelined_requests_are_answered_in_order() {
    let clock = ManualClock::new(1_000);
    let server = start(MemStorage::new(), &clock).await;
    let mut s = connect(server.local_addr()).await;
    // 300 requests in one write, past the 128 in-flight cap: the server stops
    // reading for a while (D37) but answers every one, in order.
    let mut reqs = Vec::new();
    for i in 0..200u64 {
        reqs.push((100 + i, Request::Op(op(&format!("enqueue q p{i}")))));
    }
    for i in 0..100u64 {
        reqs.push((1_000 + i, Request::Op(op("lease q 30000"))));
    }
    s.write_all(&frames(&reqs)).await.unwrap();
    for (i, (id, _)) in reqs.iter().enumerate() {
        let f = read(&mut s).await.unwrap();
        assert_eq!(f.id, *id);
        let got = events(f);
        if i < 200 {
            assert_eq!(
                got,
                [format!("enqueued job={} queue=q ready_at=1000", i + 1)]
            );
        } else {
            let job = i - 199;
            assert_eq!(
                got,
                [format!(
                    "leased job={job} token={job} deadline=31000 attempt=1 payload=p{}",
                    job - 1
                )]
            );
        }
    }
    drop(s);
    let stopped = server.shutdown().await;
    stopped.result.unwrap();
    assert_eq!(stopped.stats.commands, 300);
    assert!(stopped.stats.batches <= 300);
    // Both checkers ran after every batch (D39).
    assert_eq!(stopped.stats.checked, stopped.stats.batches);
    eprintln!("{:?}", stopped.stats);
}

#[tokio::test]
async fn time_comes_from_the_server_clock() {
    let clock = ManualClock::new(5_000);
    let server = start(MemStorage::new(), &clock).await;
    let mut s = connect(server.local_addr()).await;
    let mut ask = async |id: u64, line: &str| {
        s.write_all(&frames(&[(id, Request::Op(op(line)))]))
            .await
            .unwrap();
        events(read(&mut s).await.unwrap())
    };
    assert_eq!(
        ask(1, "enqueue q x").await,
        ["enqueued job=1 queue=q ready_at=5000"]
    );
    assert_eq!(
        ask(2, "lease q 100").await,
        ["leased job=1 token=1 deadline=5100 attempt=1 payload=x"]
    );
    // The clock steps back: D9 clamps, nothing moves backwards.
    clock.set(10);
    assert_eq!(
        ask(3, "enqueue q y").await,
        ["enqueued job=2 queue=q ready_at=5000"]
    );
    // Expiry is lazy: nothing happens until a command arrives (D31).
    clock.set(6_000);
    let got = ask(4, "tick").await;
    assert_eq!(got[0], "released job=1 token=1 reason=expired");
    drop(s);
    server.shutdown().await.result.unwrap();
}

#[tokio::test]
async fn a_restart_recovers_the_queue() {
    let clock = ManualClock::new(1_000);
    let server = start(MemStorage::new(), &clock).await;
    let mut s = connect(server.local_addr()).await;
    let reqs = [
        (1, Request::Op(op("enqueue q a key=order-1"))),
        (2, Request::Op(op("enqueue q b"))),
    ];
    s.write_all(&frames(&reqs)).await.unwrap();
    read(&mut s).await.unwrap();
    read(&mut s).await.unwrap();
    drop(s);
    let disk = server.shutdown().await.storage;

    clock.advance(1_000);
    let server = start(disk, &clock).await;
    let mut s = connect(server.local_addr()).await;
    s.write_all(&frames(&[(1, Request::Op(op("enqueue q a key=order-1")))]))
        .await
        .unwrap();
    assert_eq!(
        events(read(&mut s).await.unwrap()),
        ["deduplicated job=1 queue=q key=order-1"]
    );
    drop(s);
    server.shutdown().await.result.unwrap();
}

#[tokio::test]
async fn a_disk_error_stops_the_server() {
    let clock = ManualClock::new(1_000);
    // Opening an empty store takes 4 storage calls; the first append fails.
    let mut server = start(MemStorage::new().fail_after(4), &clock).await;
    let mut s = connect(server.local_addr()).await;
    s.write_all(&frames(&[(1, Request::Op(op("enqueue q a")))]))
        .await
        .unwrap();
    // No reply: the connection closes (D38).
    assert!(read(&mut s).await.is_none());
    let stopped = server.stopped().await;
    assert!(
        matches!(stopped.result, Err(ServerError::Store(StoreError::Io(_)))),
        "{:?}",
        stopped.result.err()
    );
    // The unsynced record may or may not be on disk; either way it recovers.
    for image in stopped.storage.crash_images() {
        let server = start(image, &clock).await;
        server.shutdown().await.result.unwrap();
    }
}
