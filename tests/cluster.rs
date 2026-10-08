//! The replicated queue over real TCP (M7, D71–D73): three in-process
//! replicas on loopback, each running both checkers after every applied
//! command, driven by the cluster client and the worker library.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use spool::client::{Client, ClientError};
use spool::cluster::{ClusterOptions, ClusterServer};
use spool::storage::MemStorage;
use spool::worker::{Outcome, Worker, WorkerOptions};
use spool::{Millis, Op, Payload, QueueName, ResultStatus};
use tokio::net::TcpListener;

fn q() -> QueueName {
    QueueName::new("jobs").unwrap()
}

struct Cluster {
    members: BTreeMap<u32, SocketAddr>,
    servers: BTreeMap<u32, ClusterServer<MemStorage>>,
}

impl Cluster {
    async fn start() -> Cluster {
        let mut listeners = BTreeMap::new();
        for id in 0..3 {
            listeners.insert(id, TcpListener::bind("127.0.0.1:0").await.unwrap());
        }
        let members: BTreeMap<u32, SocketAddr> = listeners
            .iter()
            .map(|(&id, l)| (id, l.local_addr().unwrap()))
            .collect();
        let mut c = Cluster {
            members,
            servers: BTreeMap::new(),
        };
        for (id, listener) in listeners {
            c.start_one(id, MemStorage::new(), listener).await;
        }
        c
    }

    async fn start_one(&mut self, id: u32, storage: MemStorage, listener: TcpListener) {
        let options = ClusterOptions {
            check: true,
            ..ClusterOptions::new(id, self.members.clone())
        };
        let server = ClusterServer::start(storage, listener, options)
            .await
            .unwrap();
        self.servers.insert(id, server);
    }

    fn client(&self) -> Client {
        Client::cluster(self.members.clone(), Duration::from_secs(10))
    }

    /// The replica that leads, as the replicas themselves say.
    async fn leader(&self) -> u32 {
        for _ in 0..100 {
            for (&id, &addr) in &self.members {
                if !self.servers.contains_key(&id) {
                    continue;
                }
                let one = Client::connect(addr).await.unwrap();
                match one.request(Op::Tick).await {
                    Ok(_) => return id,
                    Err(ClientError::NotLeader(_) | ClientError::Unknown) => {}
                    Err(e) => panic!("n{id}: {e}"),
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("no leader within 5 s");
    }

    async fn stop(&mut self, id: u32) -> MemStorage {
        let stopped = self.servers.remove(&id).unwrap().shutdown().await;
        stopped.result.unwrap();
        stopped.storage
    }
}

fn worker(client: Client) -> Worker {
    let options = WorkerOptions {
        visibility: Millis(5_000),
        idle_min: Duration::from_millis(1),
        idle_max: Duration::from_millis(10),
    };
    Worker::new(client, q(), options)
}

/// Lease and complete jobs until `n` are done, each with its payload as the
/// result.
async fn work(w: &mut Worker, n: usize) {
    let mut done = 0;
    while done < n {
        let outcome = w
            .step(&mut |job: spool::client::Leased| async move { Ok::<_, ()>(job.payload) })
            .await
            .unwrap();
        match outcome {
            Outcome::Completed(_) => done += 1,
            Outcome::Idle => tokio::time::sleep(Duration::from_millis(5)).await,
            other => panic!("{other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_replicas_serve_the_queue_and_followers_redirect() {
    let c = Cluster::start().await;
    let leader = c.leader().await;
    for (&id, &addr) in &c.members {
        if id != leader {
            let one = Client::connect(addr).await.unwrap();
            let err = one.request(Op::Tick).await;
            assert!(
                matches!(err, Err(ClientError::NotLeader(Some(l))) if l == leader),
                "n{id}: {err:?}"
            );
            // The connection stays usable after a redirect.
            assert!(matches!(
                one.request(Op::Tick).await,
                Err(ClientError::NotLeader(_))
            ));
        }
    }

    let client = c.client();
    let mut jobs = Vec::new();
    for i in 0..20u8 {
        let e = client
            .enqueue(&q(), Payload(vec![i]), Millis(0), None)
            .await
            .unwrap();
        assert!(!e.deduplicated, "an automatic key is not the caller's");
        jobs.push(e.job);
    }
    work(&mut worker(c.client()), 20).await;
    for (i, &job) in jobs.iter().enumerate() {
        match client.result(job).await.unwrap() {
            ResultStatus::Done { payload, .. } => assert_eq!(payload, Payload(vec![i as u8])),
            other => panic!("job {job}: {other:?}"),
        }
    }
    for (_, s) in c.servers {
        let stopped = s.shutdown().await;
        stopped.result.unwrap();
        assert!(stopped.stats.commands > 0 || stopped.stats.batches == 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_queue_survives_its_leader_and_a_restarted_replica_catches_up() {
    let mut c = Cluster::start().await;
    let client = c.client();
    let first = client
        .enqueue(&q(), Payload(b"a".to_vec()), Millis(0), None)
        .await
        .unwrap();

    // Stop the leader: the other two elect one and the client finds it.
    let old = c.leader().await;
    let storage = c.stop(old).await;
    let second = client
        .enqueue(&q(), Payload(b"b".to_vec()), Millis(0), None)
        .await
        .unwrap();
    assert!(second.job > first.job);

    // Bring it back from its log on its own address, then stop the new
    // leader: the remaining quorum needs the restarted replica, so it must
    // have caught up to commit anything.
    let listener = TcpListener::bind(c.members[&old]).await.unwrap();
    c.start_one(old, storage, listener).await;
    let new = c.leader().await;
    assert_ne!(new, old);
    c.stop(new).await;
    let third = client
        .enqueue(&q(), Payload(b"c".to_vec()), Millis(0), None)
        .await
        .unwrap();
    assert!(third.job > second.job);
    work(&mut worker(c.client()), 3).await;
    for job in [first.job, second.job, third.job] {
        assert!(matches!(
            client.result(job).await.unwrap(),
            ResultStatus::Done { .. }
        ));
    }
    for id in c.servers.keys().copied().collect::<Vec<_>>() {
        c.stop(id).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn with_no_quorum_the_client_gives_up_after_its_retry_time() {
    let mut c = Cluster::start().await;
    let leader = c.leader().await;
    for id in [0, 1, 2] {
        if id != leader {
            c.stop(id).await;
        }
    }
    let client = Client::cluster(c.members.clone(), Duration::from_millis(600));
    let started = std::time::Instant::now();
    let err = client.request(Op::Tick).await;
    assert!(err.is_err(), "{err:?}");
    assert!(started.elapsed() >= Duration::from_millis(600));
    c.stop(leader).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_does_not_wait_for_a_request_that_cannot_commit() {
    let mut c = Cluster::start().await;
    let leader = c.leader().await;
    for id in [0, 1, 2] {
        if id != leader {
            c.stop(id).await;
        }
    }
    // Sent at once, before CheckQuorum can depose the leader: it is proposed
    // and can never commit.
    let one = Client::connect(c.members[&leader]).await.unwrap();
    let stuck = tokio::spawn(async move { one.request(Op::Tick).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!stuck.is_finished(), "answered early: {:?}", stuck.await);
    let server = c.servers.remove(&leader).unwrap();
    let stopped = tokio::time::timeout(Duration::from_secs(5), server.shutdown())
        .await
        .expect("shutdown finished");
    stopped.result.unwrap();
    // The client learns nothing: its connection closes without an answer.
    assert!(stuck.await.unwrap().is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_cluster_keeps_its_leader() {
    let mut c = Cluster::start().await;
    let leader = c.leader().await;
    // Ten election timeouts with no requests: only heartbeats hold it.
    tokio::time::sleep(Duration::from_millis(3_000)).await;
    assert_eq!(c.leader().await, leader);
    for id in [0, 1, 2] {
        c.stop(id).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_requests_are_split_into_entries_that_fit_a_peer_frame() {
    let mut c = Cluster::start().await;
    let client = c.client();
    // 12 enqueues of 700 KB at once: one entry with all of them would be an
    // 8 MB append, over the 4 MiB peer frame, and would never replicate.
    let tasks: Vec<_> = (0..12u8)
        .map(|i| {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .enqueue(&q(), Payload(vec![i; 700_000]), Millis(0), None)
                    .await
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap().unwrap();
    }
    for id in [0, 1, 2] {
        let s = c.stop(id).await;
        let _ = s;
    }
}
