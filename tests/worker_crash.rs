//! Worker crashes (D46): effectively-once inside the stated boundary (D40).
//!
//! A seeded scheduler drives workers against a checked in-process server
//! (D39). Each lease meets one fate: the worker dies before its effect, dies
//! after its effect and before completing, completes but loses the reply (the
//! complete landed), completes but the request is cut off (it did not land),
//! stalls and comes back later as a zombie, or completes normally. The clock
//! moves only when the scheduler says so, which is what expires leases.
//!
//! Every effect is a write to a `FencedStore` keyed by job (D43), with the
//! lease's token, followed by a complete with the same value as the result
//! (D41). At the end:
//! - every job is completed exactly once in the queue (counts and results);
//! - the result's token is the token whose complete was accepted, and a
//!   retried complete by that lease succeeded (D42);
//! - each job's value in the store was written by the completing token, so no
//!   zombie and no crashed lease left its effect behind;
//! - per key, the store accepted tokens in non-decreasing order;
//! - the server's checkers passed after every batch.

use std::collections::BTreeMap;
use std::time::Duration;

use spool::client::{Client, ClientError};
use spool::fence::FencedStore;
use spool::protocol::{self, Request};
use spool::server::{ManualClock, Server, ServerOptions};
use spool::storage::MemStorage;
use spool::{
    Durable, JobId, Millis, Op, Options, Payload, QueueConfig, QueueName, ResultStatus, Token,
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

const JOBS: u64 = 12;
const VISIBILITY: Millis = Millis(1_000);

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

#[derive(Debug, Default)]
struct Coverage {
    crashed_before_effect: u64,
    crashed_after_effect: u64,
    reply_lost: u64,
    request_cut: u64,
    zombies: u64,
    zombie_writes_refused: u64,
    zombie_completes_refused: u64,
    zombie_completes_accepted: u64,
    normal: u64,
    clock_jumps: u64,
}

/// Send `op` on a fresh connection and vanish without reading the reply. With
/// `whole`, the request reaches the server; without, its last byte is missing,
/// so the server never sees it.
async fn vanish(addr: std::net::SocketAddr, op: Op, whole: bool) {
    let mut bytes = Vec::new();
    protocol::encode_request(0, &Request::Hello { version: 1 }, &mut bytes);
    protocol::encode_request(1, &Request::Op(op), &mut bytes);
    if !whole {
        bytes.pop();
    }
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(&bytes).await.unwrap();
    s.shutdown().await.unwrap();
}

struct Run {
    rng: Rng,
    clock: ManualClock,
    addr: std::net::SocketAddr,
    c: Client,
    queue: QueueName,
    store: FencedStore<JobId, Payload>,
    /// Tokens the store accepted per job, in order.
    writes: BTreeMap<JobId, Vec<Token>>,
    /// The token whose complete the queue accepted, per job.
    completed: BTreeMap<JobId, Token>,
    zombies: Vec<(JobId, Token, Payload)>,
    cov: Coverage,
}

impl Run {
    fn effect(&mut self, job: JobId, token: Token, value: &Payload) -> bool {
        let ok = self.store.write(job, token, value.clone()).is_ok();
        if ok {
            self.writes.entry(job).or_default().push(token);
        }
        ok
    }

    fn accepted(&mut self, job: JobId, token: Token) {
        let first = self.completed.insert(job, token);
        assert!(
            first.is_none_or(|t| t == token),
            "job {job} completed by token {token} after token {first:?}"
        );
    }

    async fn wait_done(&self, job: JobId, token: Token) {
        for _ in 0..500 {
            if let ResultStatus::Done { token: t, .. } = self.c.result(job).await.unwrap() {
                assert_eq!(t, token);
                return;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        panic!("the complete of job {job} that was sent whole never landed");
    }

    async fn lease_one(&mut self) {
        let Some(job) = self.c.lease(&self.queue, VISIBILITY).await.unwrap() else {
            return;
        };
        let (id, token) = (job.lease.job, job.lease.token);
        let mut value = job.payload.0.clone();
        value.extend_from_slice(format!("@{token}").as_bytes());
        let value = Payload(value);
        match self.rng.below(6) {
            0 => self.cov.crashed_before_effect += 1,
            1 => {
                self.effect(id, token, &value);
                self.cov.crashed_after_effect += 1;
            }
            2 => {
                self.zombies.push((id, token, value));
                self.cov.zombies += 1;
            }
            fate @ (3 | 4) => {
                assert!(self.effect(id, token, &value));
                let op = Op::Complete {
                    job: id,
                    token,
                    result: value.clone(),
                };
                let whole = fate == 3;
                vanish(self.addr, op, whole).await;
                if whole {
                    // Make the order repeatable: the lost reply's complete lands first.
                    self.wait_done(id, token).await;
                    self.cov.reply_lost += 1;
                } else {
                    self.cov.request_cut += 1;
                }
                // The worker retries; landed or not, the answer is success (D42).
                self.c.complete(id, token, value).await.unwrap();
                self.accepted(id, token);
            }
            _ => {
                assert!(self.effect(id, token, &value));
                self.c.complete(id, token, value).await.unwrap();
                self.accepted(id, token);
                self.cov.normal += 1;
            }
        }
    }

    /// A stalled worker wakes up, unaware of how long it slept, and finishes.
    async fn zombie_wakes(&mut self, i: usize) {
        let (job, token, value) = self.zombies.swap_remove(i);
        let wrote = self.effect(job, token, &value);
        if !wrote {
            self.cov.zombie_writes_refused += 1;
        }
        match self.c.complete(job, token, value).await {
            Ok(()) => {
                // Its lease was still current, so nothing newer can have written.
                assert!(wrote, "job {job}: complete accepted after a fenced write");
                self.accepted(job, token);
                self.cov.zombie_completes_accepted += 1;
            }
            Err(ClientError::Rejected(_)) => self.cov.zombie_completes_refused += 1,
            Err(e) => panic!("{e}"),
        }
    }

    async fn all_done(&self) -> bool {
        for j in 1..=JOBS {
            if !matches!(
                self.c.result(JobId(j)).await.unwrap(),
                ResultStatus::Done { .. }
            ) {
                return false;
            }
        }
        true
    }
}

async fn run_seed(seed: u64) -> Coverage {
    let clock = ManualClock::new(0);
    let options = ServerOptions {
        clock: clock.clock(),
        check: true,
        ..ServerOptions::default()
    };
    let server = Server::start(MemStorage::new(), "127.0.0.1:0".parse().unwrap(), options)
        .await
        .unwrap();
    let addr = server.local_addr();
    let c = Client::connect(addr).await.unwrap();
    let queue = QueueName::new("work").unwrap();
    c.request(Op::Configure {
        queue: queue.clone(),
        config: QueueConfig {
            max_attempts: 1_000,
            backoff_base: Millis(0),
            backoff_cap: Millis(0),
        },
    })
    .await
    .unwrap();
    for j in 0..JOBS {
        c.enqueue(
            &queue,
            Payload(format!("j{j}").into_bytes()),
            Millis(0),
            None,
        )
        .await
        .unwrap();
    }
    let mut run = Run {
        rng: Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1),
        clock,
        addr,
        c,
        queue,
        store: FencedStore::new(),
        writes: BTreeMap::new(),
        completed: BTreeMap::new(),
        zombies: Vec::new(),
        cov: Coverage::default(),
    };
    for step in 0.. {
        assert!(step < 5_000, "seed {seed}: jobs never finished");
        if run.completed.len() as u64 == JOBS {
            break;
        }
        match run.rng.below(10) {
            0..=5 => run.lease_one().await,
            6 | 7 => {
                run.clock.advance(VISIBILITY.0 + 1);
                run.cov.clock_jumps += 1;
            }
            _ if !run.zombies.is_empty() => {
                let i = run.rng.below(run.zombies.len() as u64) as usize;
                run.zombie_wakes(i).await;
            }
            _ => {}
        }
    }
    assert!(run.all_done().await);
    // The zombies still asleep wake after everything is done: all refused.
    while !run.zombies.is_empty() {
        let refused = run.cov.zombie_completes_refused;
        run.zombie_wakes(0).await;
        assert_eq!(run.cov.zombie_completes_refused, refused + 1);
    }

    for j in 1..=JOBS {
        let job = JobId(j);
        let token = run.completed[&job];
        let ResultStatus::Done { token: t, payload } = run.c.result(job).await.unwrap() else {
            panic!("seed {seed}: job {job} not done");
        };
        assert_eq!(t, token, "seed {seed}: job {job}: result token");
        let (wrote, value) = run.store.get(&job).expect("an effect for every job");
        assert_eq!(
            (wrote, value),
            (token, &payload),
            "seed {seed}: job {job}: the store holds the completing lease's effect"
        );
        let tokens = &run.writes[&job];
        assert!(
            tokens.windows(2).all(|w| w[0] <= w[1]),
            "seed {seed}: job {job}: accepted tokens {tokens:?}"
        );
    }
    drop(run.c);
    let stopped = server.shutdown().await;
    stopped.result.unwrap();
    let (d, _) = Durable::<MemStorage>::open(stopped.storage, Options::default()).unwrap();
    let counts = d.queue().counts();
    assert_eq!(
        (counts.acked, counts.waiting + counts.leased + counts.dead),
        (JOBS, 0)
    );
    assert_eq!(d.queue().results().len() as u64, JOBS);
    run.cov
}

#[tokio::test]
async fn effectively_once_under_worker_crashes() {
    let mut total = Coverage::default();
    for seed in 1..=200 {
        let c = run_seed(seed).await;
        total.crashed_before_effect += c.crashed_before_effect;
        total.crashed_after_effect += c.crashed_after_effect;
        total.reply_lost += c.reply_lost;
        total.request_cut += c.request_cut;
        total.zombies += c.zombies;
        total.zombie_writes_refused += c.zombie_writes_refused;
        total.zombie_completes_refused += c.zombie_completes_refused;
        total.zombie_completes_accepted += c.zombie_completes_accepted;
        total.normal += c.normal;
        total.clock_jumps += c.clock_jumps;
    }
    eprintln!("{total:?}");
    let c = &total;
    for (name, n) in [
        ("crashed before effect", c.crashed_before_effect),
        ("crashed after effect", c.crashed_after_effect),
        ("reply lost", c.reply_lost),
        ("request cut", c.request_cut),
        ("zombie write refused", c.zombie_writes_refused),
        ("zombie complete refused", c.zombie_completes_refused),
        ("zombie complete accepted", c.zombie_completes_accepted),
        ("normal", c.normal),
    ] {
        assert!(n > 0, "never reached: {name}; {total:?}");
    }
}
