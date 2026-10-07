# spool

A distributed task queue in Rust, built from the bottom up: its own broker and storage (no Redis or
Postgres underneath), leases with visibility timeouts and fencing tokens, effectively-once
processing, Raft replication written from scratch, and partition testing by deterministic simulation.

**Status:** M3. A single-node queue: leases with visibility timeouts and fencing tokens, heartbeats,
ack/nack, retries with capped exponential backoff, delayed jobs, a dead-letter state with redrive
(M1). Durable (M2): every command goes into a checksummed write-ahead log and is synced before
its result is returned, with periodic snapshots and recovery that is tested by failing at every
storage call and recovering every state a crash could leave on disk, including a second crash during
recovery. Networked (M3): a TCP server with a small binary protocol and group commit (about 25x the
throughput of one sync per command, D30), deduplication keys that survive restarts (D35), and an
async client and worker library that heartbeats while a job runs and drops the job when its lease
is lost (D36). Effectively-once (M4) inside a stated boundary (D40): `complete` acks a job and
stores its result in one log record, and is safe to repeat after a lost reply (D41, D42); lease
tokens fence a worker's writes to other stores, so a worker that stalled past its lease cannot
overwrite newer work (D43). A seeded test kills workers before and after their effects, loses their
replies and wakes zombies, and checks that every job's effect is the one its completing lease made
(D46). Tier 1 is done; the deterministic simulator (M5) comes next.
Design decisions with alternatives and reasons are in [DESIGN.md](DESIGN.md).

## Design in one paragraph

The queue is a pure state machine: `apply(command) -> events`, with no clock, randomness or I/O inside
(D4). Time arrives in every command, so lease expiry is deterministic and the same command log always
produces the same state, which is what makes write-ahead-log recovery, Raft replication and seeded
simulation testing possible. Delivery is at-least-once; duplicate effects are prevented inside a stated
boundary by idempotency keys, fencing tokens and transactional acks (D3, D5).

## Try it

The zombie worker, by hand: worker A's lease expires, worker B gets the job, A comes back.

```
cargo run
> @0 configure q 3 0 0
configured queue=q max_attempts=3 backoff_base=0 backoff_cap=0
> @0 enqueue q charge-card
enqueued job=1 queue=q ready_at=0
> @0 lease q 30
leased job=1 token=1 deadline=30 attempt=1 payload=charge-card
> @30 lease q 30
released job=1 token=1 reason=expired
retrying job=1 ready_at=30
leased job=1 token=2 deadline=60 attempt=2 payload=charge-card
> @41 ack 1 1
rejected reason=stale_token
> @50 ack 1 2
acked job=1
```

Durable mode keeps the queue in a directory and recovers it on the next start (D21–D29):

```
cargo run -- --data /tmp/q
> @0 enqueue q charge-card
enqueued job=1 queue=q ready_at=0
> quit
cargo run -- --data /tmp/q
spool queue in /tmp/q: snapshot at LSN 0, 1 commands replayed, 0 torn bytes cut. Type `help`.
> jobs
now=0 waiting=1 leased=0 dead=0 acked=0
  job=1 queue=q attempts=0 waiting ready_at=0
```

Over the network (D30–D36): one terminal runs the server, another talks to it. Commands are the
same, without `@<ms>`: the server stamps the time.

```
cargo run -- serve --data /tmp/q
listening on 127.0.0.1:7878

cargo run -- connect 127.0.0.1:7878
> enqueue orders charge-card key=order-42
enqueued job=1 queue=orders ready_at=1791415126468 key=order-42
> enqueue orders charge-card key=order-42
deduplicated job=1 queue=orders key=order-42
> lease orders 30000
leased job=1 token=1 deadline=1791415156480 attempt=1 payload=charge-card
```

From Rust, a worker is a closure (`spool::worker`). Returning `()` acks the job; returning a
`Payload` completes it with that result (D44). The lease's token goes to the downstream write, so a
store that checks it (D43, `spool::fence`) refuses a zombie's late write:

```rust
let mut worker = Worker::connect("127.0.0.1:7878", QueueName::new("orders")?, WorkerOptions::default()).await?;
worker
    .run(|job| async move {
        // UPDATE charges SET receipt = $1, token = $2 WHERE order_id = $3 AND token <= $2
        let receipt = charge(&job.payload, job.lease.token).await?;
        Ok::<_, Error>(receipt) // stored as the job's result, with the ack, in one record
    })
    .await?;
```

A producer that lost the reply to its enqueue retries with the same key and gets the job id back
(D35), then asks for the result:

```
> complete 1 1 receipt-77
completed job=1 token=1
> result 1
result job=1 done token=1 payload=receipt-77
```

Type `help` for every command (D12), `jobs` to see the queue, `run tests/scenarios/retry.txt` to run a
scenario file (D20).

## Roadmap

| Milestone | What |
|---|---|
| M0 | Scaffold: core types, command/event model, text format, `Queue` trait, REPL |
| M1 | Reference queue: leases, heartbeats, ack/nack, retries with backoff, delayed jobs, dead-letter queue, invariant checker |
| M2 | Durability: write-ahead log with CRCs, snapshots, recovery tested by crashing at every byte |
| M3 | TCP server, binary protocol, worker library, idempotent enqueue |
| M4 | Effectively-once: fencing tokens end to end, transactional ack, worker-crash tests |
| M5 | Deterministic simulator: network, disk and clock from one seed, fault injection |
| M6–M7 | Raft from scratch; the queue on Raft |
| M8 | Partitioning across Raft groups |
| M9 | Jepsen-style history checker |
| M10–M12 | Real processes under a fault proxy, benchmarks, write-up |
