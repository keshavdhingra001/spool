# spool

A distributed task queue in Rust, built from the bottom up: its own broker and storage (no Redis or
Postgres underneath), leases with visibility timeouts and fencing tokens, effectively-once
processing, Raft replication written from scratch, and partition testing by deterministic simulation.

**Status:** M2. A single-node queue: leases with visibility timeouts and fencing tokens, heartbeats,
ack/nack, retries with capped exponential backoff, delayed jobs, a dead-letter state with redrive
(M1), now durable (M2): every command goes into a checksummed write-ahead log and is synced before
its result is returned, with periodic snapshots and recovery that is tested by failing at every
storage call and recovering every state a crash could leave on disk, including a second crash during
recovery. Networking (M3) comes next.
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
