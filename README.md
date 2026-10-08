# spool

A distributed task queue in Rust, built from the bottom up: its own broker and storage (no Redis or
Postgres underneath), leases with visibility timeouts and fencing tokens, effectively-once
processing, partition testing by deterministic simulation, and Raft replication written from
scratch.

**Status:** M7. Tier 1 complete. A single-node queue: leases with visibility timeouts and fencing tokens, heartbeats,
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
(D46). Simulated (M5): a deterministic simulator runs the server, its log on a simulated disk, a
fenced store, producers and workers in one thread from one seed, through lost, duplicated and late
messages, partitions, crashes, torn writes, pauses and clock jumps, and checks durability,
deduplication and effectively-once effects at every batch and every recovery (D47–D55). Raft (M6):
a pure Raft node with PreVote, CheckQuorum, conflict hints and the current-term commit rule, its
log in the same checksummed format, run in the simulator as clusters of 3 or 5 under the same
faults plus one-way cuts and crashes right after a sync, and checked live for election safety,
log matching, leader completeness and state machine safety (D56–D65). Replicated (M7): the queue
runs on that Raft, one server batch per log entry, every op (reads too) answered only once its
entry commits with the term it was proposed in; followers redirect, and the cluster client follows
leaders and retries, giving every enqueue a dedup key so a retry never adds a job (D66–D73). The M5
producers, workers and fenced store run against 3- or 5-replica clusters in the simulator, and
three `spool serve --cluster` processes survive `kill -9` of their leader.
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

## Simulation

Every run of the simulator follows from one seed (D48): the network's losses and delays, which node
crashes when, which crash image its disk is left with. A failing seed replays the same events every
time, so it is a complete bug report:

```
cargo run --release -- sim --seeds 0..10000          # sweep; stops at the first failure
cargo run --release -- sim --seed 42 --trace         # replay one seed, printing every event
```

```
cargo run --release -- sim --seed 42 --trace
    2837 recv n4->n1 #169 write job=1 token=1 value=1:1     worker n4's effect reaches the store
    2847 recv n1->n4 #169 -> written ok=true
    2848 recv n4->n0 #170 complete 1 1 1:1                  then the complete, with the same token
    2855 recv n0->n4 #170 -> completed job=1 token=1
    4627 crash n4 (1 images)
```

Two bugs can be planted on purpose to show the checks find them (D54): `--bug no-fence` (the store
accepts a zombie's late write) and `--bug no-dedup-key` (a producer's retry adds a second job). Both
fail within the first few seeds, with the seed and the replay command in the message.

`--raft` runs a Raft cluster instead (M6): 3 or 5 nodes, three clients proposing 20 operations each,
the same faults plus one-way cuts and crashes right after a sync:

```
cargo run --release -- sim --raft --seeds 0..10000
cargo run --release -- sim --raft --seed 4 --trace
     234 recv n1->n0 prevote t1 last=0/t0                 n1's timer fired: could it win term 1?
     239 recv n0->n1 prevote-reply t1 granted=true
     243 recv n1->n2 vote t1 last=0/t0                    a majority said yes: now it raises its term
     250 recv n2->n1 vote-reply t1 granted=true
     256 recv n1->n0 append t1 prev=0/t0 commit=0 [t1:no-op]   leader of term 1: its no-op first (D61)
     258 recv n0->n1 append-ok t1 matched=1
```

Its planted bugs (D64) are `vote-not-persisted`, `commit-old-term`, `no-log-truncate` and
`stale-term-accept`. The last two fail at seed 0. The first two need schedules a random swarm rarely
builds (Figure 8 of the Raft paper; a crash between two candidates' vote requests), so scripted
unit tests pin them, and DESIGN.md D64 gives the numbers.

A replicated queue (M7) is three processes, each given every replica's address:

```
cargo run -- serve --data /tmp/n0 --id 0 --cluster 0=127.0.0.1:7001,1=127.0.0.1:7002,2=127.0.0.1:7003
cargo run -- serve --data /tmp/n1 --id 1 --cluster 0=127.0.0.1:7001,1=127.0.0.1:7002,2=127.0.0.1:7003
cargo run -- serve --data /tmp/n2 --id 2 --cluster 0=127.0.0.1:7001,1=127.0.0.1:7002,2=127.0.0.1:7003
cargo run -- connect --cluster 0=127.0.0.1:7001,1=127.0.0.1:7002,2=127.0.0.1:7003
```

Kill the leader and the client finds the next one; restart it on its directory and it catches up
from its Raft log. In the simulator, `--cluster` runs the M5 workload against such a cluster
under every fault of M5 and M6:

```
cargo run --release -- sim --cluster --seeds 0..10000
```

Its planted bugs (D73) are `reply-before-commit` (the leader answers before the entry commits) and
`ignore-term-on-reply` (the leader answers with whatever entry took its proposal's index); the
queue's `no-fence` and `no-dedup-key` work there too.

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
| M5 | Deterministic simulator: network, disk and clock from one seed, fault injection (done) |
| M6 | Raft from scratch: election, replication, commit, tested in the simulator (done) |
| M7 | The queue on Raft: batches as entries, redirects, retries with dedup keys (done) |
| M8 | Partitioning across Raft groups |
| M9 | Jepsen-style history checker |
| M10–M12 | Real processes under a fault proxy, benchmarks, write-up |
