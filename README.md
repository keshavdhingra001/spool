# spool

A distributed task queue in Rust, built from the bottom up: its own broker and storage (no Redis or
Postgres underneath), leases with visibility timeouts and fencing tokens, effectively-once
processing, Raft replication written from scratch, and partition testing by deterministic simulation.

**Status:** M0 (scaffold). The command/event model and text format exist; the queue itself arrives in M1.
Design decisions with alternatives and reasons are in [DESIGN.md](DESIGN.md).

## Design in one paragraph

The queue is a pure state machine: `apply(command) -> events`, with no clock, randomness or I/O inside
(D4). Time arrives in every command, so lease expiry is deterministic and the same command log always
produces the same state, which is what makes write-ahead-log recovery, Raft replication and seeded
simulation testing possible. Delivery is at-least-once; duplicate effects are prevented inside a stated
boundary by idempotency keys, fencing tokens and transactional acks (D3, D5).

## Try it

```
cargo run
> @0 enqueue emails send:42
parsed: @0 enqueue emails send:42
> @5 lease emails 30000
parsed: @5 lease emails 30000
```

Type `help` for the full command list (D12).

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
