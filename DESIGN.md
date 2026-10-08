# spool design

Living document. Every non-obvious decision gets a short entry: **what**, **alternatives**, **why**.

## Architecture (target, after M8)

```
producers / workers ──TCP──> node ──────────────────────────────────────────────┐
                              │  decode, stamp time, forward to the Raft leader │
                              ▼                                                  │
                         Raft group (one per partition)                          │
                              │  committed commands, in log order                │
                              ▼                                                  │
                         queue state machine: Command in, Events out             │
                         pure, deterministic, logical time ◄─────────────────────┘
```

Today (M5): the reference queue (D17) behind the `Queue` trait (D11): leases with visibility
timeouts and fencing tokens (D5, D18), retries with capped exponential backoff and deterministic
jitter (D13) under a per-queue policy (D14), expiry counted as an attempt (D15), a dead-letter state
with redrive (D16), delayed jobs (D17) and deduplication keys (D35). Time arrives in every command
(D9). It is checked by two independent checkers (D19) over scenario files (D20) and random command
sequences. It is durable (D21–D29): a write-ahead log of commands, synced before any result is
returned, with snapshots and crash-tested recovery. It is served over TCP (D30–D33): one core
thread owns the queue and commits batches with one sync, connections are tokio tasks, and the
server stamps the time (D31). A client and worker library (D34, D36) sit on the other side.
Effectively-once processing holds inside a stated boundary (D40): a transactional complete stores a
job's result with its ack in one command (D41, D42), and lease tokens fence writes to external
stores (D43), tested against seeded worker crashes and zombies (D46). A deterministic simulator
(D47–D55) runs the server's batch logic, its log on a simulated disk, an external fenced store,
producers and workers as message-driven processes in one thread, from one seed, under lost,
duplicated and late messages, partitions, crashes, torn writes, pauses and clock jumps, and checks
durability, deduplication and effectively-once effects at every batch, every recovery and the end.

## Decisions

### D1: Language: Rust
- **Alternatives:** Go (the usual choice for brokers and Raft: etcd, NATS, Temporal), Java/Kotlin (Kafka).
- **Why:** same toolchain and workflow as lsmkv, lob and ember (cargo, clippy, proptest, criterion),
  and no data races to chase in the network and replication layers. Go would be the industry
  default and its runtime makes the network edge shorter to write, but the core here is a
  single-threaded state machine where Rust's ownership and enums do most of the work, and a
  garbage collector would only show up later as tail latency in the M11 benchmarks.

### D2: Our own broker and storage
- **What:** spool owns its queue state, its write-ahead log (M2) and its replication (M6). No
  Redis, Postgres, Kafka or SQS underneath.
- **Alternatives:** a Celery/Sidekiq-style layer over Redis lists, or a Postgres
  `SELECT ... FOR UPDATE SKIP LOCKED` queue.
- **Why:** a wrapper delegates every hard question (what happens to a leased job when the node
  crashes, when the network splits, when a worker stalls past its timeout) to the store underneath,
  so there is nothing of ours to test or defend. Owning the state machine, the log and the
  replication is what makes those questions ours. The Postgres design is the right production
  answer for many teams and gets mentioned in the README as the baseline we are not building.

### D3: At-least-once delivery, effectively-once processing
- **What:** a job may be delivered more than once (a lease expires while the worker is still
  running, an ack is lost on the way back). Duplicate *effects* are prevented inside a stated
  boundary by three mechanisms: idempotency keys on enqueue (M3), fencing tokens on every lease (M4),
  and a transactional ack that records the job's result and the ack in one log record (M4).
- **Alternatives:** at-most-once (ack on delivery, lose jobs on worker crash); claiming
  "exactly-once delivery".
- **Why:** exactly-once *delivery* is impossible with crashing workers and an unreliable network:
  a worker that dies after its side effect but before its ack is indistinguishable from one that
  died before the side effect. What can be built is exactly-once *effect* on state the queue
  controls (or state that checks fencing tokens). That is what Kafka's "exactly-once" and SQS FIFO
  deduplication actually provide, and spool states the boundary explicitly and tests it with
  worker crashes (M4) and partitions (M9).

### D4: Pure state machine with logical time
- **What:** the queue is `apply(&Command) -> Events` with no clock, no randomness, no I/O and no
  dependence on `HashMap` iteration order. The current time arrives inside every command. Tokio,
  sockets and files live only at the edge.
- **Alternatives:** an async broker that reads `Instant::now()` and spawns timers per lease.
- **Why:** a deterministic core is what makes everything after M1 testable. Recovery replays the
  log and reaches the same state (M2). Raft replicas apply the same committed commands and stay
  identical, which only works if `apply` depends on nothing but the command (M7). The simulator
  can run thousands of seeded failure schedules and replay any failing one exactly (M5). Lease
  expiry becomes a pure function of the time in the command, so "the worker stalled for 31 s"
  is a test input rather than a sleep. FoundationDB and TigerBeetle are built this way.

### D5: Leases with visibility timeouts and fencing tokens
- **What:** leasing a job hides it from other workers until a deadline. The worker extends the
  deadline with heartbeats. A lease that reaches its deadline expires and the job becomes
  leasable again. Failed jobs retry with backoff and move to a dead-letter queue after a maximum
  number of attempts (M1). Every lease carries a fencing token that increases on every lease, and
  heartbeat, ack and nack must present the current token.
- **Alternatives:** SQS-style receipt handles without ordering; locks held by a connection
  (job is released when the TCP connection drops).
- **Why:** connection-bound locks fail exactly when it matters: a network partition keeps the
  worker alive while the broker sees the connection drop, and a stalled worker (GC pause, swap)
  keeps its connection while doing nothing. A deadline is the only signal the broker can rely on.
  But a deadline alone allows a zombie: worker A stalls, its lease expires, worker B leases the
  job, then A wakes up and acks. The fencing token closes that: A's token is older than B's, so
  A's ack is rejected, and downstream stores that check the token reject A's writes too
  (Kleppmann's fencing argument against Redlock).

### D6: Replication: Raft written from scratch (Tier 2)
- **What:** each partition is a Raft group whose log carries queue commands; the state machine
  from D4 applies committed entries.
- **Alternatives:** an existing Raft crate (openraft, raft-rs); primary/backup with a lease;
  leaderless replication; Paxos.
- **Why:** a queue needs a single order of operations per partition (two workers must never
  both lease the same job), which is consensus, and Raft is the consensus protocol designed to be
  explained and implemented. Writing it ourselves is the point of the project: election safety,
  log matching and commit rules are what distributed-systems interviews probe, and only our own
  implementation can be run inside our simulator (M5) with full control over every message.
  Primary/backup without consensus loses acknowledged jobs on failover under partition.

### D7: Partition testing: deterministic simulation plus a history checker
- **What:** M5 runs whole clusters in one thread with a simulated network, disk and clock, all
  driven by one seed, injecting drops, delays, reordering, partitions, crashes and torn writes.
  Every run records a history of client operations and results, and a Jepsen-style checker
  (M9) verifies it: no acknowledged job lost, no job's effect applied twice, linearizable
  operations on small histories. M10 repeats the same checks on real processes behind a
  fault-injecting proxy.
- **Alternatives:** only real processes with Jepsen-style fault injection; only unit tests.
- **Why:** real-process testing finds bugs but cannot reproduce them: the failing interleaving
  depends on thread and network timing. A deterministic simulator turns every failure into a
  seed that replays exactly, and runs far more schedules per minute than real processes. The
  real-process tests in M10 then check that the simulator's model of the world was not lying.
  This is FoundationDB's simulation approach plus Jepsen's checking approach.

### D8: Name and location
- **What:** `spool`, at `~/Projects/spool`. The GitHub repo stays private until Tier 1 (M0–M4) is done.
- **Why:** short, not taken by a well-known queue, and a spool is a thing that holds work until
  it is consumed. Private until the single-node queue is correct and durable, as with the sibling projects.

### D9: Time arrives in every command; the clock only moves forward
- **What:** every command carries `at: Time` (logical milliseconds). The queue's clock becomes
  `max(clock, at)` and the command runs at that time. Before applying the command, every lease
  whose deadline is at or before the clock expires. `tick` advances the clock and does nothing else.
- **Alternatives:** reject a command whose time is earlier than the clock; let the queue read a
  clock; expire leases from a timer.
- **Why:** time inside commands is what D4 requires, and it makes expiry deterministic: two
  replicas applying the same commands expire the same leases at the same point in the stream.
  Clamping instead of rejecting matters once time is stamped by a Raft leader (M7): after a
  failover, the new leader's clock can be slightly behind the old one's, and rejecting its
  commands would make the queue unavailable until its clock caught up. Clamping costs only that
  such commands run a little "late", which no lease rule can observe. The edge sends `tick`
  periodically so leases expire even when no other traffic arrives.

### D10: Broker-assigned job ids, one global counter of fencing tokens
- **What:** `enqueue` does not carry an id; the queue assigns `JobId`s from a counter (1, 2, 3, ...).
  Every lease takes the next `Token` from a second counter shared by all jobs and queues.
- **Alternatives:** client-assigned ids (as in lob's D3); random UUIDs; tokens per job.
- **Why:** the ids come from the state machine, so they are still a pure function of the command
  sequence and replay identically (D4); clients cannot collide or forge an id. Retrying an
  enqueue without creating a duplicate is a separate concern, solved with idempotency keys in M3
  (a client-assigned id would conflate the two). One global token counter means a newer lease
  always has a larger token, even across jobs, so a downstream store can keep a single
  "highest token seen" per resource. Tokens per job would also be correct for the queue itself
  but give downstream stores less to work with.

### D11: Caller-owned output buffer
- **What:** `Queue::apply(&mut self, cmd: &Command, out: &mut Vec<Event>)` appends to `out`.
- **Why:** returning a fresh `Vec` per command allocates on every command. With a reused buffer
  the steady state allocates nothing (measured in M11). Same choice as lob's D5.

### D12: Text command format (REPL, scenario files)
- **What:** one command per line, starting with its time: `@<ms> enqueue <queue> <payload> [delay=<ms>]`,
  `@<ms> lease <queue> <visibility_ms>`, `@<ms> heartbeat <job> <token> <visibility_ms>`,
  `@<ms> ack <job> <token>`, `@<ms> nack <job> <token>`, `@<ms> tick`, plus `configure` (D14) and
  `redrive` (D16). A zero delay is left out when printing. Payload bytes from `!` to
  `~` except `%` stand for themselves, any other byte is `%XX`, and `-` is the empty payload.
  Queue names are 1-64 characters from `[A-Za-z0-9_.-]`. Numbers are plain decimal (no sign).
  Events print as `name key=value ...`. `Display` prints exactly what `FromStr` parses, checked
  by a property test over random commands.
- **Alternatives:** JSON lines; hex payloads; payload as the rest of the line.
- **Why:** scenario files stay readable (`@0 enqueue emails send:42`) while any byte string still
  fits in one whitespace-free token, so every command has exactly one line form. The binary
  wire format (M3) and log format (M2) are separate decisions, where size and decode speed matter.

### D13: Retry backoff: capped exponential with deterministic jitter
- **What:** after the `n`-th lease of a job fails, it waits `d = min(cap, base * 2^(n-1))`, jittered
  into `[d/2, d]` ("equal jitter"). The jitter is a splitmix64 hash of `(job, n)`, not a random number.
  Defaults: base 1 s, cap 5 min. Overflow of the exponential saturates to the cap.
- **Alternatives:** fixed delay; exponential without jitter; full jitter `[0, d]`; a seeded RNG
  inside the queue.
- **Why:** exponential backoff stops a failing dependency from being hammered; jitter stops jobs
  that failed together (a downstream outage fails hundreds at once) from retrying together and
  failing together again (AWS Architecture Blog, "Exponential Backoff and Jitter"). Equal jitter
  instead of full jitter keeps a floor of `d/2`, so a retry is never immediate by bad luck. A hash
  of `(job, attempt)` gives the spread of randomness while staying a pure function of the command
  stream (D4); a seeded RNG would also be deterministic, but its output would depend on the order
  of every earlier retry, so one extra retry anywhere would shift every later delay. The hash
  is pinned by a unit test: changing it changes every retry time, so it must be deliberate.

### D14: Retry policy per queue
- **What:** `@<ms> configure <queue> <max_attempts> <backoff_base_ms> <backoff_cap_ms>` sets a
  queue's policy and creates the queue if needed; `enqueue` creates a queue with defaults
  (5 attempts, 1 s, 5 min). `max_attempts >= 1` and `base <= cap`, otherwise `rejected bad_config`.
  A new policy applies to every later failure, including jobs already in the queue.
- **Alternatives:** policy per job at enqueue time; one global policy; queues declared before use.
- **Why:** retry behaviour usually belongs to the kind of work (a queue), which is how SQS
  (`maxReceiveCount` on the redrive policy) and Celery (per task type) do it. Per-job overrides can
  be added later as enqueue options without changing this. Auto-creating queues keeps producers
  simple; leasing an unknown queue answers `empty` and does not create it.

### D15: Expiry counts as an attempt
- **What:** `attempts` counts leases. A lease that ends without an ack, by nack or by reaching its
  deadline, retries while `attempts < max_attempts` and is dead-lettered otherwise.
- **Alternatives:** count only nacks.
- **Why:** the most dangerous job is the one that crashes its worker (out-of-memory, segfault on a
  malformed input). It never nacks, so counting only nacks would retry it forever and take a
  worker down each time. SQS counts receives for the same reason. The cost: a job whose worker
  merely stalled past the timeout also uses up an attempt, which is why the timeout must be
  generous and kept alive with heartbeats.

### D16: Dead-letter state inside the queue
- **What:** a dead job stays in the job table in a `dead` state with its id and payload, in
  per-queue index `dead`. `@<ms> redrive <queue>` makes every dead job of that queue ready now,
  with attempts reset to 0, in id order.
- **Alternatives:** move dead jobs to a separate named queue (`<queue>.dlq`), as SQS does.
- **Why:** one job table keeps "every job is in exactly one state" a single check, and the id stays
  the same across the whole life of the job, which matters for tracing and, later, for
  idempotency keys. A separate DLQ queue can still be presented on top of this by a future API.

### D17: Delayed jobs and the reference data structures
- **What:** `enqueue ... delay=<ms>` makes a job leasable from `now + delay`. The reference queue
  stores per queue one `BTreeSet<(ready_at, JobId)>` holding every waiting job (delayed, backing
  off, or ready), a `BTreeSet<JobId>` of dead jobs, and globally a `BTreeSet<(deadline, JobId)>`
  of leases and a `HashMap<JobId, Job>` that is only looked up, never iterated on the apply path.
- **Alternatives:** a separate ready list plus a timer wheel that promotes delayed jobs.
- **Why:** with ready and not-yet-ready jobs in one set ordered by `ready_at`, a job becomes
  leasable just by the clock passing its `ready_at`: no promotion step, nothing to forget. Delays,
  backoff and redrive all use the same mechanism. `lease` is O(log n) (first entry, compare,
  pop); expiry pops leases from the front of the deadline set. Ordered sets also make every
  iteration order deterministic (D4). This is the reference, chosen for obvious correctness; a
  faster layout is a later, measured decision.

### D18: Order and the expiry boundary
- **What:** `lease` returns the job with the smallest `(ready_at, id)` whose `ready_at <= now`.
  A lease is expired once `now >= deadline`; expiry runs before the command, oldest deadline first
  (ties by id), and the job fails as of its deadline, so its backoff starts at the deadline. A
  heartbeat, ack or nack that arrives at or after the deadline is refused (`not_leased`, or
  `stale_token` once someone else holds the job) even if nobody re-leased it.
- **Alternatives:** strict FIFO by enqueue order; accept late acks while the job is unclaimed.
- **Why:** best-effort FIFO is what SQS standard queues and most brokers offer; strict order is
  incompatible with retries (a failing job would block everything behind it) and is left to
  per-key ordering in M8. Failing as of the deadline makes the state independent of when the
  expiry was noticed, so extra ticks change only where events appear in the stream, not the
  state (tested). Refusing late acks keeps one rule with no exceptions: after the deadline the
  lease is gone. Accepting them would save some duplicate work but make "is this ack valid?"
  depend on what other workers happened to do.

### D19: Two independent checkers
- **What:** after every command in tests, `check_invariants` verifies the reference queue's
  internals: every job in exactly one state and in the right index, indexes and job table in
  one-to-one correspondence, no lease past its deadline, lease tokens unique and issued, every
  assigned id either live or acked. A `Ledger` rebuilds every job's state from the events alone and
  refuses impossible events (a lease of a job that is not waiting, a non-increasing token, a
  wrong attempt number, a release not followed by a retry or dead-letter in the same command, a
  payload that changes between leases). The two must agree on how many jobs are in each state.
- **Why:** the checker sees internals but trusts them; the ledger sees only output. A bug that
  makes state and events disagree is caught by the comparison. Both run on 11 scenario files and
  on 500 random sequences of up to 300 commands per property, whose generator is checked to reach
  every event and rejection. Unit tests corrupt the state eight ways and feed the ledger eight
  impossible histories to show both actually catch something.

### D20: Scenario files
- **What:** `tests/scenarios/*.txt`: each command line (D12) is followed by one `= <event>` line per
  event it must produce; `#` lines are comments. A command with no `=` lines must produce nothing.
  Both checkers run on every command. The REPL runs one with `run <file>`.
- **Why:** each file reads as a story about one rule (the zombie worker, the poison job, the
  expiry boundary) and doubles as documentation. Expected events were recorded from the queue and
  then checked line by line against the rules, including the jitter bands.

### D21: The write-ahead log holds commands
- **What:** every command is written to the log, including rejected ones and ticks (both move the
  clock and can expire leases). Recovery replays the commands through `apply` (D4) and reaches the
  same state. Events are never stored; they are recomputed.
- **Alternatives:** log the events, or the state changes each command made (redo records).
- **Why:** the command is the smallest complete description of a step, and replay is free because
  the queue is deterministic. It is also exactly what the Raft log will carry in M7, where
  replicas apply committed commands, so the WAL format becomes the replication format. The cost:
  recovery re-executes every command since the last snapshot, and any change to `apply` that
  alters what an old command does is a format change (an old log must replay to the old state).

### D22: Durable before visible
- **What:** a command's events are returned only after its log record is synced. Order inside
  `apply`: apply in memory, append the record, sync, return the events. Nothing reaches a caller
  between the in-memory apply and the sync.
- **Alternatives:** reply first and sync in the background (Redis `appendfsync everysec`).
- **Why:** a worker that saw `acked` or a producer that saw `enqueued` must never find the job
  missing or un-acked after a crash. Applying before writing is safe because nobody can observe the
  new state until the sync succeeds, and if the write fails the handle is poisoned (D25) so the
  in-memory state that ran ahead of the disk is thrown away.

### D23: Log record framing
- **What:** a segment file starts with a 24-byte header (`SPOOLWAL`, format version `u32`, first
  LSN `u64`, CRC-32C of those 20 bytes). Each record is `[len u32][crc u32][lsn u64][body]`,
  little-endian, with CRC-32C over `len`, `lsn` and `body`. LSNs start at 1 and must be
  consecutive across all segments; a segment's file name carries its first LSN.
- **Alternatives:** lsmkv's `[crc][kind][lens][data]` with no sequence number; LevelDB's 32 KiB
  blocks with fragmented records.
- **Why:** the CRC covers `len` as belt and braces: a damaged length moves the end of the record,
  so the CRC over the body would almost surely fail anyway (the M2 mutation pass confirmed no test
  can tell the difference). The LSN ties records to snapshots (replay starts after the
  snapshot's LSN) and catches a missing or duplicated record, which a per-record CRC cannot.
  CRC-32C has hardware support on x86 and ARM. **Known limit (as lsmkv D2):** a corrupted length
  that points past the end of the file looks like a torn tail and is truncated. Fixed-size blocks
  would close that; not worth it before M11 measures the log.

### D24: Binary command encoding
- **What:** a hand-written little-endian codec: `at u64`, a one-byte op tag, then the fields in
  declaration order; queue names as `u8` length + bytes (validated on decode), payloads as `u32`
  length + bytes. Decoding must consume the whole body. The same codec will carry commands in the
  M3 wire protocol.
- **Alternatives:** the D12 text format; serde + bincode.
- **Why:** the format is part of the on-disk contract, so it is written down in one file and pinned
  by a golden-bytes test rather than delegated to a library whose encoding can change between
  versions. Text would work but costs parsing on every replay and needs escaping for payloads.

### D25: Sync policy and poisoning
- **What:** the log API is `append(commands)` then `sync()` (`fdatasync`). `Durable::apply` syncs
  every command; `apply_batch` appends several and syncs once (the group-commit shape; M11 measures
  it). Any storage error poisons the handle: every later call returns `Poisoned`, and the only way
  back is to reopen, which recovers from whatever reached the disk.
- **Alternatives:** sync on a timer (loses the last interval on a crash); retry a failed sync.
- **Why:** after a failed `fsync` the kernel may have dropped the dirty pages and cleared the error
  (PostgreSQL's 2018 "fsyncgate"), so retrying can report success for data that is gone. The
  in-memory state may also be ahead of the disk (D22). Poisoning on every error, not just sync,
  keeps one rule to explain; reopening is the single recovery path and is tested at every byte.

### D26: Torn tails and corruption
- **What:** as lsmkv D2. Reading stops at the first record that is short, has a bad CRC or a length
  past the end. If that record is in the last segment and is followed only by its own bytes or by
  zeros, it is a torn tail: the segment is truncated there and synced before new records go after
  it. A bad record with non-zero data after it, a bad record in an earlier segment, an LSN gap or a
  body that fails to decode is `Corruption` and the queue refuses to open.
- **Why:** a crash can only tear the end of what was being written; anything else is disk damage or
  a bug, and starting anyway would silently drop acknowledged commands. Truncating before appending
  matters because new records written after garbage would be lost on the next recovery.
  **Known limit:** damage to the very last record of the log looks exactly like a torn write and
  is truncated, so that one synced command is lost silently (RocksDB's default
  `kTolerateCorruptedTailRecords` makes the same trade). A segment header that fails its CRC with
  nothing after it is a crash during segment creation; with records after it, it is corruption,
  because records are only appended once the header is synced.

### D27: Snapshots
- **What:** a snapshot is a full, canonical dump of the queue state: clock, id and token counters,
  acked count, every queue's config in name order, every live job in id order (the indexes are
  rebuilt from the jobs and then checked with `check_invariants`). File `snap-<lsn>`: `SPOOLSNP`,
  version, the LSN of the last command it includes, body length, CRC-32C over header and body.
  Written as `snap-<lsn>.tmp`, synced, renamed, directory synced. Then a new log segment starts at
  `lsn + 1` and older snapshots and segments are deleted. Taken every `snapshot_every` commands
  (a count, never a timer, so it is deterministic).
- **Alternatives:** no snapshots (replay the whole log forever); incremental or copy-on-write
  snapshots; a snapshot timer.
- **Why:** recovery time and disk use stay bounded by `snapshot_every`. A full dump is the simplest
  thing that is obviously correct at M2 sizes; incremental snapshots come with Raft log compaction
  (Tier 3). Because the encoding is canonical, two queues are in the same state exactly when their
  snapshots are byte-equal, which the crash tests use as their equality check. The `HashMap` of
  jobs is sorted by id before writing, so its iteration order never reaches the file (D4).

### D28: Storage seam and crash testing
- **What:** all file access goes through a `Storage` trait (list, read, create, append, sync,
  truncate, rename, remove, sync the directory). `FileStorage` is the real one. `MemStorage` keeps
  synced and unsynced bytes apart, and directory changes apart from the synced directory. Its crash
  images are: the synced directory or the current one, with every file at its synced length, plus
  for each file with unsynced bytes, every prefix of them and an all-zero tail of the same length.
  `fail_after(n)` makes the `n+1`-th mutating call fail (an append that fails still leaves its bytes
  unsynced, so a partial write is one of the images). The harness runs a workload, fails it at
  every storage call, recovers every crash image, and checks: recovery succeeds; the state equals a
  replay of `k` commands where every command whose events were returned is included and nothing
  beyond the failed command is; the invariant checker passes; the log accepts new commands that
  survive the next reopen; and a second crash during recovery, at every call, recovers to the same
  state as the first recovery.
- **Alternatives:** real files and `kill -9` only; a fault-injection crate (`failpoints`).
- **Why:** killing a real process only tests the crash points the timing happens to hit. The seam
  makes every point reachable and every run repeatable, and it is the disk the M5 simulator will
  plug in. Run: three generated workloads of 40 commands with keyed and unkeyed enqueues, completes
  and result queries (snapshot every 6) and the 11 scenario files back to back (snapshot every 25),
  single commands and batches mixed: 636 failure points, 43,525 crash images, 169,015 second
  crashes inside recovery, about 15 s in a debug build. The
  harness asserts it reached unacknowledged commands that survived, unsynced commands that were
  lost, torn tails and recovery from a snapshot. Its model is stated, not hidden: it does not produce garbage other than zeros inside
  unsynced data, or reorder writes within a file; the CRC covers the first and M5 can add the second.

### D29: Recovery and directory layout
- **What:** a data directory holds `snap-<lsn>`, `wal-<first lsn>` segments (LSNs zero-padded to 20
  digits so names sort) and `LOCK`, held with an exclusive `flock` so only one process opens it.
  Recovery: delete `*.tmp`; load the newest snapshot (none means an empty queue at LSN 0); find the
  segment containing the next LSN and ignore older ones (left over from a crash before cleanup);
  replay records after the snapshot's LSN, checking continuity; truncate a torn tail; reopen the
  last segment for appending, or start a new one. `Durable<S, Q>` wraps any state machine with a
  snapshot codec; its `apply` returns `Result<&[Event], StoreError>`.
- **Alternatives:** one ever-growing log file; no lock; `Durable` implementing the `Queue` trait.
- **Why:** segments let a snapshot free disk by deleting whole files instead of rewriting a log.
  The lock closes the gap lsmkv D7 left open: two processes appending to one log interleave
  records and corrupt it. `Durable` cannot implement `Queue` because `Queue::apply` has no way to
  report a disk error, and swallowing it would break D22.

### D30: Server structure
- **What:** tokio handles the sockets. One dedicated OS thread owns the `Durable` queue and takes
  requests from a bounded channel. It blocks for the first request, then drains whatever else is
  already queued (up to 256 requests) and applies them all with one `apply_batch`: one append and
  one sync for the whole batch, which is group commit. Replies go back on per-request oneshot
  channels after the sync.
- **Alternatives:** threads with the queue behind a mutex; the queue inside an async task.
- **Why:** one owner means no locks and one total order of commands, which is the order the log
  holds and the order Raft will impose in M7. `fdatasync` blocks for milliseconds; on its own
  thread it never stalls the reactor that serves the sockets, and while it runs the next batch
  builds up in the channel, so the sync cost is shared by every client waiting at that moment.
  A mutex would give the same order but one sync per request unless batching is rebuilt on top of
  it; an async task would block a runtime worker on every sync.
- **Numbers** (`examples/group_commit.rs`, release build, 64 clients each enqueueing 100 jobs one
  at a time, Intel 660p NVMe under btrfs, three runs): one command per sync, 1,054–1,167
  enqueues/s; batches of up to 256, 23,758–31,820 enqueues/s, with about 32 commands per sync.
  Roughly 25 times the throughput, entirely from sharing the sync. M11 measures latency.

### D31: Time source
- **What:** the server's core thread stamps every command with wall-clock milliseconds since the
  Unix epoch, read once per batch. This is the only clock read in spool; the core still never reads
  one (D4). Clients never send a time: the wire carries ops, not commands. A clock that steps back
  (NTP, a restart on another machine) is clamped by D9. There are no logged periodic ticks: expiry is
  lazy and happens at the next command that reaches the queue. The clock is a parameter of the
  server so tests can drive it by hand.
- **Alternatives:** client-sent time; a monotonic `Instant`; a timer that logs a tick every second.
- **Why:** epoch time keeps rising across restarts, so a recovered queue's clock and the new
  process's stamps line up; an `Instant` restarts from an arbitrary origin in every process. A
  client clock would let one skewed worker expire everyone's leases. Ticks would make an idle queue
  write to its log forever; lazy expiry is correct because a lease that expired unnoticed is
  indistinguishable from one noticed late: D18 fails it as of its deadline either way.

### D32: Frames
- **What:** every frame is `[len u32][kind u8][request id u64][body]`, little-endian, `len`
  counting everything after itself. Frames over 1 MiB are refused and the connection closed. The
  client's first frame must be `hello` with protocol version 1; the server answers `hello_ok` or an
  error and closes. Then each `request` frame carries one op (the D24 op encoding, without `at`).
  Requests may be pipelined; replies come back in request order and echo the request id, which the
  client checks. Replies are `events` (the events of that command) or `error` (code and message,
  then close).
- **Alternatives:** gRPC/protobuf; HTTP + JSON; a RESP-style text protocol.
- **Why:** the op codec already exists and is pinned by golden bytes, so the wire adds a header
  and nothing else. A length prefix lets either side read a whole frame before decoding it, and the
  cap stops a bad length from allocating gigabytes. In-order replies follow from the single core
  thread and need no reordering; the echoed id turns a framing bug into an error instead of a reply
  delivered to the wrong caller. gRPC would bring code generation, HTTP/2 and a large dependency
  tree for six message types.

### D33: Reply encoding
- **What:** a binary codec for `Event` in the D24 style: a one-byte tag, fields in declaration
  order, decoding must consume the whole body. Pinned by golden bytes and a round-trip property over
  generated events. A reply carries every event its command produced, including expiry events of
  other jobs that the command triggered; the client library picks out the ones about its own op.
- **Alternatives:** send the D12 text form; filter the events per client on the server.
- **Why:** the same reasons as D24, and the events are already the exact result of the command
  (D4). Filtering would need the server to know which events belong to which op, which is the
  core's business; a few bytes of other jobs' expiry are cheaper than that coupling.

### D34: Waiting for work
- **What:** `lease` never blocks: it returns `Empty` at once. The worker library retries an empty
  lease after a jittered delay that starts at 10 ms and doubles to a cap of 1 s, and resets after it
  gets a job.
- **Alternatives:** long polling (the server parks the lease until a job is ready or a timeout).
- **Why:** a parked lease is state outside the queue: it must be woken by the right enqueue, survive
  nothing across a restart, and later be forwarded to a Raft leader. With D4's pure core, that state
  would live in the server and be untested by everything built so far. Polling costs at most one
  wasted request per idle worker per second. Long polling is a Tier 3 item, measured against this.

### D35: Deduplication keys
- **What:** `enqueue` takes an optional key: 1 to 128 visible ASCII characters. The queue remembers
  `(queue, key) -> job` for 5 minutes of logical time from the first enqueue (a fixed window, like
  SQS FIFO deduplication), whether or not the job has since been acked. A repeat inside the window
  enqueues nothing and returns a `Deduplicated` event with the original job id; the repeat's payload
  and delay are ignored. Entries expire at the start of every command, at `enqueued_at + 5 min`
  (exclusive, as leases in D18). The table is part of the state machine: logged with the command,
  rebuilt on replay, in the snapshot, replicated in M7. On the wire and in the log a keyed enqueue
  is a new op tag 9 and an unkeyed one keeps tag 1, so every existing log decodes unchanged.
  Snapshots move to version 2, which appends the table sorted by `(queue, key)`; version 1 still
  loads (as an empty table). The invariant checker checks the table and its expiry index; the
  ledger checks that a `Deduplicated` names the job last enqueued with that key.
- **Alternatives:** a dedup cache in server memory (lost on restart and invisible to replicas);
  client-side dedup only; keys that live as long as the job.
- **Why:** the producer's problem is a lost reply: it enqueued, the connection dropped, and it
  cannot tell whether the job exists. Retrying with the same key is safe only if the queue answers
  the same way after a crash or a failover, so the table must be in the replicated state, not in a
  server cache. Keeping the entry after the ack matters: the retry may arrive after a fast worker
  finished the job, and a second job then would be a duplicate effect. The window bounds memory;
  5 minutes is SQS's choice and longer than any client retry loop should run. Expiry measured in
  logical time keeps replay deterministic (D4).

### D36: Worker library
- **What:** an async `Client` with one connection shared by clones: a writer task assigns request
  ids and a reader task matches replies to callers in order, so concurrent calls pipeline. Methods
  `enqueue`, `lease`, `heartbeat`, `ack`, `nack` return typed results; a rejection is an error with
  its `RejectReason`. `Worker::run(queue, handler)` loops: lease (D34), run the handler while
  heartbeating every visibility/3, ack on `Ok`, nack on `Err`. If a heartbeat is rejected, the
  lease is gone (expired and probably leased by someone else), so the handler's future is dropped
  and nothing is acked.
- **Alternatives:** a bare client only; a blocking client; one connection per call.
- **Why:** the heartbeat must run while the handler does, on the same connection, which is what the
  shared, pipelined client gives. Dropping the handler on a lost lease is the cheapest guard against
  the zombie worker (D5) inside the process; it cannot undo effects already made, which is what
  fencing tokens checked downstream are for (M4). Heartbeating at a third of the visibility leaves
  two heartbeats' worth of slack before the lease can expire.

### D37: Backpressure
- **What:** the channel to the core thread holds 1024 requests; each connection may have 128
  requests in flight (sent and not yet answered). A connection that hits either limit stops reading
  its socket until a slot frees, so TCP flow control pushes back on the client.
- **Alternatives:** unbounded queues; a "busy" error reply.
- **Why:** unbounded queues turn overload into memory growth and latency without limit. A busy
  reply moves the retry loop into every client. Not reading is the standard TCP answer and needs no
  protocol: a pipelining client simply sees its writes slow down. The per-connection cap stops one
  client from filling the shared channel ahead of everyone else.

### D38: Disk errors in the server
- **What:** when `Durable` returns an error, the core thread stops; every pending and later
  request fails (the client sees the connection close), and `spool serve` exits non-zero. A restart
  runs recovery (D29).
- **Alternatives:** a read-only mode; retrying the failed write.
- **Why:** after a failed write or sync the handle is poisoned (D25) and the in-memory state may be
  ahead of the disk, so there is nothing safe to serve, not even reads. Crash-only: the one recovery
  path is the one the crash harness tests at every storage call.

### D39: Tests for the network layer
- **What:** servers run in-process on `127.0.0.1:0` with a temporary data directory and a clock the
  test controls, and with both checkers (D19) run by the core thread after every batch. Tested:
  frame and event codecs (golden bytes, round-trip property, every truncation); the handshake and
  protocol errors; pipelined requests answered in order; concurrent workers where no job is ever
  leased twice at once and every job is acked exactly once; dedup across a server restart; lease
  expiry by moving the test clock; the zombie worker stopped by a rejected heartbeat; backpressure
  that does not deadlock a client pipelining far past the caps. Worker-crash and fencing tests are
  M4.
- **Alternatives:** test only against an external `spool serve` process.
- **Why:** in-process servers are fast, need no ports or cleanup, and can run the checkers inside
  the server, so every end-to-end test is also an invariant test. The binary itself is exercised by
  one smoke test.

### D40: Where "effectively once" holds
- **What:** spool promises at-least-once delivery everywhere and exactly-once *effect* inside two
  stated boundaries. (1) Effects on state the queue owns: a job's result, recorded by a
  transactional complete (D41) that lands exactly once per job. (2) Effects on an external store
  that checks fencing tokens (D43) and keys each job's effect by the job, so a retry overwrites
  instead of adding and a zombie's late write is refused. An effect outside both (an email, a
  card charge through an API without idempotency keys) can happen more than once, and the docs
  say so.
- **Alternatives:** claim exactly-once without a boundary; offer only boundary (1).
- **Why:** a worker that dies after its side effect and before its ack is indistinguishable from
  one that died before the effect (D3), so no queue can make an arbitrary effect exactly-once.
  What it can do is make its own state transitions atomic and hand workers a token that external
  stores can check. Naming the boundary is what makes the claim testable (D46) and defensible.

### D41: Transactional complete
- **What:** a new op `complete <job> <token> <result>` checks the lease exactly as ack does, then
  removes the job and stores `result` for it, in one command and so in one log record. Event:
  `Completed { job, token }`. `result <job>` returns `Result { job, status }`, where the status is
  `pending` (the job is still in the queue), `done` with the completing token and the payload, or
  `unknown` (never existed, acked without a result, or the result's window ended). Results are kept
  for 5 minutes of logical time after completion, expiring at the start of every command like
  dedup keys (D35). A completed job counts as acked in every count and check.
- **Alternatives:** ack, then a separate "store result" command; results in an external database.
- **Why:** with two commands a crash between them leaves an acked job with no result or a result
  for a job that will run again. One command is one record (D21), so recovery replays both halves
  or neither. A producer that enqueued with a dedup key and lost the reply can retry the enqueue,
  get the job id back (D35) and fetch the result, with the same 5-minute window on both sides.

### D42: Complete is idempotent per lease
- **What:** a `complete` for a job that is already completed, with the same token, while its
  result is kept, answers `Completed` again and changes nothing (the result sent the second time
  is ignored). A different token, or a job acked without a result, is rejected as before.
- **Alternatives:** reject the retry with `unknown_job`, as a repeated ack is.
- **Why:** the worker's problem is a lost reply: it sent `complete`, the connection dropped, and
  it cannot tell whether the queue recorded it. With this rule it simply retries; the answer is
  `Completed` whether the first attempt landed or not, and only the lease that won can get that
  answer. Plain `ack` keeps its M1 behaviour because it stores nothing to compare against.

### D43: Fencing downstream
- **What:** the worker's handler receives the lease's token. `spool::fence::FencedStore` is a
  reference store: `write(key, token, value)` succeeds only if `token` is at least the highest
  token accepted for `key` (equal, so one lease can rewrite its own value), and remembers it. A
  real store does the same with a conditional write, e.g. `UPDATE ... SET value = $v, token = $t
  WHERE key = $k AND token <= $t`.
- **Alternatives:** trust that a worker stops before its lease ends; per-job tokens.
- **Why:** a paused worker cannot know it was paused, so only the store can refuse its late
  write, and only by comparing something the queue issued. Tokens come from one global counter
  (D10), so the same fence works for a resource that several different jobs write, which per-job
  tokens could not order.

### D44: Worker API for results and tokens
- **What:** a handler returns `Result<T, E>` with `T: Into<Completion>`: `()` means ack, a
  `Payload` means complete with that result (D41). If the complete's reply is lost, the worker
  retries it once on a new connection (D42). The handler gets the whole lease, token included.
- **Alternatives:** separate `run_ack` and `run_complete` loops; result passed through a channel.
- **Why:** one loop keeps the heartbeat, cancellation and backoff logic in one place, and M3
  handlers that return `Ok(())` keep compiling.

### D45: Formats for M4
- **What:** op tags 10 (`complete`) and 11 (`result`), event tags 13 (`completed`) and 14
  (`result`, status 0 pending, 1 done with token and payload, 2 unknown). Snapshot version 3
  appends the results table (`job token payload expires_at`, by job id); versions 1 and 2 still
  load with no results. The log format is unchanged.
- **Why:** the same rule as D35: new tags and a new snapshot version, never a changed meaning for
  bytes already on disk.

### D46: Worker-crash tests
- **What:** a seeded test runs workers against a checked in-process server (D39) and kills them
  at chosen points: before the effect, after the effect and before the complete, and with the
  complete sent but its connection dropped before the reply. Others stall past their deadline and
  come back as zombies. The test clock expires leases. Effects go to a `FencedStore` keyed by job.
  Checks: every job is completed exactly once in the queue, with the result of the completing
  lease; in the store, each job's value was written by the completing token; no write with a
  token below one already accepted for that key ever succeeded; and the server's checkers pass
  after every batch.
- **Alternatives:** kill real worker processes.
- **Why:** from the server's side a dead worker is a connection that stops, which closing the
  socket produces exactly. One scheduler drives every step and waits for each lost-reply complete
  to land before going on, so a seed replays the same schedule every time (three runs print the
  same coverage), the property the M5 simulator generalises.
- **Run:** 200 seeds of 12 jobs, about 1.7 s in a debug build: 743 crashes before the effect, 696
  after it, 734 lost replies, 669 cut requests, 723 zombies (144 of their writes refused by the
  fence, 403 of their completes refused by the queue, 320 accepted because their lease was still
  current), 677 normal completions. The test asserts every one of these cases was reached.

### D47: Simulator shape: sans-IO processes on a discrete-event loop
- **What:** `spool::sim` runs a whole system in one thread. Every participant is a `Process<M>`
  with three entry points (start, a message arrived, a timer fired) and no other way to see the
  world: a `Ctx` gives it its node's clock, the simulator's random numbers, `send` and
  `set_timer`. The `World` keeps a queue of pending deliveries and timers ordered by `(time,
  sequence number)` and executes them one at a time; nothing runs between events, so the order of
  events is the whole schedule. The server's batch logic moves out of the core thread into
  `server::Core` (apply a batch at one time, run the checkers, count), which the real core thread
  and the simulated server node both call.
- **Alternatives:** a deterministic async executor that runs the real tokio code (madsim style,
  or writing our own); the `turmoil` crate; real threads under a controlling scheduler.
- **Why:** Raft (M6) has to be a state machine of messages and timers to be tested at all, so the
  simulator is built for that shape now, and the queue core already is one (D4). An event loop
  needs no executor, no crate and no `unsafe`, and makes every interleaving an explicit, printable
  event. The cost is stated: the tokio plumbing (sockets, tasks, backpressure) and the real
  `Client` and `Worker` are not inside the simulation; the simulated server shares `Core`,
  `Durable`, the codecs and the queue with the real one, and the simulated worker follows the
  same protocol as `Worker` (D36, D44). The tokio layer is covered by D39/D46 and again by M10.

### D48: One seed, our own generator, integer probabilities
- **What:** a run is a function of one `u64` seed. The generator is SplitMix64 (64-bit state, one
  add and three xor-shift-multiplies per number). Probabilities are integers in parts per million
  and ranges are drawn with `below(n)`; no floating point decides anything. Every run keeps a
  64-bit FNV-1a hash of its trace (each executed event: time, kind, nodes, message digest); two
  runs of a seed must give the same hash, and a test checks it.
- **Alternatives:** the `rand` crate; a generator per process; floats for probabilities.
- **Why:** a seed is only a bug report if the whole run follows from it, so the generator must not
  change with a dependency update, and SplitMix64 is ten lines with good statistical quality for
  this. One generator drawn in event order is deterministic because the event order is. Integer
  probabilities avoid any argument about float behaviour across platforms. The trace hash turns
  "deterministic" from a belief into a checked property.

### D49: Network model: datagrams with loss, delay, duplication and partitions
- **What:** a message from node `a` to node `b` is delivered at `now + delay` or dropped. Each
  message independently: dropped with probability `drop`, duplicated with probability `dup` (the
  copy gets its own delay), delayed by a uniform base delay and, with probability `spike`, by up
  to a long spike (seconds). Reordering follows from independent delays. A partition is a set of
  cut directed links, checked at send and at delivery; healing removes the cut. Messages to a node
  that is down or crashed are lost. In the queue world (D51) a queue message is one protocol frame
  (D32), encoded and decoded with the real codec; there is no connection and no hello.
- **Alternatives:** model TCP connections (ordered, reliable until reset); fixed delays.
- **Why:** Raft must survive loss, duplication and reordering, so the network provides all three.
  For the queue, a lost TCP connection means requests and replies vanish and the client retries
  on a new connection, which looks to the server exactly like datagrams lost and duplicated, so
  the datagram model covers it and adds reordering, which is stricter. Checking cuts again at
  delivery means a partition also kills messages already in flight.

### D50: Disk model: crash images and failing calls
- **What:** each node's disk is a `SimDisk` (a shared `MemStorage`, D28) owned by the world and
  surviving the node's crashes. A crash replaces the disk with one crash image chosen by the seed:
  synced or unsynced directory, a prefix or zero tail of an unsynced write (D28). A torn write is
  a crash in the middle of a batch: the disk is told to fail at a chosen mutating call of the next
  batch, the batch fails, the server stops (D38), and the crash image is taken from that state, so
  half-written records and half-written snapshots happen. The node restarts after a downtime and
  recovers (D29).
- **Alternatives:** whole files lost or garbage written; reordered writes within a file.
- **Why:** this reuses the crash model the M2 harness already proves exhaustively, now at random
  points of a live workload with clients waiting on replies. Garbage beyond zeros and reordered
  writes stay outside the model, as D28 says.

### D51: What M5 simulates before Raft exists
- **What:** one queue server node, workers, producers and an external store, all processes:
  - the **server** wraps `Core` over a `SimDisk`. Requests wait while a batch is syncing; the sync
    takes 1–5 ms of simulated time, and everything that arrived meanwhile is the next batch, so
    group commit (D30) happens by itself. Its clock is simulated time plus an offset faults move.
  - **producers** enqueue a fixed list of jobs, each with a dedup key (D35), retrying on timeout
    until they get the job id.
  - **workers** follow D36/D44: lease, heartbeat every visibility/3, drop the job on a rejected
    heartbeat, write the effect to the store with the lease token and wait for its ack, then
    `complete` with the effect as the result, retrying `complete` with the same token on timeout
    (D42); idle backoff with jitter.
  - the **store** is a `FencedStore` (D43) over the network, so a delayed write is a zombie write.
- **Alternatives:** wait for M6 and only simulate Raft; simulate the queue without a store.
- **Why:** the single-node queue already has the properties M5 must check end to end (durable
  replies, dedup across retries and crashes, effectively-once effects), so the simulator proves
  itself on them before Raft adds a second source of bugs. A store across the network is what
  makes zombies natural instead of scripted: any delayed write is one.

### D52: Faults, chosen per seed (swarm testing)
- **What:** each seed first picks which faults are on and how strong: drop up to 20%, duplication
  up to 5%, delay spikes, partitions (one node cut off for up to 3 s, then healed), crashes of
  workers, producers and the server with restarts after up to 2 s, torn writes on the server,
  pauses (a node frozen for up to three visibility timeouts, its messages and timers held until it
  wakes), and server clock jumps forward (expiring leases early) and back (clamped by D9). Faults
  are injected for the first 30 s of simulated time; then the network heals, faults stop, and the
  run must finish within a deadline.
- **Alternatives:** every fault on at fixed rates in every run.
- **Why:** with everything on at once, some bugs need a quiet stretch to show (a fault that only
  matters if nothing else interrupts it), and others hide behind more common faults. Varying the
  mix per seed explores both, which is the swarm-testing result (Groce et al.) and what
  FoundationDB does. The quiet phase turns liveness into a check: a system that is correct but
  stuck fails the run.

### D53: What a run checks
- **What:** during the run: after every server batch, both checkers (D19) with the ledger resumed
  after each recovery; at every recovery, that the recovered log length `k` is at least the last
  replied command and at most the last attempted one, and that the recovered state equals a fresh
  replay of the first `k` commands of the server's history (the D28 rule, live). At the end, on a
  queue recovered from the server's disk: every key a producer enqueued maps to one job and no
  other job exists (dedup across retries and crashes); every job is completed, none dead or still
  queued; each job's result is the value the store holds for it, and that value was written by
  the completing token (D40, D46); per key the store's accepted tokens never went down; and the
  run finished before the deadline. A failure panics with the seed and the command that replays
  it with a trace.
- **Alternatives:** only end-of-run checks; a full history checker now.
- **Why:** checks at every batch and every recovery point at the moment a bug happens instead of
  its consequence later. The Jepsen-style history checker over client-observed operations is M9;
  these checks use the server's own view plus the store, which is enough for one node.

### D54: Planted bugs
- **What:** the queue world takes a `Bug` switch that breaks one thing on purpose: `NoFence` (the
  store accepts any token) or `NoDedupKey` (producers retry without a key). A test runs seeds with
  each bug until one fails and asserts it fails within a bound and with the expected check.
- **Alternatives:** rely on mutation passes alone.
- **Why:** a simulator that never fails proves nothing; one that finds a known bug within a few
  seeds shows its faults reach the cases that matter. Mutation passes still run on the real code;
  the planted bugs are the always-on version for the two properties a reader asks about first.

### D55: Running and replaying seeds
- **What:** `tests/sim.rs` runs a fixed range of seeds and asserts every fault and every
  interesting outcome was reached (coverage counters, as D46). `SPOOL_SIM_SEEDS=a..b` runs another
  range. `spool sim --seed N [--trace]` replays one seed and prints its events; `spool sim
  --seeds a..b` sweeps a range and prints the first failing seed.
- **Why:** the fixed range keeps `cargo test` fast and repeatable; the sweep finds new failures;
  the replay is how a failure is debugged, since the same seed gives the same events every time.
- **Run:** `cargo test` runs 200 seeds (2 producers of 15 jobs, 3 workers) in about 6 s in a debug
  build. A release sweep of seeds 0..100,000 passed in 568 s on this laptop (about 5.7 ms a seed),
  reaching 788,707 server recoveries (687,677 from a snapshot, 397,991 that cut a torn tail,
  411,542 that lost unanswered commands, 22,158 that kept commands whose batch failed before its
  reply, 14,432 crashes during recovery), 21,533 zombie writes refused by the fence, 319,384
  completes answered after a resend, 532,814 expired leases and batches of up to 134 requests.
  The planted bugs fail at seed 3 (`NoFence`) and seed 0 (`NoDedupKey`).
- **Mutation pass:** 15 mutants, all killed. Of the six in the queue, log and fence, the simulator's
  tests kill five on their own: a reply before the sync, dedup keys never recorded, leases that
  never expire (as a liveness failure), results never stored and missing fencing. It does not kill
  "a repeated complete by the same lease is rejected" (D42): in this world that only changes what
  the worker is told, not any effect, and the unit, scenario and worker-crash tests kill it.
  Removing heartbeats is caught only by a coverage assertion, not by a correctness check: with
  fencing, heartbeats are for liveness and wasted work, not for safety, which is the point of D43.

### D56: Raft shape: a pure node driven by inputs and outputs
- **What:** `spool::raft::Node` is a state machine with no I/O. It is told that a message arrived
  (`receive`), that its election timer fired (`election_timeout`), that the heartbeat interval
  passed (`heartbeat`), or that a client proposes data (`propose`); after each call `take_ready`
  hands the driver a `Ready`: records to persist, messages to send, newly committed entries to
  apply, and whether to restart the election timer. The driver persists and syncs before it sends
  anything from the same `Ready`. In the simulator a thin adapter (`sim::raft::Server`) makes the
  node a `Process` over a `RaftLog` on its `SimDisk`; in M7 and M10 the real server drives the same
  node.
- **Alternatives:** write Raft directly as a simulator process; adopt an existing Raft crate.
- **Why:** the queue core (D4) and `server::Core` (D47) already work this way, and it is what lets
  one implementation run both in the simulator and in the real server. A node that cannot do I/O
  cannot depend on timing or interleavings the simulator does not control. This is the etcd
  "ready" style, written from the paper rather than taken from a crate, because the point of M6
  is to own every line of it.

### D57: Raft features in M6
- **What:** leader election, log replication, the commit rule (D61), persistence of term, vote and
  log, plus PreVote and CheckQuorum. PreVote: a node whose election timer fired first asks whether
  it could win at its term plus one, without changing any term; a node grants that only if its own
  timer has fired since it last heard from a leader, and the log is up to date. CheckQuorum: a
  leader that has not heard from a majority within an election timeout steps down. Pre-candidates
  and candidates resend their requests every heartbeat interval to peers that have not granted,
  since the network loses messages (the paper's RPCs are retried until answered).
- **Alternatives:** core Raft only; core Raft with snapshots and membership changes now.
- **Why:** without PreVote, a node isolated by a partition keeps raising its term and forces a
  needless election when it rejoins; without CheckQuorum, a leader cut off from the majority keeps
  accepting proposals it can never commit. Both turn swarm partitions into liveness failures that
  look like bugs. Log compaction (snapshots) and membership changes are Tier 3; linearizable reads
  (ReadIndex) belong to the queue on Raft (M7).

### D58: Raft's durable state: one write-ahead log
- **What:** each node keeps one log file in the `wal.rs` format (length + CRC records, D26) with
  three record types: `HardState { term, vote }`, `Append { index, entries }` and
  `Truncate { from }`. Recovery replays the file to rebuild term, vote and log. A torn tail is cut
  as in D26. The rule: a `HardState` or `Append` is synced before any message that depends on it
  is sent (a vote, a successful append reply, a leader's own count of itself).
- **Alternatives:** a separate hard-state file replaced atomically by rename, next to a log file.
- **Why:** one file reuses the record format and the crash-image testing already proven in M2 and
  M5, and recovery has one place to read. A rename-swapped file adds a second durability path with
  its own crash cases for two integers.

### D59: Election timeouts come from the driver
- **What:** the node asks for its election timer to be restarted; the driver picks the timeout
  and later calls `election_timeout`. The simulator draws it uniformly from 150–300 ms with the
  world's generator; the real server uses the OS. The driver calls `heartbeat` every 50 ms.
- **Alternatives:** the node draws its own random timeout.
- **Why:** randomness is what breaks split votes, but D4 keeps it out of the core. Moving the draw
  to the driver keeps the node deterministic for a given sequence of inputs, which makes unit
  tests of the node exact.

### D60: Replication: conflict hints and bounded appends
- **What:** a follower that rejects an append replies with a hint `(conflict_term,
  first_index_of_that_term)` (or its log length if too short); the leader jumps `next_index` back
  past the whole term in one step. An append carries at most 64 entries. A reply starts another
  append only if it made progress: a success that raised the follower's matched index while more
  entries wait, or a rejection that moved `next_index` back. Heartbeats resend whatever was lost.
- **Alternatives:** step `next_index` back by one entry per rejection (the basic paper rule).
- **Why:** a follower that missed a long stretch catches up in about one round trip per
  conflicting term instead of one per entry, which matters under message loss. The cap keeps a
  message small enough for the datagram model (D49). Following up only on progress came from the
  simulator: a first version answered every reply with another append, so each heartbeat started
  a new chain of appends that never ended while a follower stayed behind; under a planted bug a
  run grew past 8 GB. Now every chain ends within the length of the log.

### D61: The commit rule and the leader's no-op
- **What:** a leader advances the commit index only to an entry of its current term stored on a
  majority; earlier entries commit with it. A new leader appends an empty entry of its term at
  once.
- **Why:** an entry from an older term can sit on a majority and still be overwritten by a later
  leader (Figure 8 of the Raft paper), so counting replicas is only safe for current-term entries.
  The no-op lets a new leader commit everything before it without waiting for a client. A planted
  bug (D64) breaks this rule.

### D62: What a Raft run checks
- **What:** live, as it happens: at most one leader per term (election safety); for every
  `(index, term)` any node writes, the same data and the same term before it (log matching, checked
  by induction on the entry before); each new leader holds every entry committed in an earlier term
  (leader completeness); every node applies index after index, and what any node applies at an
  index is what every node applied there (state machine safety). One global ledger of committed
  entries, with the term each was first applied in, survives node crashes, so a committed entry
  lost by recovery fails the run. A client's acknowledged operation must be in the ledger at the
  index it was told. At the end, after faults heal: some node leads, every node has applied the
  whole ledger, and every client is done, within 30 s.
- **Leader completeness, precisely:** a candidate held up by a pause or slow votes can win a term
  after a later term has already committed entries, and it need not hold those. The checker first
  flagged exactly this (seed 195); it now requires only entries whose first apply happened in a
  term below the new leader's, which is sound because the first apply is never earlier than the
  commit.
- **Why:** these are the safety properties of Figure 3 of the paper plus durability and liveness;
  checking them at every event points at the step that broke them.

### D63: Cluster sizes and faults
- **What:** each seed picks a cluster of 3 or 5 nodes and a swarm mix (D52): drop, duplication,
  delay spikes, partitions (one node isolated), one-way cuts (one directed link), pauses, crashes
  with crash images, torn writes (failing disk calls), and crash-after-sync (the node dies right
  after its next step that synced, once its messages have left: the persist-then-send boundary of
  D58). Half of node faults pick the leader of the highest term. A node whose disk call fails
  halts, like the server (D38); half the crashes and every halt may come back within 10–200 ms, as
  a supervised process does, so a node can rejoin the election it crashed in.
- **Run:** `cargo test` runs 300 seeds (3 clients of 20 operations) in about 8 s in a debug build.
  A release sweep of seeds 0..100,000 passed in 306 s on this laptop (about 3 ms a seed): 6,000,000
  acknowledged operations, 7,965,350 committed entries, 1,260,170 elections won out of 1,695,561
  started, 14,260,496 pre-votes refused because a leader was alive, 225,977 CheckQuorum
  step-downs, 1,918,276 higher-term step-downs, 118,507 truncated conflicts, 188,382 conflict-hint
  jumps, full 64-entry appends, 1,256,590 recoveries (370,253 cutting a torn tail, 5,106 crashing
  again during recovery), 430,411 crashes right after a sync and 15,891 proposals replaced by
  another leader's entry.
- **Why:** a paused leader that wakes with a stale term, a one-way cut that hides a leader from
  one follower, and a crash between persisting and sending are the cases Raft gets wrong in
  practice; one-way cuts are what exercise PreVote and CheckQuorum.

### D64: Planted bugs for Raft
- **What:** `vote-not-persisted`, `commit-old-term` (no no-op and old-term replicas counted: both
  halves of D61 gone), `no-log-truncate` (a follower keeps conflicting entries after an append)
  and `stale-term-accept` (a node takes appends from an older term).
- **Why:** each breaks a different safety argument, as D54 does for the queue; mutation passes
  cover the rest of the code.
- **What the simulator finds, and what it does not:** `no-log-truncate` and `stale-term-accept`
  fail at seed 0 and `tests/raft_sim.rs` asserts it. The other two need a precise schedule that
  random faults rarely build. `commit-old-term` needs Figure 8: an old-term entry committed while
  no client wrote in that term, then its leader lost and a node with a later-term entry at that
  index elected; 18 of 30,000 seeds found it, the first at 6,038 (with only the counting half of
  the bug and the no-op kept, none did: the no-op is sent with the old entries, so a majority
  acknowledges both at once). `vote-not-persisted` needs two candidates in one term and the voter
  dead and back between their requests; PreVote makes split candidacies rare, and 0 of 30,000
  seeds found it (two earlier versions of the world found it once each, near seed 15,000 and
  20,000). Both are pinned instead by scripted unit tests on the node: Figure 8 in
  `an_entry_of_an_older_term_commits_only_with_one_of_the_current_term`, and the crash between
  votes in `a_vote_survives_a_crash_so_a_term_has_one_leader`. The lesson is stated: a swarm
  explores, but a protocol's known worst schedules are cheaper to write down than to wait for.
- **Mutation pass:** 22 mutants of the node, its log and the simulator's driver, all killed
  (`target/tmp/mut6.py`). Most die in the unit tests and the simulator's tests together; four die
  only in unit tests: counting old-term replicas (masked in the swarm by the no-op, as above),
  pre-votes that ignore a live leader, a follower committing past what an append proved, and
  replies that each start another append. One survived the first pass, a late rejection moving
  `next_index` forward, because the floor at `matched + 1` hid it in the existing test; a new unit
  test kills it.

### D65: The workload
- **What:** clients propose numbered operations to the node they think is leader, follow
  redirects and retry on timeout. The applied state is the list of applied operations.
- **Why:** the simplest workload that still exposes a lost or duplicated commit. A retried
  proposal may legitimately be applied twice here; deduplicating retries is the queue's job in M7,
  through its dedup keys (D35).

### D66: A Raft entry is one server batch
- **What:** the leader drains up to 256 requests (D30), stamps them with one time read (D31) and
  proposes them as one entry: `at:u64 count:u32 op*` in the op encoding of D24. Every replica applies
  an entry's commands in order at that time. The leader's empty no-op (D61) applies nothing.
- **Alternatives:** one command per entry.
- **Why:** group commit survives replication: one Raft round (one sync on each replica) is shared by
  every request in the batch, as one sync was in M3. The time travels inside the entry, so every
  replica computes the same state without reading a clock (D4).
- **As built:** a batch also stops at 1 MiB of encoded ops, and an append at 1 MiB of entry data
  past its first entry (amending D60), so a peer frame stays under 4 MiB. Every op goes through
  the log (D69), idle workers' lease polls included: 20,000 simulated seeds committed 13,358,386
  entries for 640,573 completions. Batching keeps that one sync per batch; M11 measures it.

### D67: The Raft log is the queue's only log
- **What:** a replicated node keeps one file, the Raft log (D58). The queue lives in memory; after
  a restart it starts empty and is rebuilt by applying entries as the node learns they are
  committed. The M2 write-ahead log and snapshots stay for the single-node server.
- **Alternatives:** keep the queue's own WAL and snapshots and record the last applied index.
- **Why:** one durability path instead of two that must agree after every crash. The cost: replay
  time grows with the log until log compaction (Raft snapshots, Tier 3).

### D68: Time across leader changes
- **What:** nothing new. A new leader whose clock is behind stamps earlier times; the queue clock
  clamps them (D9) and stands still until the leader's clock catches up. Lease deadlines are judged
  by whichever leader stamps the next batch.
- **Alternatives:** a replicated clock entry, or leader leases bounded by clock drift.
- **Why:** D9 already makes a step back safe, and every replica applies the same stamped time, so
  skew between nodes shifts when leases expire but never makes replicas disagree.

### D69: Reads go through the log
- **What:** every op, including `result`, is proposed and answered when it applies. There is no
  read path that bypasses the log.
- **Alternatives:** ReadIndex (leader confirms leadership with a heartbeat round, then reads);
  leader leases (read locally, relying on bounded clock drift).
- **Why:** almost every queue op writes (a lease changes state), so a read fast path would serve
  only `result`. Through the log a read is linearizable with no second mechanism. ReadIndex is
  left for M11 if benchmarks show reads matter.

### D70: A reply belongs to the entry, index and term
- **What:** the leader remembers, for each entry it proposed, the index, the term and the
  requests in it. When that index applies with that term, each request gets its events. If another
  entry applies there, each request gets `unknown` and the client retries. A leader that steps
  down keeps its pending requests: the next leader may still commit the entry, and the answer then
  is the real one. Requests pending in a replica that restarts are lost with it; their clients
  time out and retry.
- **Alternatives:** reply when the leader has appended the entry.
- **Why:** an entry is the proposer's only if it is applied with the proposer's term (D65); the
  index alone can be reused by a later leader. Keeping requests across a step-down is also what
  makes the term check carry weight: answering `unknown` at every step-down would hide a missing
  term check, since a replaced entry is only ever applied after its proposer stopped leading. A reply before commit would acknowledge writes a
  new leader may overwrite.

### D71: Redirects
- **What:** protocol version 2 adds two replies: `not_leader` with the leader's id if known, and
  `unknown` (the outcome is not known; retry). A node that is not the leader answers `not_leader`.
  The cluster client knows every node's address by id, follows a hint, and otherwise tries the next
  node after a backoff. Servers accept versions 1 and 2; only a cluster sends the new replies.
- **Alternatives:** followers forward requests to the leader.
- **Why:** forwarding would make each follower track outcomes for requests it did not propose;
  redirects keep one place (the leader) that answers, and every hop is visible in tests.
- **As built:** peers use the same port: a connection whose first frame is `peer` (version, id)
  carries `raft` frames one way and is never answered. Each replica sends only on connections it
  dialed (one task per peer, redialing every 100 ms) and receives on the ones its peers dialed; a
  full outgoing queue drops the message, which Raft resends. A leader cut off from its quorum
  never answers, so the cluster client gives each attempt 1 s before it counts the connection
  as broken and moves on; it retries for a time the caller chooses.

### D72: Retries rely on each op being safe to retry
- **What:** after `unknown`, a timeout or a lost connection, the cluster client resends the same op.
  It gives every enqueue without a key a fresh dedup key (D35), so a retried enqueue never adds a
  second job. `complete` is idempotent per lease (D42). `heartbeat` renews again. A retried `lease`
  may lease a second job; the first lease is orphaned and expires after its visibility timeout.
- **Alternatives:** client sessions (Raft thesis 6.3): a per-client table of the last request id
  and reply in the state machine, giving exactly-once for every op.
- **Why:** it reuses what exists. The cost is stated: an orphaned lease delays its job by one
  visibility timeout and spends one attempt (D15), so a job near its limit can reach the dead-letter
  queue early; a retried `ack` or `nack` that had in fact applied comes back rejected. Workers that
  use `complete` (D41) are unaffected.

### D73: How M7 is built and checked
- **What:** (1) `replica::Replica`: one Raft node, its log and the queue, with no I/O of its own
  beyond the log, driven the way `raft::Node` is. (2) a simulator world with the M5 producers,
  workers and fenced store against a 3- or 5-node cluster under the M6 faults, checked by the M5
  end checks plus state-machine agreement between replicas. (3) a real TCP transport between peers,
  `spool serve --id --peers`, and a three-process test. Planted bugs: `reply-before-commit` and
  `ignore-term-on-reply`.
- **Alternatives:** simulator only, with real processes left to M10.
- **Why:** M5's checks are end to end (no lost or doubled effects, dedup, fencing) and apply
  unchanged to a replicated queue. A small real cluster shows the driver is not simulator-only;
  M10 adds the fault-injecting proxy.
