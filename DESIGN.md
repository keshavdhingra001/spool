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

Today (M3): the reference queue (D17) behind the `Queue` trait (D11): leases with visibility
timeouts and fencing tokens (D5, D18), retries with capped exponential backoff and deterministic
jitter (D13) under a per-queue policy (D14), expiry counted as an attempt (D15), a dead-letter state
with redrive (D16), delayed jobs (D17) and deduplication keys (D35). Time arrives in every command
(D9). It is checked by two independent checkers (D19) over scenario files (D20) and random command
sequences. It is durable (D21–D29): a write-ahead log of commands, synced before any result is
returned, with snapshots and crash-tested recovery. It is served over TCP (D30–D33): one core
thread owns the queue and commits batches with one sync, connections are tokio tasks, and the
server stamps the time (D31). A client and worker library (D34, D36) sit on the other side.

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
  makes state and events disagree is caught by the comparison. Both run on 10 scenario files and
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
  plug in. Run: three generated workloads of 40 commands with keyed and unkeyed enqueues (snapshot
  every 6) and the 10 scenario files back to back (snapshot every 25), single commands and batches
  mixed: 590 failure points, 38,616 crash images, 152,990 second crashes inside recovery, about
  12 s in a debug build. The
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
