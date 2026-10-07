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

Today (M1): the reference queue (D17) behind the `Queue` trait (D11): leases with visibility
timeouts and fencing tokens (D5, D18), retries with capped exponential backoff and deterministic
jitter (D13) under a per-queue policy (D14), expiry counted as an attempt (D15), a dead-letter state
with redrive (D16) and delayed jobs (D17). Time arrives in every command (D9). It is checked by two
independent checkers (D19) over scenario files (D20) and random command sequences.

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
  makes state and events disagree is caught by the comparison. Both run on 9 scenario files and
  on 500 random sequences of up to 300 commands per property, whose generator is checked to reach
  every event and rejection. Unit tests corrupt the state seven ways and feed the ledger eight
  impossible histories to show both actually catch something.

### D20: Scenario files
- **What:** `tests/scenarios/*.txt`: each command line (D12) is followed by one `= <event>` line per
  event it must produce; `#` lines are comments. A command with no `=` lines must produce nothing.
  Both checkers run on every command. The REPL runs one with `run <file>`.
- **Why:** each file reads as a story about one rule (the zombie worker, the poison job, the
  expiry boundary) and doubles as documentation. Expected events were recorded from the queue and
  then checked line by line against the rules, including the jitter bands.
