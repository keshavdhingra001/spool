//! Crash at every storage call, recover every image a crash could leave (D28).
//!
//! A workload runs on a `MemStorage` that fails at call `f`, for every `f`
//! until the workload finishes without reaching it. For every crash image of
//! the disk at that point:
//! 1. recovery succeeds and the state equals the reference queue after `k`
//!    commands, where every command whose events were returned is included
//!    (`k >= released`) and nothing after the failed call is (`k <= attempted`);
//! 2. the invariant checker passes;
//! 3. the recovered log accepts the next command, and it survives a reopen;
//! 4. a second crash during recovery, at every call, recovers the same state.
//!
//! Coverage is asserted at the end so the harness cannot pass by never
//! reaching the interesting cases.

use std::path::Path;

use spool::storage::MemStorage;
use spool::{Command, Durable, Event, Options, Queue, Recovery, ReferenceQueue, Snapshot};

type Mem<'a> = Durable<&'a mut MemStorage>;

fn encode(q: &ReferenceQueue) -> Vec<u8> {
    let mut out = Vec::new();
    q.encode_state(&mut out);
    out
}

#[derive(Debug, Default)]
struct Coverage {
    fail_points: u64,
    images: u64,
    second_crashes: u64,
    /// Recovered all attempted commands although some were never acknowledged.
    unacknowledged_survived: u64,
    /// Recovered fewer commands than were attempted.
    unsynced_lost: u64,
    torn_tails: u64,
    from_snapshot: u64,
}

/// Group sizes for the workload: mostly single commands, some batches (D25).
const GROUPS: [usize; 6] = [1, 1, 3, 1, 2, 1];

fn groups(n: usize) -> Vec<std::ops::Range<usize>> {
    let mut out = Vec::new();
    let mut at = 0;
    for &size in GROUPS.iter().cycle() {
        if at >= n {
            break;
        }
        let end = (at + size).min(n);
        out.push(at..end);
        at = end;
    }
    out
}

/// Run the workload on `disk` until the first error. Returns how many commands
/// had their events returned and how many were attempted.
fn run(disk: &mut MemStorage, cmds: &[Command], options: Options) -> (usize, usize) {
    let Ok((mut d, _)) = Mem::open(disk, options) else {
        return (0, 0);
    };
    let mut released = 0;
    for group in groups(cmds.len()) {
        let result = if group.len() == 1 {
            d.apply(&cmds[group.start]).map(|_| ())
        } else {
            d.apply_batch(&cmds[group.clone()]).map(|_| ())
        };
        if result.is_err() {
            return (released, group.end);
        }
        released = group.end;
    }
    (released, released)
}

fn check(name: &str, cmds: &[Command], options: Options) -> Coverage {
    let mut states = vec![encode(&ReferenceQueue::new())];
    let mut q = ReferenceQueue::new();
    let mut out: Vec<Event> = Vec::new();
    for c in cmds {
        q.apply(c, &mut out);
        states.push(encode(&q));
    }
    let mut cov = Coverage::default();
    for f in 0.. {
        let mut disk = MemStorage::new().fail_after(f);
        let (released, attempted) = run(&mut disk, cmds, options);
        if disk.calls() <= f {
            assert_eq!(released, cmds.len());
            break;
        }
        cov.fail_points += 1;
        for image in disk.crash_images() {
            cov.images += 1;
            let ctx = format!("{name}: fail at call {f}, image {:?}", image.files().keys());
            let (k, rec) = recover(&image, &states, released, attempted, &ctx);
            cov.unacknowledged_survived += u64::from(k == attempted && attempted > released);
            cov.unsynced_lost += u64::from(k < attempted);
            cov.torn_tails += u64::from(rec.truncated > 0);
            cov.from_snapshot += u64::from(rec.snapshot_lsn > 0);

            // The recovered log takes the next command, which survives a reopen.
            if k < cmds.len() {
                let mut disk = image.clone();
                let (mut d, _) = Mem::open(&mut disk, options).unwrap();
                d.apply(&cmds[k])
                    .unwrap_or_else(|e| panic!("{ctx}: append: {e}"));
                drop(d);
                let clean = MemStorage::from_files(disk.files());
                let (k2, _) = recover(&clean, &states, k + 1, k + 1, &ctx);
                assert_eq!(k2, k + 1);
            }

            // A second crash anywhere inside recovery ends in the same state.
            for g in 0.. {
                let mut disk = image.clone().fail_after(g);
                let _ = Mem::open(&mut disk, options);
                if disk.calls() <= g {
                    break;
                }
                for again in disk.crash_images() {
                    cov.second_crashes += 1;
                    let ctx = format!("{ctx}, second crash at call {g}");
                    recover(&again, &states, k, k, &ctx);
                }
            }
        }
    }
    eprintln!("{name}: {cov:?}");
    cov
}

/// Recover `image` and return which prefix of the workload it holds.
fn recover(
    image: &MemStorage,
    states: &[Vec<u8>],
    released: usize,
    attempted: usize,
    ctx: &str,
) -> (usize, Recovery) {
    let mut disk = image.clone();
    let (d, rec) = Mem::open(&mut disk, Options::default())
        .unwrap_or_else(|e| panic!("{ctx}: recovery failed: {e}"));
    d.queue()
        .check_invariants()
        .unwrap_or_else(|e| panic!("{ctx}: invariant: {e}"));
    let got = encode(d.queue());
    let k = (released..=attempted)
        .find(|&k| states[k] == got)
        .unwrap_or_else(|| {
            let other = states.iter().position(|s| *s == got);
            panic!(
                "{ctx}: recovered state is not the state after {released}..={attempted} \
                 commands (it matches {other:?})"
            )
        });
    (k, rec)
}

/// A small deterministic generator: xorshift64, seeded. Tests may use
/// randomness; the queue may not (D4).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// `n` commands that exercise every operation: heartbeats, acks and nacks pick
/// one of the leases issued so far, so some are current and some stale.
fn workload(seed: u64, n: usize) -> Vec<Command> {
    let mut rng = Rng(seed);
    let mut q = ReferenceQueue::new();
    let mut leases = Vec::new();
    let mut now = 0u64;
    let mut cmds = Vec::new();
    while cmds.len() < n {
        now = now
            .saturating_add(rng.below(6))
            .saturating_sub(rng.below(2));
        let queue = ["a", "b"][rng.below(2) as usize];
        let line = match rng.below(10) {
            0..=2 => format!(
                "@{now} enqueue {queue} p{} delay={}{}",
                cmds.len(),
                rng.below(8),
                ["", " key=k1", " key=k2"][rng.below(3) as usize]
            ),
            3 | 4 => format!("@{now} lease {queue} {}", 1 + rng.below(12)),
            5..=7 if !leases.is_empty() => {
                let (job, token) = leases[rng.below(leases.len() as u64) as usize];
                match rng.below(3) {
                    0 => format!("@{now} heartbeat {job} {token} {}", 1 + rng.below(12)),
                    1 => format!("@{now} ack {job} {token}"),
                    _ => format!("@{now} nack {job} {token}"),
                }
            }
            8 => format!(
                "@{now} configure {queue} {} 1 {}",
                1 + rng.below(3),
                1 + rng.below(6)
            ),
            9 if rng.below(2) == 0 => format!("@{now} redrive {queue}"),
            _ => format!("@{now} tick"),
        };
        let cmd: Command = line.parse().unwrap();
        let mut out = Vec::new();
        q.apply(&cmd, &mut out);
        for e in out {
            if let Event::Leased { lease, .. } = e {
                leases.push((lease.job.0, lease.token.0));
            }
        }
        cmds.push(cmd);
    }
    cmds
}

#[test]
fn crash_everywhere_with_snapshots() {
    let mut total = Coverage::default();
    for seed in 1..=3 {
        let cmds = workload(seed, 40);
        let cov = check(
            &format!("seed {seed}"),
            &cmds,
            Options { snapshot_every: 6 },
        );
        total.fail_points += cov.fail_points;
        total.images += cov.images;
        total.second_crashes += cov.second_crashes;
        total.unacknowledged_survived += cov.unacknowledged_survived;
        total.unsynced_lost += cov.unsynced_lost;
        total.torn_tails += cov.torn_tails;
        total.from_snapshot += cov.from_snapshot;
    }
    assert!(total.fail_points > 100, "{total:?}");
    assert!(total.unacknowledged_survived > 0, "{total:?}");
    assert!(total.unsynced_lost > 0, "{total:?}");
    assert!(total.torn_tails > 0, "{total:?}");
    assert!(total.from_snapshot > 0, "{total:?}");
    assert!(total.second_crashes > total.images, "{total:?}");
}

#[test]
fn crash_everywhere_in_the_scenario_files() {
    let mut cmds = Vec::new();
    let mut paths: Vec<_> = std::fs::read_dir(Path::new("tests/scenarios"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    paths.sort();
    for p in paths {
        let text = std::fs::read_to_string(p).unwrap();
        cmds.extend(
            text.lines()
                .map(str::trim)
                .filter(|l| !l.is_empty() && !l.starts_with('#') && !l.starts_with('='))
                .map(|l| l.parse::<Command>().unwrap()),
        );
    }
    let cov = check("scenarios", &cmds, Options { snapshot_every: 25 });
    assert!(cov.from_snapshot > 0 && cov.torn_tails > 0, "{cov:?}");
}
