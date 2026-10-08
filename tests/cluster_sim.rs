//! The queue on Raft in the deterministic simulator (M7, D73).
//!
//! A fixed range of seeds must pass and, together, reach every fault and the
//! client paths of D71 and D72. `SPOOL_SIM_SEEDS=a..b` runs another range. A
//! seed must replay exactly (D48). The planted bugs the swarm finds quickly
//! must be found within a few seeds.

use spool::replica::Bug;
use spool::sim::WorldStats;
use spool::sim::cluster::{self, Coverage, Options};
use spool::sim::queue;

fn seeds() -> std::ops::Range<u64> {
    match std::env::var("SPOOL_SIM_SEEDS") {
        Ok(range) => {
            let (a, b) = range.split_once("..").expect("SPOOL_SIM_SEEDS=a..b");
            a.parse().unwrap()..b.parse().unwrap()
        }
        Err(_) => 0..100,
    }
}

#[test]
fn every_seed_passes_and_together_they_cover_every_fault() {
    let mut cov = Coverage::default();
    let mut world = WorldStats::default();
    let mut sizes = std::collections::BTreeSet::new();
    for seed in seeds() {
        let report = cluster::run(&Options::new(seed)).unwrap_or_else(|f| panic!("{f}"));
        cov.add(&report.coverage);
        let w = report.world;
        world.dropped += w.dropped;
        world.duplicated += w.duplicated;
        world.spiked += w.spiked;
        world.cut += w.cut;
        world.held += w.held;
        world.halts += w.halts;
        sizes.insert(report.swarm.nodes);
    }
    println!("{cov:#?}\n{world:#?}");
    if std::env::var("SPOOL_SIM_SEEDS").is_ok() {
        return;
    }
    assert_eq!(sizes.into_iter().collect::<Vec<_>>(), [3, 5]);
    assert!(world.dropped > 0 && world.duplicated > 0 && world.spiked > 0);
    assert!(world.cut > 0 && world.held > 0 && world.halts > 0);
    let q = cov.queue;
    let reached = [
        // The faults...
        ("replica crashes", q.server_crashes),
        ("leader crashes", cov.leader_crashes),
        ("leader pauses", cov.leader_pauses),
        ("leader partitions", cov.leader_partitions),
        ("one-way cuts", cov.one_way_cuts),
        ("client crashes", q.client_crashes),
        ("torn writes armed", q.torn_armed),
        ("clock steps forward", q.clock_forward),
        ("clock steps back", q.clock_back),
        // ...recovery from them (D67)...
        ("recoveries", q.recoveries),
        ("torn tails cut", q.torn_tails),
        ("disk failures", q.disk_failures),
        // ...the client paths (D71, D72)...
        ("redirects", q.redirects),
        ("not-leader answers", q.not_leader),
        ("unknown answers", q.unknown),
        ("enqueue retries", q.enqueue_retries),
        ("deduplicated retries", q.deduplicated),
        ("lease retries", q.lease_retries),
        ("completes answered after a retry", q.completed_after_retry),
        // ...and the queue's own cases (D43).
        ("expired leases", q.leases_expired),
        ("zombie writes refused", q.writes_refused),
    ];
    for (what, count) in reached {
        assert!(count > 0, "never reached: {what}");
    }
    assert!(cov.max_term > 10);
    assert!(cov.committed > 0);
}

#[test]
fn a_seed_replays_exactly() {
    for seed in [3, 42] {
        let first = cluster::run(&Options::new(seed)).unwrap();
        let traced = cluster::run(&Options {
            trace: true,
            ..Options::new(seed)
        })
        .unwrap();
        assert_eq!(first.hash, traced.hash, "seed {seed}");
        assert_eq!(first.coverage, traced.coverage);
        assert_eq!(first.world, traced.world);
        assert!(traced.trace.len() as u64 >= traced.world.delivered);
    }
}

#[test]
fn planted_bugs_are_found_within_a_few_seeds() {
    let cases = [
        (
            Options {
                bug: Some(Bug::ReplyBeforeCommit),
                ..Options::new(0)
            },
            "reply-before-commit",
        ),
        (
            Options {
                bug: Some(Bug::IgnoreTermOnReply),
                ..Options::new(0)
            },
            "ignore-term-on-reply",
        ),
        (
            Options {
                queue_bug: Some(queue::Bug::NoFence),
                ..Options::new(0)
            },
            "no-fence",
        ),
        (
            Options {
                queue_bug: Some(queue::Bug::NoDedupKey),
                ..Options::new(0)
            },
            "no-dedup-key",
        ),
    ];
    for (options, name) in cases {
        let found = (0..100).find_map(|seed| {
            cluster::run(&Options {
                seed,
                ..options.clone()
            })
            .err()
        });
        let failure = found.unwrap_or_else(|| panic!("{name} survived 100 seeds"));
        println!("{name}: {failure}");
        assert!(failure.to_string().contains("sim --cluster --seed"));
    }
}
