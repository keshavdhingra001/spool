//! Raft in the deterministic simulator (M6, D62–D65).
//!
//! A fixed range of seeds must pass and, together, reach every fault and every
//! interesting Raft case. `SPOOL_SIM_SEEDS=a..b` runs another range. A seed
//! must replay exactly (D48). The planted bugs the swarm finds quickly must be
//! found within a few seeds, by the check meant to find them (D64); the two it
//! finds only rarely are pinned by unit tests in `src/raft/tests.rs`.

use spool::raft::Bug;
use spool::sim::WorldStats;
use spool::sim::raft::{self, Coverage, Options};

fn seeds() -> std::ops::Range<u64> {
    match std::env::var("SPOOL_SIM_SEEDS") {
        Ok(range) => {
            let (a, b) = range.split_once("..").expect("SPOOL_SIM_SEEDS=a..b");
            a.parse().unwrap()..b.parse().unwrap()
        }
        Err(_) => 0..300,
    }
}

#[test]
fn every_seed_passes_and_together_they_cover_every_fault() {
    let mut cov = Coverage::default();
    let mut world = WorldStats::default();
    let mut sizes = std::collections::BTreeSet::new();
    let mut mixes = std::collections::BTreeSet::new();
    for seed in seeds() {
        let report = raft::run(&Options::new(seed)).unwrap_or_else(|f| panic!("{f}"));
        cov.add(&report.coverage);
        let w = report.world;
        world.dropped += w.dropped;
        world.duplicated += w.duplicated;
        world.spiked += w.spiked;
        world.cut += w.cut;
        world.held += w.held;
        world.halts += w.halts;
        world.most_images = world.most_images.max(w.most_images);
        let s = report.swarm;
        sizes.insert(s.nodes);
        mixes.insert(format!("{s:?}"));
        assert_eq!(
            report.coverage.acked, 60,
            "seed {seed}: every operation acknowledged"
        );
    }
    println!("{cov:#?}\n{world:#?}\n{} fault mixes", mixes.len());
    if std::env::var("SPOOL_SIM_SEEDS").is_ok() {
        return;
    }
    assert_eq!(sizes.into_iter().collect::<Vec<_>>(), [3, 5]);
    assert!(world.dropped > 0 && world.duplicated > 0 && world.spiked > 0);
    assert!(world.cut > 0 && world.held > 0 && world.halts > 0);
    assert!(
        world.most_images > 1,
        "a crash chose between several images"
    );
    let n = cov.nodes;
    let reached = [
        // The faults...
        ("crashes", cov.crashes),
        ("leader crashes", cov.leader_crashes),
        ("crashes right after a sync", cov.crashed_after_sync),
        ("pauses", cov.pauses),
        ("leader pauses", cov.leader_pauses),
        ("partitions", cov.partitions),
        ("leader partitions", cov.leader_partitions),
        ("one-way cuts", cov.one_way_cuts),
        ("torn writes armed", cov.torn_armed),
        // ...recovery from them (D58)...
        ("recoveries", cov.recoveries),
        ("torn tails cut", cov.torn_tails),
        ("disk failures", cov.disk_failures),
        ("crashes during recovery", cov.recovery_failures),
        // ...every path through elections (D57, D59)...
        (
            "pre-votes refused while a leader lives",
            n.prevotes_refused_leader_alive,
        ),
        (
            "elections lost or split",
            n.elections_started - n.elections_won,
        ),
        ("CheckQuorum step-downs", n.check_quorum_stepdowns),
        ("higher-term step-downs", n.higher_term_stepdowns),
        ("stale messages", n.stale_messages),
        // ...replication (D60, D61)...
        ("truncated conflicting entries", n.truncations),
        ("rejected appends", n.appends_rejected),
        ("conflict-hint jumps", n.hint_jumps),
        // ...and every client case (D65).
        ("redirects", cov.redirects),
        ("not-leader answers", cov.not_leader),
        ("client timeouts", cov.client_timeouts),
        ("replaced proposals", cov.replaced),
    ];
    for (what, count) in reached {
        assert!(count > 0, "never reached: {what}");
    }
    assert!(
        n.largest_append > 1,
        "appends carry batches (the 64-entry cap is unit-tested)"
    );
    assert!(cov.max_term > 10);
}

#[test]
fn a_seed_replays_exactly() {
    for seed in [1, 17, 99] {
        let first = raft::run(&Options::new(seed)).unwrap();
        let again = raft::run(&Options::new(seed)).unwrap();
        let traced = raft::run(&Options {
            trace: true,
            ..Options::new(seed)
        })
        .unwrap();
        for other in [&again, &traced] {
            assert_eq!(first.hash, other.hash, "seed {seed}");
            assert_eq!(first.coverage, other.coverage);
            assert_eq!(first.world, other.world);
            assert_eq!(first.finished, other.finished);
        }
        assert!(first.trace.is_empty());
        // Every delivery is traced (timers of an old life are not).
        assert!(traced.trace.len() as u64 >= traced.world.delivered);
    }
    let a = raft::run(&Options::new(1)).unwrap();
    let b = raft::run(&Options::new(2)).unwrap();
    assert_ne!(a.hash, b.hash);
}

#[test]
fn planted_bugs_are_found_within_a_few_seeds() {
    for (bug, expected) in [
        (
            Bug::NoLogTruncate,
            &["log matching", "state machine safety"][..],
        ),
        (
            Bug::StaleTermAccept,
            &[
                "lost committed entries",
                "leader completeness",
                "state machine safety",
            ],
        ),
    ] {
        let found = (0..20).find_map(|seed| {
            raft::run(&Options {
                bug: Some(bug),
                ..Options::new(seed)
            })
            .err()
        });
        let failure = found.unwrap_or_else(|| panic!("{bug:?} survived 20 seeds"));
        println!("{bug:?}: {failure}");
        assert!(
            expected.iter().any(|e| failure.message.contains(e)),
            "{bug:?}: {failure}"
        );
        assert!(failure.to_string().contains("sim --raft --seed"));
    }
}
