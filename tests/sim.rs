//! The deterministic simulator (M5) on the single-node queue world (D51).
//!
//! A fixed range of seeds must pass and, together, reach every fault and every
//! interesting outcome (D55). `SPOOL_SIM_SEEDS=a..b` runs another range. A
//! seed must replay exactly, with or without its trace (D48). Each planted bug
//! must be found within a few seeds, by the check meant to find it (D54).

use spool::sim::queue::{self, Bug, Coverage, Options};
use spool::sim::{MILLION, WorldStats};

fn seeds() -> std::ops::Range<u64> {
    match std::env::var("SPOOL_SIM_SEEDS") {
        Ok(range) => {
            let (a, b) = range.split_once("..").expect("SPOOL_SIM_SEEDS=a..b");
            a.parse().unwrap()..b.parse().unwrap()
        }
        Err(_) => 0..200,
    }
}

#[test]
fn every_seed_passes_and_together_they_cover_every_fault() {
    let mut cov = Coverage::default();
    let mut world = WorldStats::default();
    let mut swarm_mixes = std::collections::BTreeSet::new();
    let mut lossless = 0;
    for seed in seeds() {
        let report = queue::run(&Options::new(seed)).unwrap_or_else(|f| panic!("{f}"));
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
        lossless += u64::from(s.drop_ppm == 0 && s.dup_ppm == 0);
        swarm_mixes.insert(format!(
            "{} {} {} {} {} {}",
            s.partitions, s.crashes, s.server_crashes, s.torn_writes, s.pauses, s.clock_jumps
        ));
        assert!(s.drop_ppm <= MILLION / 5);
    }
    println!("{cov:#?}\n{world:#?}\n{} fault mixes", swarm_mixes.len());
    if std::env::var("SPOOL_SIM_SEEDS").is_ok() {
        return;
    }
    // The network, the disk and the clock all did their worst...
    assert!(world.dropped > 0 && world.duplicated > 0 && world.spiked > 0);
    assert!(world.cut > 0 && world.held > 0 && world.halts > 0);
    assert!(
        world.most_images > 1,
        "a crash chose between several images"
    );
    assert!(lossless > 0, "some seeds run on a lossless network");
    assert!(swarm_mixes.len() > 30, "swarm testing varied the mix");
    let reached = [
        ("partitions", cov.partitions),
        ("client crashes", cov.client_crashes),
        ("server crashes", cov.server_crashes),
        ("torn writes armed", cov.torn_armed),
        ("pauses", cov.pauses),
        ("clock forward", cov.clock_forward),
        ("clock back", cov.clock_back),
        // ...the server recovered from all of it...
        ("recoveries", cov.recoveries),
        ("recovered from a snapshot", cov.recovered_from_snapshot),
        ("torn tails cut", cov.torn_tails),
        ("unanswered commands lost", cov.unsynced_lost),
        ("unanswered commands kept", cov.unreplied_survived),
        ("crashes during recovery", cov.recovery_failures),
        ("disk failures", cov.disk_failures),
        // ...and the clients met every case of D36, D42 and D43.
        ("enqueue retries", cov.enqueue_retries),
        ("deduplicated enqueues", cov.deduplicated),
        ("expired leases", cov.leases_expired),
        ("rejected heartbeats", cov.heartbeats_rejected),
        ("write retries", cov.write_retries),
        ("zombie writes refused", cov.writes_refused),
        ("complete retries", cov.complete_retries),
        ("completed after a retry", cov.completed_after_retry),
        ("rejected completes", cov.completes_rejected),
    ];
    for (what, n) in reached {
        assert!(n > 0, "never reached: {what}");
    }
    assert_eq!(cov.unfenced_accepts, 0);
    assert!(cov.largest_batch > 1, "group commit batched requests");
}

#[test]
fn a_seed_replays_exactly() {
    for seed in [1, 17, 99] {
        let first = queue::run(&Options::new(seed)).unwrap();
        let again = queue::run(&Options::new(seed)).unwrap();
        let traced = queue::run(&Options {
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
        assert!(traced.trace.len() as u64 >= traced.world.events);
    }
    let a = queue::run(&Options::new(1)).unwrap();
    let b = queue::run(&Options::new(2)).unwrap();
    assert_ne!(a.hash, b.hash);
}

#[test]
fn planted_bugs_are_found_within_a_few_seeds() {
    for (bug, expected) in [
        (Bug::NoFence, "but the store holds the effect of token"),
        (Bug::NoDedupKey, "a retry added a job"),
    ] {
        let found = (0..50).find_map(|seed| {
            queue::run(&Options {
                bug: Some(bug),
                ..Options::new(seed)
            })
            .err()
        });
        let failure = found.unwrap_or_else(|| panic!("{bug:?} survived 50 seeds"));
        println!("{bug:?}: {failure}");
        assert!(failure.message.contains(expected), "{bug:?}: {failure}");
        assert!(
            failure.to_string().contains("--seed"),
            "the failure says how to replay it"
        );
    }
}
