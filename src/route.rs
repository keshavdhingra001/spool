//! Which partition an op goes to (D76): a pure function of the op and the
//! cluster's partition count, shared by the cluster client and the simulator.

use crate::command::Op;

/// Where an op goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    /// To this partition.
    One(u16),
    /// To one partition after another, until one has something (D78): a
    /// lease, or an enqueue with neither key to hash.
    Rotate,
    /// To every partition, one at a time: ops that change a queue's
    /// settings, which every partition holds a copy of (D74).
    All,
}

/// The partition of an enqueue's ordering key or, without one, of its dedup
/// key (D76); of a job id's partition bits (D77) for ops on one job.
pub fn route(op: &Op, partitions: u16) -> Route {
    assert!(partitions > 0, "a cluster has partitions");
    let hashed = |bytes: &[u8]| Route::One((fnv1a(bytes) % u64::from(partitions)) as u16);
    match op {
        Op::Enqueue {
            order: Some(order), ..
        } => hashed(order.as_str().as_bytes()),
        Op::Enqueue { key: Some(key), .. } => hashed(key.as_str().as_bytes()),
        Op::Enqueue { .. } | Op::Lease { .. } => Route::Rotate,
        Op::Heartbeat { job, .. }
        | Op::Ack { job, .. }
        | Op::Nack { job, .. }
        | Op::Complete { job, .. }
        | Op::Result { job } => Route::One(job.partition()),
        Op::Configure { .. } | Op::Redrive { .. } | Op::Subscribe { .. } => Route::All,
        // Only moves partition 0's clock; every partition expires leases as
        // its next command arrives anyway.
        Op::Tick => Route::One(0),
    }
}

/// FNV-1a, 64 bits: spelled out so the partition of a key never changes
/// between builds or platforms, as `std::hash` may (D76).
pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Command;
    use crate::types::JobId;

    fn op(line: &str) -> Op {
        format!("@0 {line}").parse::<Command>().unwrap().op
    }

    #[test]
    fn fnv1a_matches_the_published_vectors() {
        assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a(b"foobar"), 0x8594_4171_f739_67e8);
    }

    #[test]
    fn the_ordering_key_wins_over_the_dedup_key() {
        let by_order = route(&op("enqueue q x order=user-1"), 7);
        assert_eq!(route(&op("enqueue q x key=k1 order=user-1"), 7), by_order);
        assert_eq!(route(&op("enqueue q y key=k2 order=user-1"), 7), by_order);
        assert_eq!(
            route(&op("enqueue q x key=k1"), 7),
            Route::One((fnv1a(b"k1") % 7) as u16)
        );
        assert_eq!(route(&op("enqueue q x"), 7), Route::Rotate);
    }

    #[test]
    fn keys_spread_over_partitions() {
        let mut seen = [0; 4];
        for i in 0..400 {
            if let Route::One(p) = route(&op(&format!("enqueue q x key=k{i}")), 4) {
                seen[usize::from(p)] += 1;
            }
        }
        assert!(seen.iter().all(|&n| n > 60), "{seen:?}");
    }

    #[test]
    fn job_ops_go_to_the_partition_in_the_id() {
        let job = JobId::new(3, 17);
        for line in [
            format!("ack {job} 1"),
            format!("nack {job} 1"),
            format!("heartbeat {job} 1 10"),
            format!("complete {job} 1 r"),
            format!("result {job}"),
        ] {
            assert_eq!(route(&op(&line), 4), Route::One(3), "{line}");
        }
        assert_eq!(route(&op("lease q 10"), 4), Route::Rotate);
        for line in ["configure q 1 0 0", "redrive q", "subscribe q g"] {
            assert_eq!(route(&op(line), 4), Route::All, "{line}");
        }
        assert_eq!(route(&op("tick"), 4), Route::One(0));
        assert_eq!(route(&op("enqueue q x key=k"), 1), Route::One(0));
    }
}
