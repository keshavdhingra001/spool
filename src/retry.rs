//! Per-queue retry policy (D13, D14): how many leases a job gets, and how long
//! it waits between them.

use std::fmt;

use crate::types::{JobId, Millis};

/// Settings of one queue, changed with the `configure` command (D14).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct QueueConfig {
    /// Leases a job gets before it is dead-lettered (D15). At least 1.
    pub max_attempts: u32,
    /// Delay before the first retry, before jitter. Doubles on every attempt.
    pub backoff_base: Millis,
    /// Upper bound on the delay before jitter. At least `backoff_base`.
    pub backoff_cap: Millis,
}

impl Default for QueueConfig {
    /// 5 attempts, retries after ~1 s, 2 s, 4 s, 8 s, delays capped at 5 minutes.
    fn default() -> Self {
        QueueConfig {
            max_attempts: 5,
            backoff_base: Millis(1_000),
            backoff_cap: Millis(300_000),
        }
    }
}

impl QueueConfig {
    pub fn is_valid(&self) -> bool {
        self.max_attempts >= 1 && self.backoff_base <= self.backoff_cap
    }

    /// How long `job` waits before its next lease, after its `attempt`-th lease
    /// failed (attempt counts from 1).
    ///
    /// Exponential backoff `d = min(cap, base * 2^(attempt-1))`, then "equal
    /// jitter": the result is `d/2` plus a pseudo-random amount in `[0, d - d/2]`,
    /// so it lands in `[d/2, d]`. The jitter comes from a hash of `(job, attempt)`,
    /// not from a random generator, so the queue stays deterministic (D4) while
    /// jobs that failed together still retry at different times (D13).
    pub fn retry_delay(&self, job: JobId, attempt: u32) -> Millis {
        let cap = self.backoff_cap.0;
        // 2^(attempt-1) overflows for attempt > 64 and the product can overflow
        // sooner; either way the delay is past the cap.
        let d = 1u64
            .checked_shl(attempt.saturating_sub(1))
            .and_then(|factor| self.backoff_base.0.checked_mul(factor))
            .map_or(cap, |d| d.min(cap));
        let half = d / 2;
        let spread = d - half;
        Millis(half + mix(job.0, attempt) % (spread + 1))
    }
}

/// A well-mixed 64-bit hash of `(job, attempt)`: splitmix64's finalizer applied
/// twice. Fixed arithmetic, so every platform and every replica gets the same value.
fn mix(job: u64, attempt: u32) -> u64 {
    splitmix64(splitmix64(job) ^ u64::from(attempt))
}

fn splitmix64(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The positional arguments of `configure`: `<max_attempts> <base_ms> <cap_ms>`.
impl fmt::Display for QueueConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} {}",
            self.max_attempts, self.backoff_base, self.backoff_cap
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(base: u64, cap: u64) -> QueueConfig {
        QueueConfig {
            max_attempts: 5,
            backoff_base: Millis(base),
            backoff_cap: Millis(cap),
        }
    }

    #[test]
    fn delay_is_within_the_jitter_band_of_the_exponential() {
        let c = config(1_000, 300_000);
        for job in 1..200 {
            for attempt in 1..=12 {
                let d = (1_000u64 << (attempt - 1)).min(300_000);
                let got = c.retry_delay(JobId(job), attempt).0;
                assert!(
                    (d / 2..=d).contains(&got),
                    "job {job} attempt {attempt}: {got} not in [{}, {d}]",
                    d / 2
                );
            }
        }
    }

    #[test]
    fn jitter_spreads_jobs_that_failed_together() {
        let c = config(1_000, 300_000);
        let delays: std::collections::BTreeSet<u64> = (1..=100)
            .map(|job| c.retry_delay(JobId(job), 1).0)
            .collect();
        // 100 jobs over a band of 501 values: a constant or badly mixed jitter
        // would give a handful of distinct delays.
        assert!(delays.len() > 80, "only {} distinct delays", delays.len());
    }

    #[test]
    fn huge_attempts_and_bases_hit_the_cap_without_overflow() {
        let c = config(u64::MAX / 2, u64::MAX);
        for attempt in [2, 3, 63, 64, 65, 1_000, u32::MAX] {
            let got = c.retry_delay(JobId(7), attempt).0;
            assert!(got >= u64::MAX / 2, "attempt {attempt}: {got}");
        }
        let c = config(1, 10);
        for attempt in [5, 64, 65, u32::MAX] {
            assert!((5..=10).contains(&c.retry_delay(JobId(7), attempt).0));
        }
    }

    #[test]
    fn zero_base_retries_immediately() {
        let c = config(0, 0);
        for attempt in [1, 2, 100] {
            assert_eq!(c.retry_delay(JobId(1), attempt), Millis(0));
        }
    }

    #[test]
    fn delay_is_deterministic_and_known() {
        // Pinned values: a change to the hash or the formula changes every retry
        // time in every log, so it should be a deliberate, visible change.
        let c = QueueConfig::default();
        let got: Vec<u64> = (1..=4).map(|a| c.retry_delay(JobId(1), a).0).collect();
        assert_eq!(got, PINNED);
    }

    const PINNED: [u64; 4] = [777, 1825, 3306, 6019];

    #[test]
    fn validity() {
        assert!(QueueConfig::default().is_valid());
        assert!(config(5, 5).is_valid());
        assert!(!config(6, 5).is_valid());
        let zero = QueueConfig {
            max_attempts: 0,
            ..QueueConfig::default()
        };
        assert!(!zero.is_valid());
    }
}
