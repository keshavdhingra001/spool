//! The simulator's only source of randomness (D48): SplitMix64, drawn in event
//! order, so a seed fixes the whole run.

/// SplitMix64 (Steele, Lea, Flood 2014): a 64-bit counter stepped by the golden
/// ratio and scrambled by two xor-shift-multiplies.
#[derive(Clone, Debug)]
pub struct Rng {
    state: u64,
}

/// Probabilities are integers in parts per million (D48).
pub const MILLION: u32 = 1_000_000;

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A number in `0..n`, by the high half of a 64x64-bit product (Lemire).
    /// The bias is below `n / 2^64`, far under anything a run can notice.
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "below(0)");
        ((u128::from(self.next_u64()) * u128::from(n)) >> 64) as u64
    }

    /// A number in `lo..=hi`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(lo <= hi, "empty range {lo}..={hi}");
        match (hi - lo).checked_add(1) {
            Some(n) => lo + self.below(n),
            None => self.next_u64(),
        }
    }

    /// True with probability `ppm` parts per million.
    pub fn chance(&mut self, ppm: u32) -> bool {
        self.below(u64::from(MILLION)) < u64::from(ppm)
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_reference_splitmix64() {
        // The first outputs of the reference implementation for seed 0.
        let mut r = Rng::new(0);
        assert_eq!(r.next_u64(), 0xE220_A839_7B1D_CDAF);
        assert_eq!(r.next_u64(), 0x6E78_9E6A_A1B9_65F4);
        assert_eq!(r.next_u64(), 0x06C4_5D18_8009_454F);
    }

    #[test]
    fn draws_stay_in_range_and_cover_it() {
        let mut r = Rng::new(7);
        let mut seen = [0u32; 6];
        for _ in 0..6_000 {
            seen[r.below(6) as usize] += 1;
            let x = r.range(10, 12);
            assert!((10..=12).contains(&x));
        }
        assert!(seen.iter().all(|&n| (800..1_200).contains(&n)), "{seen:?}");
        assert_eq!(r.range(5, 5), 5);
        let _ = r.range(0, u64::MAX);
        assert!(!r.chance(0));
        assert!(r.chance(MILLION));
        let hits = (0..10_000).filter(|_| r.chance(250_000)).count();
        assert!((2_200..2_800).contains(&hits), "{hits}");
    }
}
