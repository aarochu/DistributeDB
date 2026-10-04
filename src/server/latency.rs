//! Lock-free latency histogram for `STATS` percentiles (SOW §17).
//!
//! Buckets are quarter-octaves of nanoseconds: bucket `i` holds durations in
//! `[2^(i/4), 2^((i+1)/4))` ns. A percentile is reported as the upper bound of
//! the bucket containing it, so it never understates latency and overstates it
//! by at most a factor of 2^(1/4) (about 19%). Recording is one relaxed atomic
//! increment, so worker threads never contend on a lock to record a sample.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Quarter-octave buckets from 1 ns up to 2^40 ns (about 18 minutes); longer
/// samples are counted in the last bucket.
const BUCKETS: usize = 160;

pub(crate) struct LatencyHistogram {
    counts: [AtomicU64; BUCKETS],
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            counts: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl std::fmt::Debug for LatencyHistogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LatencyHistogram")
            .field("samples", &self.samples())
            .finish()
    }
}

fn bucket(nanos: u64) -> usize {
    if nanos <= 1 {
        return 0;
    }
    ((nanos as f64).log2() * 4.0) as usize
}

fn upper_bound_nanos(index: usize) -> u64 {
    2f64.powf((index + 1) as f64 / 4.0).ceil() as u64
}

impl LatencyHistogram {
    pub(crate) fn record(&self, elapsed: Duration) {
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.counts[bucket(nanos).min(BUCKETS - 1)].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn samples(&self) -> u64 {
        self.counts.iter().map(|c| c.load(Ordering::Relaxed)).sum()
    }

    /// Nearest-rank percentile (`per_mille` of 1000) as a bucket upper bound
    /// in nanoseconds, or `None` before any sample. Concurrent recording can
    /// make one call see a slightly inconsistent set of buckets; the result is
    /// still the bound of a bucket that held a sample.
    pub(crate) fn percentile_nanos(&self, per_mille: u64) -> Option<u64> {
        let counts: Vec<u64> = self
            .counts
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .collect();
        let total: u64 = counts.iter().sum();
        if total == 0 {
            return None;
        }
        let rank = (total * per_mille).div_ceil(1000).max(1);
        let mut seen = 0;
        for (index, count) in counts.iter().enumerate() {
            seen += count;
            if seen >= rank {
                return Some(upper_bound_nanos(index));
            }
        }
        Some(upper_bound_nanos(BUCKETS - 1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_histogram_has_no_percentile() {
        assert_eq!(LatencyHistogram::default().percentile_nanos(500), None);
    }

    #[test]
    fn percentile_bounds_the_sample_within_one_bucket() {
        for nanos in [1u64, 7, 1_000, 123_456, 9_876_543_210] {
            let histogram = LatencyHistogram::default();
            histogram.record(Duration::from_nanos(nanos));
            let bound = histogram.percentile_nanos(990).unwrap();
            assert!(bound >= nanos, "{bound} < {nanos}");
            assert!(
                bound as f64 <= nanos as f64 * 1.19 + 2.0,
                "{bound} vs {nanos}"
            );
        }
    }

    #[test]
    fn percentiles_use_nearest_rank() {
        let histogram = LatencyHistogram::default();
        for _ in 0..90 {
            histogram.record(Duration::from_micros(10));
        }
        for _ in 0..10 {
            histogram.record(Duration::from_millis(10));
        }
        let p50 = histogram.percentile_nanos(500).unwrap();
        let p95 = histogram.percentile_nanos(950).unwrap();
        assert!((10_000..12_000).contains(&p50), "{p50}");
        assert!((10_000_000..12_000_000).contains(&p95), "{p95}");
        assert_eq!(histogram.samples(), 100);
    }

    #[test]
    fn oversized_samples_land_in_the_last_bucket() {
        let histogram = LatencyHistogram::default();
        histogram.record(Duration::from_secs(u64::MAX));
        assert_eq!(
            histogram.percentile_nanos(500),
            Some(upper_bound_nanos(BUCKETS - 1))
        );
    }
}
