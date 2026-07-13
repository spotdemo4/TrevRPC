use std::collections::BTreeMap;

use serde::Serialize;

#[derive(Debug, Default)]
pub(crate) struct LogLinearHistogram {
    buckets: BTreeMap<u64, u64>,
}

impl LogLinearHistogram {
    pub(crate) fn record(&mut self, value: u64) {
        let upper_bound = upper_bound(value.max(1));
        *self.buckets.entry(upper_bound).or_default() += 1;
    }

    pub(crate) fn merge(&mut self, other: Self) {
        for (upper_bound, count) in other.buckets {
            *self.buckets.entry(upper_bound).or_default() += count;
        }
    }

    pub(crate) fn count(&self) -> u64 {
        self.buckets.values().sum()
    }

    pub(crate) fn into_buckets(self) -> Vec<HistogramBucket> {
        self.buckets
            .into_iter()
            .map(|(upper_bound, count)| HistogramBucket {
                upper_bound_ns: upper_bound.to_string(),
                count: count.to_string(),
            })
            .collect()
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct HistogramBucket {
    upper_bound_ns: String,
    count: String,
}

fn upper_bound(value: u64) -> u64 {
    let shift = value.ilog2().saturating_sub(9);
    let upper = ((((u128::from(value)) >> shift) + 1) << shift) - 1;
    u64::try_from(upper).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{LogLinearHistogram, upper_bound};

    #[test]
    fn log_linear_bounds_follow_v1_formula() {
        assert_eq!(upper_bound(1), 1);
        assert_eq!(upper_bound(1023), 1023);
        assert_eq!(upper_bound(1024), 1025);
        assert_eq!(upper_bound(1025), 1025);
        assert_eq!(upper_bound(2048), 2051);
        assert_eq!(upper_bound(u64::MAX), u64::MAX);
    }

    #[test]
    fn histogram_is_sparse_and_mergeable() {
        let mut left = LogLinearHistogram::default();
        left.record(1024);
        left.record(1025);
        let mut right = LogLinearHistogram::default();
        right.record(1);
        left.merge(right);

        assert_eq!(left.count(), 3);
        let buckets = left.into_buckets();
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].upper_bound_ns, "1");
        assert_eq!(buckets[1].upper_bound_ns, "1025");
        assert_eq!(buckets[1].count, "2");
    }
}
