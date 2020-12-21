use super::Snapshot;
use crate::model::entities::KeepSpec;
use crate::model::entities::RetentionRuleset;

use chrono::{DateTime, Utc};
use std::{cmp::Reverse, collections::HashSet, iter::repeat};
use std::{convert::TryFrom, num::NonZeroUsize};

pub fn evaluate_retention<'a, T: Snapshot>(snapshots: &'a [T], rules: &RetentionRuleset) -> RetentionEvaluation<'a, T> {
    let mut snapshots: Vec<_> = snapshots.iter().collect();
    snapshots.sort_unstable_by_key(|b| Reverse(b.datetime()));
    let snapshots = snapshots;

    let begin_time = snapshots[0].datetime();
    let mut keep_interval_buckets = rules
        .interval
        .iter()
        .flat_map(|m| repeat(m).take(usize::try_from(m.repeat.get()).unwrap()))
        .scan(begin_time, |end_time_state, sm| {
            *end_time_state = *end_time_state - chrono::Duration::from_std(sm.duration).unwrap();
            Some(RetainBucket::new(sm.keep, *end_time_state))
        })
        .collect::<Vec<_>>();

    let mut keep_minimum_snapshots = vec![];
    let mut drop_snapshots = vec![];
    let mut bucket_iter = keep_interval_buckets.iter_mut();
    let mut current_bucket = bucket_iter.next();
    for (index, snapshot) in snapshots.into_iter().enumerate() {
        while let Some(ref bucket) = current_bucket {
            if snapshot.datetime() >= bucket.end_time {
                break;
            } else {
                current_bucket = bucket_iter.next();
            }
        }

        match current_bucket {
            Some(ref mut bucket) if bucket.snapshots.len() < bucket.max_fill.get() => bucket.snapshots.push(snapshot),
            _ if index < usize::try_from(rules.newest_count.get()).unwrap() => keep_minimum_snapshots.push(snapshot),
            _ => drop_snapshots.push(snapshot),
        }
    }

    RetentionEvaluation {
        drop_snapshots,
        keep_minimum_snapshots,
        keep_interval_buckets,
    }
}

pub struct RetentionEvaluation<'a, T> {
    pub drop_snapshots: Vec<&'a T>,
    pub keep_minimum_snapshots: Vec<&'a T>,
    pub keep_interval_buckets: Vec<RetainBucket<'a, T>>,
}

impl<'a, T: Snapshot> RetentionEvaluation<'a, T> {
    fn into_drop_set(self) -> HashSet<DateTime<Utc>> {
        self.drop_snapshots.into_iter().map(|s| s.datetime()).collect()
    }
}

#[derive(Debug)]
pub struct RetainBucket<'a, T> {
    pub snapshots: Vec<&'a T>,
    pub max_fill: NonZeroUsize,
    pub end_time: chrono::DateTime<Utc>,
}

impl<'a, T> RetainBucket<'a, T> {
    fn new(keep: KeepSpec, end_time: DateTime<Utc>) -> Self {
        Self {
            snapshots: Default::default(),
            max_fill: match keep {
                KeepSpec::Newest(n) => NonZeroUsize::new(usize::try_from(n.get()).unwrap()).unwrap(),
                KeepSpec::All => NonZeroUsize::new(usize::MAX).unwrap(),
            },
            end_time,
        }
    }
}
