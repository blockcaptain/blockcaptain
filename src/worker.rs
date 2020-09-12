use crate::model::{entities::KeepSpec, Entity};
use crate::{
    core::{self, BtrfsContainer, BtrfsContainerSnapshot, BtrfsDataset, BtrfsDatasetSnapshot},
    model::entities::RetentionRuleset,
};
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use log::*;
use std::iter::repeat;
use std::{convert::TryFrom, num::NonZeroUsize, rc::Rc};

pub trait Job {
    fn run(&self) -> Result<()>;
    fn next_check(&self) -> Result<Duration>;

    fn is_ready(&self) -> Result<bool> {
        self.next_check().map(|d| {
            trace!(
                "Job is {}ready based on having {} delay to next check.",
                if d.is_zero() { "" } else { "not " },
                if d.is_zero() { "no" } else { "a" }
            );
            d.is_zero()
        })
    }
}

pub struct LocalSnapshotJob {
    dataset: Rc<BtrfsDataset>,
}

impl LocalSnapshotJob {
    pub fn new(dataset: &Rc<BtrfsDataset>) -> Self {
        Self {
            dataset: Rc::clone(dataset),
        }
    }
}

impl Job for LocalSnapshotJob {
    fn run(&self) -> Result<()> {
        self.dataset.create_local_snapshot()
    }

    fn next_check(&self) -> Result<Duration> {
        let latest = self.dataset.latest_snapshot()?;
        Ok(if let Some(latest_snapshot) = latest {
            trace!(
                "Existing snapshot for {} at {}.",
                self.dataset.model().id(),
                latest_snapshot.datetime()
            );
            let now = Utc::now();
            let next_datetime = latest_snapshot.datetime() + Duration::hours(1);
            if now < next_datetime {
                next_datetime - now
            } else {
                Duration::zero()
            }
        } else {
            trace!("No existing snapshot for {}.", self.dataset.model().id());
            Duration::zero()
        })
    }
}

pub struct LocalSyncJob {
    dataset: Rc<BtrfsDataset>,
    container: Rc<BtrfsContainer>,
}

impl LocalSyncJob {
    pub fn new(dataset: &Rc<BtrfsDataset>, container: &Rc<BtrfsContainer>) -> Self {
        Self {
            dataset: Rc::clone(dataset),
            container: Rc::clone(container),
        }
    }

    fn ready_snapshots(&self) -> Result<(Vec<BtrfsDatasetSnapshot>, Vec<BtrfsContainerSnapshot>, Vec<usize>)> {
        let dataset_snapshots = {
            let mut snaps = self.dataset.snapshots()?;
            snaps.sort_unstable_by_key(|s| s.datetime());
            snaps
        };

        let container_snapshots = {
            let mut snaps = self.container.snapshots(self.dataset.model())?;
            snaps.sort_unstable_by_key(|s| s.datetime());
            snaps
        };

        let ready_index = if container_snapshots.is_empty() {
            dataset_snapshots
                .last()
                .map_or(vec![], |_| vec![dataset_snapshots.len() - 1])
        } else {
            let latest_in_container = container_snapshots.last().unwrap();
            dataset_snapshots
                .iter()
                .enumerate()
                .skip_while(|(_, s)| s.datetime() <= latest_in_container.datetime())
                .map(|(i, _)| i)
                .collect()
        };

        Ok((dataset_snapshots, container_snapshots, ready_index))
    }
}

impl Job for LocalSyncJob {
    fn run(&self) -> Result<()> {
        let (dataset_snapshots, mut container_snapshots, send_snapshots) = self.ready_snapshots()?;

        trace!("Identified {} snapshots to send.", send_snapshots.len());
        for snapshot_index in send_snapshots {
            let source_snapshot = &dataset_snapshots[snapshot_index];
            trace!("Sending snapshot {:?}.", source_snapshot);

            let mut maybe_parent_snapshot = None;
            if snapshot_index > 0 {
                info!("Local snapshot has predecessors. Searching for viable parent for delta send...");
                let previous_snapshot = &dataset_snapshots[snapshot_index - 1];
                if source_snapshot.parent_uuid() == previous_snapshot.parent_uuid() {
                    info!("Predecessor has same parent as this snapshot, parent identified.");
                    maybe_parent_snapshot = Some(previous_snapshot)
                } else if source_snapshot.parent_uuid().expect("Should always have parent here.") == self.dataset.uuid()
                {
                    info!("Predecessor does not have same parent as this snapshot, but it is a snapshot of the active dataset.");
                    if let Some(dataset_parent_uuid) = self.dataset.parent_uuid() {
                        info!("Active dataset has a parent.");
                        maybe_parent_snapshot = dataset_snapshots.iter().find(|s| s.uuid() == dataset_parent_uuid);
                        if maybe_parent_snapshot.is_some() {
                            info!("Parent exists in local snapshots, parent identified.");
                        } else {
                            info!("Parent could not be found in local snapshots. Delta send will not be possible.");
                        }
                    } else {
                        info!("Active dataset does not have a parent. Delta send will not be possible.");
                    }
                } else {
                    info!("Could not identify a viable parent. Delta send will not be possible.");
                }

                if let Some(candidate) = maybe_parent_snapshot {
                    if container_snapshots
                        .iter()
                        .find(|s| s.received_uuid() == candidate.uuid())
                        .is_none()
                    {
                        if let Some(candidate_received_from) = candidate.received_uuid() {
                            info!("Parent appears to be a restored snapshot.");
                            if container_snapshots
                                .iter()
                                .find(|s| s.received_uuid() == candidate_received_from)
                                .is_none()
                            {
                                info!("Restored snapshot does not exist in the destination container. Delta send will not be possible.");
                                maybe_parent_snapshot = None;
                            }
                        } else {
                            info!(
                                "Parent does not exist in the destination container. Delta send will not be possible."
                            );
                            maybe_parent_snapshot = None;
                        }
                    }
                }
            } else {
                info!("Snapshot is first local snapshot. Delta send will not be possible.");
            }
            let new_snapshot = match maybe_parent_snapshot {
                Some(parent_snapshot) => {
                    info!("Sending delta snapshot.");
                    core::transfer_delta_snapshot(parent_snapshot, source_snapshot, &self.container)
                }
                None => {
                    info!("Sending full snapshot.");
                    core::transfer_full_snapshot(source_snapshot, &self.container)
                }
            }?;
            container_snapshots.push(new_snapshot);
        }
        Ok(())
    }

    fn is_ready(&self) -> Result<bool> {
        Ok(!self.ready_snapshots()?.2.is_empty())
    }

    fn next_check(&self) -> Result<Duration> {
        Ok(Duration::minutes(10))
    }
}

pub struct LocalPruneJob {
    dataset: Rc<BtrfsDataset>,
}

impl LocalPruneJob {
    pub fn new(dataset: &Rc<BtrfsDataset>) -> Self {
        Self {
            dataset: Rc::clone(dataset),
        }
    }
}

impl Job for LocalPruneJob {
    fn run(&self) -> Result<()> {
        let rules = self
            .dataset
            .model()
            .snapshot_retention
            .as_ref()
            .expect("Validated by is_ready.");
        let evaluation = evaluate_retention(self.dataset.snapshots()?, rules)?;

        if log_enabled!(Level::Trace) {
            for snapshot in evaluation.keep_interval_buckets.iter().flat_map(|b| b.snapshots.iter()) {
                trace!("Keeping snapshot {} reason: in retention interval.", snapshot);
            }

            for snapshot in evaluation.keep_minimum_snapshots.iter() {
                trace!("Keeping snapshot {} reason: keep minimum newest.", snapshot);
            }
        }

        for snapshot in evaluation.drop_snapshots {
            info!(
                "Snapshot {} is being pruned because it did not meet any retention criteria.",
                snapshot
            );
            snapshot.delete().map_err(|e| e.source)?;
        }

        Ok(())
    }

    fn next_check(&self) -> Result<Duration> {
        Ok(Duration::hours(1))
    }

    fn is_ready(&self) -> Result<bool> {
        Ok(self.dataset.model().snapshot_retention.is_some() && !self.dataset.model().pause_pruning)
    }
}

pub fn evaluate_retention(
    mut snapshots: Vec<BtrfsDatasetSnapshot>,
    rules: &RetentionRuleset,
) -> Result<RetentionEvaluation> {
    snapshots.sort_unstable_by(|a, b| b.datetime().cmp(&a.datetime()));
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
    let mut current_bucket = bucket_iter.next().unwrap();
    for (index, snapshot) in snapshots.into_iter().enumerate() {
        let out_of_interval_range = loop {
            if snapshot.datetime() < current_bucket.end_time {
                if let Some(bucket) = bucket_iter.next() {
                    current_bucket = bucket;
                } else {
                    break true;
                }
            } else {
                break false;
            }
        };

        if !out_of_interval_range && current_bucket.snapshots.len() < current_bucket.max_fill.get() {
            current_bucket.snapshots.push(snapshot);
        } else if index < usize::try_from(rules.newest_count.get()).unwrap() {
            keep_minimum_snapshots.push(snapshot);
        } else {
            drop_snapshots.push(snapshot);
        }
    }

    Ok(RetentionEvaluation {
        drop_snapshots,
        keep_minimum_snapshots,
        keep_interval_buckets,
    })
}

pub struct RetentionEvaluation {
    pub drop_snapshots: Vec<BtrfsDatasetSnapshot>,
    pub keep_minimum_snapshots: Vec<BtrfsDatasetSnapshot>,
    pub keep_interval_buckets: Vec<RetainBucket>,
}

#[derive(Debug)]
pub struct RetainBucket {
    pub snapshots: Vec<BtrfsDatasetSnapshot>,
    pub max_fill: NonZeroUsize,
    pub end_time: chrono::DateTime<Utc>,
}

impl RetainBucket {
    fn new(keep: KeepSpec, end_time: DateTime<Utc>) -> Self {
        Self {
            snapshots: Default::default(),
            max_fill: match keep {
                KeepSpec::Newest(n) => NonZeroUsize::new(usize::try_from(n.get()).unwrap()).unwrap(),
                KeepSpec::All => NonZeroUsize::new(usize::MAX).unwrap(),
            },
            end_time: end_time,
        }
    }
}
