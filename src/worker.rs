use crate::core::{self, BtrfsContainer, BtrfsContainerSnapshot, BtrfsDataset, BtrfsDatasetSnapshot};
use crate::model::Entity;
use anyhow::Result;
use chrono::{Duration, Utc};
use log::*;

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

pub struct LocalSnapshotJob<'a> {
    dataset: &'a BtrfsDataset,
}

impl<'a> LocalSnapshotJob<'a> {
    pub fn new(dataset: &'a BtrfsDataset) -> Self {
        Self { dataset }
    }
}

impl<'a> Job for LocalSnapshotJob<'a> {
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

pub struct LocalSyncJob<'a> {
    dataset: &'a BtrfsDataset,
    container: &'a BtrfsContainer,
}

impl<'a> LocalSyncJob<'a> {
    pub fn new(dataset: &'a BtrfsDataset, container: &'a BtrfsContainer) -> Self {
        Self { dataset, container }
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

impl<'a> Job for LocalSyncJob<'a> {
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
                    core::transfer_delta_snapshot(self.dataset, parent_snapshot, source_snapshot, self.container)
                }
                None => {
                    info!("Sending full snapshot.");
                    core::transfer_full_snapshot(self.dataset, source_snapshot, self.container)
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
