use crate::actors::observation::observable_func;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use futures_util::future::ready;
use libblkcapt::model::{entities::KeepSpec, entities::ObservableEvent, Entity};
use libblkcapt::{
    core::{self, BtrfsContainer, BtrfsContainerSnapshot, BtrfsDataset, BtrfsDatasetSnapshot},
    model::entities::RetentionRuleset,
};
use log::*;
use std::{cmp::Reverse, future::Future, iter::repeat, pin::Pin, sync::Arc};
use std::{convert::TryFrom, num::NonZeroUsize};

pub struct LocalSyncJob {
    dataset: Arc<BtrfsDataset>,
    container: Arc<BtrfsContainer>,
}

impl LocalSyncJob {
    fn work(&self) -> Result<()> {
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
}
