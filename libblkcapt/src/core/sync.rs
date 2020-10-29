use std::collections::HashSet;

use chrono::{DateTime, Utc};
use log::*;

use super::{BtrfsContainerSnapshotHandle, BtrfsDatasetSnapshotHandle};

pub fn find_ready<'a>(
    dataset_snapshots: &'a [BtrfsDatasetSnapshotHandle],
    container_snapshots: &[BtrfsContainerSnapshotHandle],
    find_mode: FindMode,
) -> Option<&'a BtrfsDatasetSnapshotHandle> {
    if dataset_snapshots.is_empty() {
        trace!("No snapshots in the dataset.");
        return None;
    }

    let container_latest = container_snapshots.last().map(|s| s.datetime);
    let (_parents, to_send) = match container_latest {
        Some(container_latest) => dataset_snapshots.split_at(
            dataset_snapshots
                .iter()
                .take_while(|s| s.datetime <= container_latest)
                .count(),
        ),
        // INVARIANT: Len > 0 checked above.
        None => dataset_snapshots.split_at(dataset_snapshots.len() - 1),
    };

    if to_send.is_empty() {
        trace!("No new snapshots are ready to send.");
        return None;
    }

    match find_mode {
        FindMode::Latest => to_send.last(),
        FindMode::LatestBefore(end_cycle) => to_send.iter().rev().find(|s| s.datetime < end_cycle),
        FindMode::EarliestBefore(end_cycle) => to_send.iter().find(|s| s.datetime < end_cycle),
    }
}

pub enum FindMode {
    Latest,
    LatestBefore(DateTime<Utc>),
    EarliestBefore(DateTime<Utc>),
}

pub fn find_parent<'a>(
    child_snapshot: &BtrfsDatasetSnapshotHandle,
    dataset_snapshots: &'a [BtrfsDatasetSnapshotHandle],
    container_snapshots: &[BtrfsContainerSnapshotHandle],
) -> Option<&'a BtrfsDatasetSnapshotHandle> {
    if dataset_snapshots.is_empty() {
        trace!("No snapshots in the dataset.");
        return None;
    }

    if container_snapshots.is_empty() {
        trace!("No snapshots in the container.");
        return None;
    }

    // logic needs to handle walking source snapshots for restore chains.

    let eligbile_source = dataset_snapshots
        .iter()
        .map(|s| s.datetime)
        .filter(|d| d < &child_snapshot.datetime)
        .collect::<HashSet<_>>();
    let eligbile_destination = container_snapshots
        .iter()
        .map(|s| s.datetime)
        .filter(|d| d < &child_snapshot.datetime)
        .collect::<HashSet<_>>();
    let eligbile = eligbile_source.intersection(&eligbile_destination).last();
    eligbile.and_then(|d| dataset_snapshots.iter().find(|s| &s.datetime == d))
}

// impl LocalSyncJob {
//     fn work(&self) -> Result<()> {
//         let (dataset_snapshots, mut container_snapshots, send_snapshots) = self.ready_snapshots()?;

//         trace!("Identified {} snapshots to send.", send_snapshots.len());
//         for snapshot_index in send_snapshots {
//             let source_snapshot = &dataset_snapshots[snapshot_index];
//             trace!("Sending snapshot {:?}.", source_snapshot);

//             let mut maybe_parent_snapshot = None;
//             if snapshot_index > 0 {
//                 info!("Local snapshot has predecessors. Searching for viable parent for delta send...");
//                 let previous_snapshot = &dataset_snapshots[snapshot_index - 1];
//                 if source_snapshot.parent_uuid() == previous_snapshot.parent_uuid() {
//                     info!("Predecessor has same parent as this snapshot, parent identified.");
//                     maybe_parent_snapshot = Some(previous_snapshot)
//                 } else if source_snapshot.parent_uuid().expect("Should always have parent here.") == self.dataset.uuid()
//                 {
//                     info!("Predecessor does not have same parent as this snapshot, but it is a snapshot of the active dataset.");
//                     if let Some(dataset_parent_uuid) = self.dataset.parent_uuid() {
//                         info!("Active dataset has a parent.");
//                         maybe_parent_snapshot = dataset_snapshots.iter().find(|s| s.uuid() == dataset_parent_uuid); //does this find oldest parent due to sorting??P
//                         if maybe_parent_snapshot.is_some() {
//                             info!("Parent exists in local snapshots, parent identified.");
//                         } else {
//                             info!("Parent could not be found in local snapshots. Delta send will not be possible.");
//                         }
//                     } else {
//                         info!("Active dataset does not have a parent. Delta send will not be possible.");
//                     }
//                 } else {
//                     info!("Could not identify a viable parent. Delta send will not be possible.");
//                 }

//                 if let Some(candidate) = maybe_parent_snapshot {
//                     if container_snapshots
//                         .iter()
//                         .find(|s| s.received_uuid() == candidate.uuid())
//                         .is_none()
//                     {
//                         if let Some(candidate_received_from) = candidate.received_uuid() {
//                             info!("Parent appears to be a restored snapshot.");
//                             if container_snapshots
//                                 .iter()
//                                 .find(|s| s.received_uuid() == candidate_received_from)
//                                 .is_none()
//                             {
//                                 info!("Restored snapshot does not exist in the destination container. Delta send will not be possible.");
//                                 maybe_parent_snapshot = None;
//                             }
//                         } else {
//                             info!(
//                                 "Parent does not exist in the destination container. Delta send will not be possible."
//                             );
//                             maybe_parent_snapshot = None;
//                         }
//                     }
//                 }
//             } else {
//                 info!("Snapshot is first local snapshot. Delta send will not be possible.");
//             }
//             let new_snapshot = match maybe_parent_snapshot {
//                 Some(parent_snapshot) => {
//                     info!("Sending delta snapshot.");
//                     core::transfer_delta_snapshot(parent_snapshot, source_snapshot, &self.container)
//                 }
//                 None => {
//                     info!("Sending full snapshot.");
//                     core::transfer_full_snapshot(source_snapshot, &self.container)
//                 }
//             }?;
//             container_snapshots.push(new_snapshot);
//         }
//         Ok(())
//     }
// }
