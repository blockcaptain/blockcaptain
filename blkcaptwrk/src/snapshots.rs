use anyhow::Result;
use chrono::{DateTime, Utc};
use libblkcapt::core::{retention::RetentionEvaluation, BtrfsSnapshot, SnapshotHandle};
use slog::{info, trace, Logger};
use std::collections::HashSet;
use uuid::Uuid;
use xactor::message;

pub fn find_ready<'a>(
    dataset_snapshots: &'a [SnapshotHandle],
    container_snapshots: &[SnapshotHandle],
    find_mode: FindMode,
) -> Option<&'a SnapshotHandle> {
    if dataset_snapshots.is_empty() {
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
        return None;
    }

    match find_mode {
        FindMode::Earliest => to_send.first(),
        FindMode::Latest => to_send.last(),
        FindMode::LatestBefore(end_cycle) => to_send.iter().rev().find(|s| s.datetime < end_cycle),
        FindMode::EarliestBefore(end_cycle) => to_send.iter().find(|s| s.datetime < end_cycle),
    }
}

pub enum FindMode {
    Earliest,
    Latest,
    LatestBefore(DateTime<Utc>),
    EarliestBefore(DateTime<Utc>),
}

pub fn find_parent<'a>(
    child_snapshot: &SnapshotHandle,
    dataset_snapshots: &'a [SnapshotHandle],
    container_snapshots: &[SnapshotHandle],
) -> Option<&'a SnapshotHandle> {
    if dataset_snapshots.is_empty() {
        return None;
    }

    if container_snapshots.is_empty() {
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

#[message()]
#[derive(Clone)]
pub struct PruneMessage();

pub fn prune_snapshots<T: BtrfsSnapshot>(evaluation: RetentionEvaluation<T>, log: &Logger) -> Result<()> {
    for snapshot in evaluation.keep_interval_buckets.iter().flat_map(|b| b.snapshots.iter()) {
        trace!(log, "Keeping snapshot {} reason: in retention interval.", snapshot);
    }

    for snapshot in evaluation.keep_minimum_snapshots.iter() {
        trace!(log, "Keeping snapshot {} reason: keep minimum newest.", snapshot);
    }

    for snapshot in evaluation.drop_snapshots {
        info!(
            log,
            "Snapshot {} is being pruned because it did not meet any retention criteria.", snapshot
        );
        snapshot.delete()?;
    }

    Ok(())
}

#[message(result = "ContainerSnapshotsResponse")]
pub struct GetContainerSnapshotsMessage {
    pub source_dataset_id: Uuid,
}

pub struct ContainerSnapshotsResponse {
    pub snapshots: Vec<SnapshotHandle>,
}
