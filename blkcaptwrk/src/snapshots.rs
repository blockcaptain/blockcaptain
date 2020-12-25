use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use libblkcapt::{
    core::{
        retention::{evaluate_retention, RetentionEvaluation},
        BtrfsSnapshot, Snapshot, SnapshotHandle,
    },
    model::entities::RetentionRuleset,
};
use slog::{debug, info, trace, Logger};
use std::collections::HashSet;
use uuid::Uuid;
use xactor::message;

use crate::actorbase::log_result;

pub fn find_ready<'a>(
    dataset_snapshots: &'a [SnapshotHandle], container_snapshots: &[SnapshotHandle], find_mode: FindMode,
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
    child_snapshot: &SnapshotHandle, dataset_snapshots: &'a [SnapshotHandle], container_snapshots: &[SnapshotHandle],
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
pub struct PruneMessage;

pub fn log_evaluation<T: Snapshot>(evaluation: &RetentionEvaluation<T>, log: &Logger) {
    for snapshot in evaluation.keep_interval_buckets.iter().flat_map(|b| b.snapshots.iter()) {
        trace!(log, "Keeping snapshot {} reason: in retention interval.", snapshot);
    }

    for snapshot in evaluation.keep_minimum_snapshots.iter() {
        trace!(log, "Keeping snapshot {} reason: keep minimum newest.", snapshot);
    }

    for snapshot in evaluation.drop_snapshots.iter() {
        info!(
            log,
            "Snapshot {} is being pruned because it did not meet any retention criteria.", snapshot
        );
    }
}

pub fn delete_snapshots<T: BtrfsSnapshot>(snapshots: &[&T], log: &Logger) -> HashSet<DateTime<Utc>> {
    snapshots
        .iter()
        .filter_map(|s| {
            let result = s.delete();
            log_result(log, &result);
            result.map(|_| s.datetime()).ok()
        })
        .collect()
}

pub fn clear_deleted<T: Snapshot>(snapshots: &mut Vec<T>, deleted: HashSet<DateTime<Utc>>) {
    snapshots.retain(|s| !deleted.contains(&s.datetime()));
}

pub fn prune_btrfs_snapshots<T: BtrfsSnapshot>(
    snapshots: &mut Vec<T>, holds: &[Uuid], rules: &RetentionRuleset, log: &Logger,
) -> usize {
    let evaluation = {
        let mut eval = evaluate_retention(snapshots, rules);
        eval.drop_snapshots.retain(|s| {
            let retain = !holds.contains(&s.uuid());
            if !retain {
                debug!(log, "Snapshot {} is marked for deletion, but is currently held.", s);
            }
            retain
        });
        eval
    };
    log_evaluation(&evaluation, log);
    let deleted = delete_snapshots(&evaluation.drop_snapshots, log);
    let failed_deletes = evaluation.drop_snapshots.len() - deleted.len();
    clear_deleted(snapshots, deleted);
    failed_deletes
}

pub fn failed_snapshot_deletes_as_result(failed_count: usize) -> Result<()> {
    if failed_count == 0 {
        Ok(())
    } else {
        Err(anyhow!("{} snapshots failed to delete", failed_count))
    }
}

#[message(result = "ContainerSnapshotsResponse")]
pub struct GetContainerSnapshotsMessage {
    pub source_dataset_id: Uuid,
}

pub struct ContainerSnapshotsResponse {
    pub snapshots: Vec<SnapshotHandle>,
}
