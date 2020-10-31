use anyhow::Result;
use libblkcapt::core::{retention::RetentionEvaluation, BtrfsSnapshot};
use slog::{info, trace, Logger};
use xactor::message;

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

