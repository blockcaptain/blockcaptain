use chrono::{DateTime, Utc};

use super::{BtrfsDatasetSnapshot, BtrfsSnapshot};

pub fn ready_snapshots(mut snapshots: Vec<BtrfsDatasetSnapshot>, container_latest: Option<DateTime<Utc>>) -> Vec<BtrfsDatasetSnapshot> {
    snapshots.sort_unstable_by_key(|s| s.datetime());

    match container_latest {
        Some(latest) => {
            snapshots
            .into_iter()
            .skip_while(|s| s.datetime() <= latest)
            .collect()
        }
        None => snapshots.pop().into_iter().collect()
    }
}

