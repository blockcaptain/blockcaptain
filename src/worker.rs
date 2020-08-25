use crate::model::entities::{BtrfsDatasetEntity, BtrfsPoolEntity, SubvolumeEntity};
use anyhow::Result;
use chrono::{DateTime, Utc, Duration};
use crate::snapshot;
use crate::sys::btrfs;

pub trait Job {
    fn run(&self) -> Result<()>;
    fn is_ready(&self) -> Result<bool>;
}

pub struct LocalSnapshotJob<'a> {
    pool: &'a BtrfsPoolEntity, 
    dataset: &'a BtrfsDatasetEntity,
}

impl<'a> LocalSnapshotJob<'a> {
    pub fn new(pool: &'a BtrfsPoolEntity, dataset: &'a BtrfsDatasetEntity) -> Self {
        Self {
            pool, dataset
        }
    }
}

impl<'a>  Job for LocalSnapshotJob<'a>  {
    fn run(&self) -> Result<()> {
        snapshot::local_snapshot(self.pool, self.dataset)
    }

    fn is_ready(&self) -> Result<bool> {
        let fs = btrfs::Filesystem::query_uuid(&self.pool.uuid)?.unwrap_mounted()?;
        let subvol = fs.subvolume_by_uuid(self.dataset.uuid())?;
        let latest = self.dataset.latest_snapshot(&subvol)?;
        Ok(if let Some(latest_datetime) = latest {
            let next_datetime = latest_datetime + Duration::hours(1);
            Utc::now() >= next_datetime
        } else {
            true
        })
    }
}