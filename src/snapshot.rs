use crate::model::btrfs::{BtrfsDataset, BtrfsPool, full_path, SubvolumeEntity};
use crate::btrfs::{self, QueriedFilesystem::*};
use anyhow::Result;
use std::path::{PathBuf, Path};
use chrono::{DateTime, Utc};

pub fn local_snapshot(pool: &BtrfsPool, dataset: &BtrfsDataset) -> Result<()> {

    let fs = btrfs::Filesystem::query_uuid(&pool.uuid)?.unwrap_mounted()?;
    let subvol = fs.subvolume_by_uuid(dataset.uuid())?;
    
    let now = Utc::now();
    let snapshot_path = dataset.snapshot_container_path().join(now.format("%FT%H-%M-%SZ").to_string());
    fs.snapshot_subvolume(&subvol, &snapshot_path)?;

    Ok(())
}
