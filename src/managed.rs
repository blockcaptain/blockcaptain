use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
pub struct BtrfsPool {
    pub mountpoint_path: PathBuf,
    pub name: String,
    pub uuid: Uuid,
    pub uuid_subs: Vec<Uuid>,

    pub datasets: Vec<BtrfsDataset>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BtrfsDataset {
    pub name: PathBuf,
    pub uuid: Uuid,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Entities {
    pub btrfs_pools: Vec<BtrfsPool>,
}

impl Entities {
    pub fn pool_by_uuid(&self, uuid: &Uuid) -> Option<&BtrfsPool> {
        self.btrfs_pools.iter().filter(|p| p.uuid == *uuid).next()
    }

    pub fn pool_by_mountpoint(&self, path: &Path) -> Option<&BtrfsPool> {
        self.btrfs_pools.iter().filter(|p| p.mountpoint_path == path).next()
    }

    pub fn pool_by_mountpoint_mut(&mut self, path: &Path) -> Option<&mut BtrfsPool> {
        self.btrfs_pools.iter_mut().filter(|p| p.mountpoint_path == path).next()
    }
}
