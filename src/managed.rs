use uuid::Uuid;
use std::path::{PathBuf, Path};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct BtrfsPool 
{
    pub mountpoint_path: PathBuf,
    pub name: String,
    pub uuid: Uuid,
    pub uuid_subs: Vec<Uuid>
}


#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Entities 
{
    pub btrfs_pools: Vec<BtrfsPool>
}

impl Entities {
    pub fn pool_by_uuid(&self, uuid: &Uuid) -> Option<&BtrfsPool> {
        self.btrfs_pools.iter().filter(|p| p.uuid == *uuid).next()
    }

    pub fn pool_by_mountpoint(&self, path: &Path) -> Option<&BtrfsPool> {
        self.btrfs_pools.iter().filter(|p| p.mountpoint_path == path).next()
    }
}