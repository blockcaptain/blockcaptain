use serde::{Deserialize, Serialize};
use std::path::Path;
use uuid::Uuid;
pub mod btrfs;
use anyhow::{anyhow, Result};
use btrfs::BtrfsPool;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Entities {
    btrfs_pools: Vec<BtrfsPool>,
}

impl Entities {
    pub fn attach_pool(&mut self, pool: BtrfsPool) -> Result<()> {
        self.pool_by_uuid(&pool.uuid)
            .map_or(Ok(()), |p| Err(anyhow!("uuid already used by pool {}.", p.name)))?;
        self.pool_by_mountpoint(&pool.mountpoint_path)
            .map_or(Ok(()), |p| Err(anyhow!("mountpoint already used by pool {}.", p.name)))?;

        self.btrfs_pools.push(pool);
        Ok(())
    }

    fn pool_by_uuid(&self, uuid: &Uuid) -> Option<&BtrfsPool> {
        self.btrfs_pools.iter().filter(|p| p.uuid == *uuid).next()
    }

    fn pool_by_mountpoint(&self, path: &Path) -> Option<&BtrfsPool> {
        self.btrfs_pools.iter().filter(|p| p.mountpoint_path == path).next()
    }

    pub fn pool_by_mountpoint_mut(&mut self, path: &Path) -> Option<&mut BtrfsPool> {
        self.btrfs_pools.iter_mut().filter(|p| p.mountpoint_path == path).next()
    }
}

trait Entity {
    fn name(&self) -> &str;
}