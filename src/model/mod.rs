use serde::{Deserialize, Serialize};
use std::path::Path;
use uuid::Uuid;
pub mod action;
pub mod btrfs;
use action::SnapshotSync;
use anyhow::{anyhow, Result};
use btrfs::{BtrfsContainer, BtrfsDataset, BtrfsPool};
use std::iter::repeat;
use strum_macros::Display;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Entities {
    btrfs_pools: Vec<BtrfsPool>,
    snapshot_syncs: Vec<SnapshotSync>,
}

impl Entities {
    pub fn attach_pool(&mut self, pool: BtrfsPool) -> Result<()> {
        self.pool(&pool.name)
            .map_or(Ok(()), |p| Err(anyhow!("Pool name '{}' already exists.", p.name)))?;
        self.pool_by_uuid(&pool.uuid)
            .map_or(Ok(()), |p| Err(anyhow!("uuid already used by pool {}.", p.name)))?;
        self.pool_by_mountpoint(&pool.mountpoint_path)
            .map_or(Ok(()), |p| Err(anyhow!("mountpoint already used by pool {}.", p.name)))?;

        self.btrfs_pools.push(pool);
        Ok(())
    }

    fn pool_by_uuid(&self, uuid: &Uuid) -> Option<&BtrfsPool> {
        self.btrfs_pools.iter().find(|p| p.uuid == *uuid)
    }

    fn pool_by_mountpoint(&self, path: &Path) -> Option<&BtrfsPool> {
        self.btrfs_pools.iter().find(|p| p.mountpoint_path == path)
    }

    fn pool(&self, name: &str) -> Option<&BtrfsPool> {
        self.btrfs_pools.iter().find(|p| p.name == name)
    }

    pub fn dataset_by_id(&self, id: &Uuid) -> Option<(&BtrfsDataset, &BtrfsPool)> {
        self.btrfs_pools
            .iter()
            .flat_map(|p| p.datasets.iter().zip(repeat(p)))
            .find(|p| p.0.id() == *id)
    }

    pub fn container_by_id(&self, id: &Uuid) -> Option<(&BtrfsContainer, &BtrfsPool)> {
        self.btrfs_pools
            .iter()
            .flat_map(|p| p.containers.iter().zip(repeat(p)))
            .find(|p| p.0.id() == *id)
    }

    pub fn pool_by_mountpoint_mut(&mut self, path: &Path) -> Option<&mut BtrfsPool> {
        self.btrfs_pools.iter_mut().find(|p| p.mountpoint_path == path)
    }

    pub fn snapshot_syncs(&self) -> impl Iterator<Item = &SnapshotSync> {
        self.snapshot_syncs.iter()
    }
}

#[derive(Display)]
pub enum EntityType {
    Pool,
    Dataset,
    Container,
    SnapshotSync,
}

pub trait Entity {
    fn name(&self) -> &str;
    fn id(&self) -> Uuid;
    fn entity_type(&self) -> EntityType;
}
