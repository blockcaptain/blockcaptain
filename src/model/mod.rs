pub mod entities;
pub mod storage;

use anyhow::{anyhow, Result};
use entities::{BtrfsContainerEntity, BtrfsDatasetEntity, BtrfsPoolEntity, SnapshotSyncEntity, HealthchecksObserverEntity};
use serde::{Deserialize, Serialize};
use std::iter::repeat;
use std::{borrow::Borrow, path::Path};
use strum_macros::Display;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Entities {
    pub btrfs_pools: Vec<BtrfsPoolEntity>,
    pub snapshot_syncs: Vec<SnapshotSyncEntity>,
    pub observers: Vec<HealthchecksObserverEntity>,
}

impl Entities {
    pub fn attach_pool(&mut self, pool: BtrfsPoolEntity) -> Result<()> {
        self.pool(&pool.name())
            .map_or(Ok(()), |p| Err(anyhow!("Pool name '{}' already exists.", p.name())))?;
        self.pool_by_uuid(&pool.uuid)
            .map_or(Ok(()), |p| Err(anyhow!("uuid already used by pool {}.", p.name())))?;
        self.pool_by_mountpoint(&pool.mountpoint_path)
            .map_or(Ok(()), |p| Err(anyhow!("mountpoint already used by pool {}.", p.name())))?;

        self.btrfs_pools.push(pool);
        Ok(())
    }

    pub fn pool_by_uuid(&self, uuid: &Uuid) -> Option<&BtrfsPoolEntity> {
        self.btrfs_pools.iter().find(|p| p.uuid == *uuid)
    }

    pub fn pool_by_mountpoint(&self, path: &Path) -> Option<&BtrfsPoolEntity> {
        self.btrfs_pools.iter().find(|p| p.mountpoint_path == path)
    }

    pub fn pools(&self) -> impl Iterator<Item = &BtrfsPoolEntity> {
        self.btrfs_pools.iter()
    }

    pub fn pool(&self, name: &str) -> Option<&BtrfsPoolEntity> {
        self.btrfs_pools.iter().find(|p| p.name() == name)
    }

    pub fn datasets(&self) -> impl Iterator<Item = EntityPath<BtrfsDatasetEntity, BtrfsPoolEntity>> {
        self.btrfs_pools
            .iter()
            .flat_map(|p| p.datasets.iter().zip(repeat(p)))
            .map(|t| EntityPath {
                entity: t.0,
                parent: t.1,
            })
    }

    pub fn dataset_by_id(&self, id: &Uuid) -> Option<(&BtrfsDatasetEntity, &BtrfsPoolEntity)> {
        self.btrfs_pools
            .iter()
            .flat_map(|p| p.datasets.iter().zip(repeat(p)))
            .find(|p| p.0.id() == *id)
    }

    pub fn container_by_id(&self, id: &Uuid) -> Option<(&BtrfsContainerEntity, &BtrfsPoolEntity)> {
        self.btrfs_pools
            .iter()
            .flat_map(|p| p.containers.iter().zip(repeat(p)))
            .find(|p| p.0.id() == *id)
    }

    pub fn pool_by_mountpoint_mut(&mut self, path: &Path) -> Option<&mut BtrfsPoolEntity> {
        self.btrfs_pools.iter_mut().find(|p| p.mountpoint_path == path)
    }

    pub fn snapshot_syncs(&self) -> impl Iterator<Item = &SnapshotSyncEntity> {
        self.snapshot_syncs.iter()
    }
}
pub struct EntityPath<'a, T: Entity, U: Entity> {
    pub entity: &'a T,
    pub parent: &'a U,
}

impl<'a, T: Entity, U: Entity> EntityPath<'a, T, U> {
    pub fn to_id_path(self) -> EntityIdPath {
        EntityIdPath {
            entity: self.entity.id(),
            parent: self.parent.id(),
        }
    }
}

impl<'a, T: Entity, U: Entity> Entity for EntityPath<'a, T, U> {
    fn name(&self) -> &str {
        self.entity.name()
    }

    fn id(&self) -> Uuid {
        self.entity.id()
    }

    fn entity_type(&self) -> EntityType {
        self.entity.entity_type()
    }
}

impl<'a, T: Entity, U: Entity> AsRef<dyn Entity + 'a> for EntityPath<'a, T, U> {
    fn as_ref(&self) -> &(dyn Entity + 'a) {
        self
    }
}

pub struct EntityIdPath {
    pub entity: Uuid,
    pub parent: Uuid,
}

#[derive(Display)]
pub enum EntityType {
    Pool,
    Dataset,
    Container,
    SnapshotSync,
    Observer,
}

pub trait Entity {
    fn name(&self) -> &str;
    fn id(&self) -> Uuid;
    fn entity_type(&self) -> EntityType;
}

pub fn entity_by_id<T: Entity>(vec: &Vec<T>, id: Uuid) -> Option<&T> {
    vec.iter().find(|e| e.id() == id)
}

pub fn entity_by_name<'a, T: Entity>(vec: &'a Vec<T>, name: &str) -> Option<&'a T> {
    vec.iter().find(|e| e.name() == name)
}

pub fn entity_by_id_mut<T: Entity>(vec: &mut Vec<T>, id: Uuid) -> Option<&mut T> {
    vec.iter_mut().find(|e| e.id() == id)
}

pub fn entity_by_name_mut<'a, T: Entity>(vec: &'a mut Vec<T>, name: &str) -> Option<&'a mut T> {
    vec.iter_mut().find(|e| e.name() == name)
}

pub fn entity_by_name_or_id<'a, T: AsRef<dyn Entity + 'a>>(
    iter: impl Iterator<Item = T>,
    name_or_id: &str,
) -> Result<Option<T>> {
    let mut matches = iter
        .filter(|e| e.as_ref().id().to_string().starts_with(name_or_id) || e.as_ref().name() == name_or_id)
        .collect::<Vec<_>>();
    match matches.len() {
        0 => Ok(None),
        1 => Ok(Some(matches.remove(0))),
        _ => Err(anyhow!("multiple matches")),
    }
}

