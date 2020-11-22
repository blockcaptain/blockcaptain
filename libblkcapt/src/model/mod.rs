pub mod entities;
pub mod storage;

use anyhow::{anyhow, Result};
use entities::{
    BtrfsContainerEntity, BtrfsDatasetEntity, BtrfsPoolEntity, HealthchecksObserverEntity, ResticContainerEntity,
    SnapshotSyncEntity,
};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::{fmt::Debug, iter::repeat};
use strum_macros::Display;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Entities {
    pub btrfs_pools: Vec<BtrfsPoolEntity>,
    pub snapshot_syncs: Vec<SnapshotSyncEntity>,
    pub observers: Vec<HealthchecksObserverEntity>,
    pub restic_containers: Vec<ResticContainerEntity>,
}

impl Entities {
    pub fn attach_pool(&mut self, pool: BtrfsPoolEntity) -> Result<()> {
        self.pool_by_name(pool.name())
            .map_or(Ok(()), |p| Err(anyhow!("Pool name '{}' already exists.", p.name())))?;
        self.pool_by_uuid(pool.uuid)
            .map_or(Ok(()), |p| Err(anyhow!("uuid already used by pool {}.", p.name())))?;
        self.pool_by_mountpoint(&pool.mountpoint_path).map_or(Ok(()), |p| {
            Err(anyhow!("mountpoint already used by pool {}.", p.name()))
        })?;

        self.btrfs_pools.push(pool);
        Ok(())
    }

    pub fn attach_observer(&mut self, observer: HealthchecksObserverEntity) -> Result<()> {
        entity_by_name(&self.observers, observer.name())
            .map_or(Ok(()), |o| Err(anyhow!("Observer name '{}' already exists.", o.name())))?;

        if let Some(other) = self.observers.iter().find(|o| o.custom_url == observer.custom_url) {
            let other_type = match observer.custom_url {
                Some(_) => "same custom Healthchecks instance",
                None => "Healthchecks.io service",
            };
            slog_scope::warn!(
                "Observer {} already uses the {}. It is not neccessary to create multiple observers \
                that report to the same instance.",
                other.name(),
                other_type
            );
        }

        self.observers.push(observer);
        Ok(())
    }

    pub fn pool_by_uuid(&self, uuid: Uuid) -> Option<&BtrfsPoolEntity> {
        self.btrfs_pools.iter().find(|p| p.uuid == uuid)
    }

    pub fn pool_by_mountpoint(&self, path: &Path) -> Option<&BtrfsPoolEntity> {
        self.btrfs_pools.iter().find(|p| p.mountpoint_path == path)
    }

    pub fn pool_by_name(&self, name: &str) -> Option<&BtrfsPoolEntity> {
        entity_by_name(&self.btrfs_pools, name)
    }

    pub fn pool(&self, id: Uuid) -> Option<&BtrfsPoolEntity> {
        entity_by_id(self.btrfs_pools.iter(), id)
    }

    pub fn observer(&self, id: Uuid) -> Option<&HealthchecksObserverEntity> {
        entity_by_id(self.observers.iter(), id)
    }

    pub fn snapshot_sync(&self, id: Uuid) -> Option<&SnapshotSyncEntity> {
        entity_by_id(self.snapshot_syncs.iter(), id)
    }

    pub fn datasets(&self) -> impl Iterator<Item = EntityPath2<BtrfsDatasetEntity, BtrfsPoolEntity>> {
        self.btrfs_pools
            .iter()
            .flat_map(|p| p.datasets.iter().zip(repeat(p)))
            .map(|t| EntityPath2 {
                entity: t.0,
                parent: t.1,
            })
    }

    pub fn dataset(&self, id: Uuid) -> Option<EntityPath2<BtrfsDatasetEntity, BtrfsPoolEntity>> {
        entity_by_id(self.datasets(), id)
    }

    pub fn containers(&self) -> impl Iterator<Item = EntityPath2<BtrfsContainerEntity, BtrfsPoolEntity>> {
        self.btrfs_pools
            .iter()
            .flat_map(|p| p.containers.iter().zip(repeat(p)))
            .map(|t| EntityPath2 {
                entity: t.0,
                parent: t.1,
            })
    }

    pub fn container(&self, id: Uuid) -> Option<EntityPath2<BtrfsContainerEntity, BtrfsPoolEntity>> {
        entity_by_id(self.containers(), id)
    }

    pub fn restic_container(&self, id: Uuid) -> Option<&ResticContainerEntity> {
        entity_by_id(self.restic_containers.iter(), id)
    }

    pub fn pool_by_mountpoint_mut(&mut self, path: &Path) -> Option<&mut BtrfsPoolEntity> {
        self.btrfs_pools.iter_mut().find(|p| p.mountpoint_path == path)
    }
}

#[derive(Debug)]
pub struct EntityPath1<'a, T: Entity> {
    pub entity: &'a T,
}

#[derive(Debug)]
pub struct EntityPath2<'a, T: Entity, U: Entity> {
    pub entity: &'a T,
    pub parent: &'a U,
}

impl<'a, T: Entity, U: Entity> EntityPath2<'a, T, U> {
    pub fn into_id_path(self) -> EntityIdPath2 {
        EntityIdPath2 {
            entity: self.entity.id(),
            parent: self.parent.id(),
        }
    }
}

impl<'a, T: Entity> Entity for EntityPath1<'a, T> {
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

impl<'a, T: Entity> AsRef<dyn Entity + 'a> for EntityPath1<'a, T> {
    fn as_ref(&self) -> &(dyn Entity + 'a) {
        self
    }
}

impl<'a, T: Entity, U: Entity> Entity for EntityPath2<'a, T, U> {
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

impl<'a, T: Entity, U: Entity> AsRef<dyn Entity + 'a> for EntityPath2<'a, T, U> {
    fn as_ref(&self) -> &(dyn Entity + 'a) {
        self
    }
}

pub trait EntityPath: Entity {
    fn path(&self) -> String;
}

pub struct EntityIdPath2 {
    pub entity: Uuid,
    pub parent: Uuid,
}

impl<'a, T: Entity> EntityPath for EntityPath1<'a, T> {
    fn path(&self) -> String {
        self.entity.name().to_owned()
    }
}

impl<'a, T: Entity, U: Entity> EntityPath for EntityPath2<'a, T, U> {
    fn path(&self) -> String {
        format!("{}/{}", self.parent.name(), self.entity.name())
    }
}

impl<'a, T: Entity + EntityStatic, U: Entity> EntityStatic for EntityPath2<'a, T, U> {
    fn entity_type_static() -> EntityType {
        T::entity_type_static()
    }
}

impl<T: Entity + EntityStatic> EntityStatic for &T {
    fn entity_type_static() -> EntityType {
        T::entity_type_static()
    }
}

#[derive(Display)]
#[strum(serialize_all = "snake_case")]
pub enum EntityType {
    Pool,
    Dataset,
    Container,
    SnapshotSync,
    Observer,
    //ResticContainer,
}

pub trait Entity: Debug {
    fn name(&self) -> &str;
    fn id(&self) -> Uuid;
    fn entity_type(&self) -> EntityType;
}

pub trait EntityStatic {
    fn entity_type_static() -> EntityType;
}

pub fn entity_by_name<'a, T: Entity>(vec: &'a [T], name: &str) -> Option<&'a T> {
    vec.iter().find(|e| e.name() == name)
}

pub fn entity_by_id_mut<T: Entity>(vec: &mut [T], id: Uuid) -> Option<&mut T> {
    vec.iter_mut().find(|e| e.id() == id)
}

pub fn entity_by_name_mut<'a, T: Entity>(vec: &'a mut Vec<T>, name: &str) -> Option<&'a mut T> {
    vec.iter_mut().find(|e| e.name() == name)
}

pub fn entity_by_id<'a, T: AsRef<dyn Entity + 'a>>(mut iter: impl Iterator<Item = T>, id: Uuid) -> Option<T> {
    iter.find(|e| e.as_ref().id() == id)
}

pub fn entity_by_name_or_id<'a, T: AsRef<dyn Entity + 'a> + EntityStatic>(
    iter: impl Iterator<Item = T>,
    name_or_id: &str,
) -> Result<T> {
    let mut matches = iter
        .filter(|e| e.as_ref().id().to_string().starts_with(name_or_id) || e.as_ref().name() == name_or_id)
        .collect::<Vec<_>>();
    match matches.len() {
        0 => Err(anyhow!("{} '{}' not found", T::entity_type_static(), name_or_id)),
        1 => Ok(matches.pop().unwrap()),
        _ => Err(anyhow!(
            "'{}' identifies multiple {}s",
            T::entity_type_static(),
            name_or_id
        )),
    }
}
