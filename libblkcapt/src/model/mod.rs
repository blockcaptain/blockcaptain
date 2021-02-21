pub mod entities;
pub mod storage;

use crate::parsing::parse_uuid;
use anyhow::{anyhow, Result};
use entities::{
    BtrfsContainerEntity, BtrfsDatasetEntity, BtrfsPoolEntity, HealthchecksObserverEntity, ResticContainerEntity,
    SnapshotSyncEntity,
};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, iter::repeat};
use std::{path::Path, str::FromStr};
use strum_macros::Display;
use strum_macros::EnumString;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Default, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EntityId(Uuid);

impl EntityId {
    fn new() -> Self {
        EntityId(Uuid::new_v4())
    }
}

impl FromStr for EntityId {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_uuid(value).map(EntityId)
    }
}

impl std::fmt::Display for EntityId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.to_hyphenated_ref())
    }
}

impl From<EntityId> for Uuid {
    fn from(id: EntityId) -> Self {
        id.0
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Entities {
    pub btrfs_pools: Vec<BtrfsPoolEntity>,
    pub snapshot_syncs: Vec<SnapshotSyncEntity>,
    pub observers: Vec<HealthchecksObserverEntity>,
    pub restic_containers: Vec<ResticContainerEntity>,
}

impl Entities {
    pub(super) fn post_deserialize(&mut self) {
        for pool in self.btrfs_pools.iter_mut() {
            pool.post_deserialize()
        }
    }

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

    pub fn pool(&self, id: EntityId) -> Option<&BtrfsPoolEntity> {
        entity_by_id(self.btrfs_pools.iter(), id)
    }

    pub fn observer(&self, id: EntityId) -> Option<&HealthchecksObserverEntity> {
        entity_by_id(self.observers.iter(), id)
    }

    pub fn snapshot_sync(&self, id: EntityId) -> Option<&SnapshotSyncEntity> {
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

    pub fn dataset(&self, id: EntityId) -> Option<EntityPath2<BtrfsDatasetEntity, BtrfsPoolEntity>> {
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

    pub fn container(&self, id: EntityId) -> Option<EntityPath2<BtrfsContainerEntity, BtrfsPoolEntity>> {
        entity_by_id(self.containers(), id)
    }

    pub fn any_container(&self, id: EntityId) -> Option<AnyContainer> {
        entity_by_id(self.containers(), id)
            .map(|r| AnyContainer::Btrfs(r.entity))
            .or_else(|| entity_by_id(self.restic_containers.iter(), id).map(|r| AnyContainer::Restic(r)))
    }

    pub fn restic_container(&self, id: EntityId) -> Option<&ResticContainerEntity> {
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

impl<'a, T: Entity> EntityPath1<'a, T> {
    pub fn into_id_path(self) -> EntityIdPath1 {
        EntityIdPath1 {
            entity: self.entity.id(),
        }
    }
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

    fn id(&self) -> EntityId {
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

    fn id(&self) -> EntityId {
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

pub struct EntityIdPath1 {
    pub entity: EntityId,
}

pub struct EntityIdPath2 {
    pub entity: EntityId,
    pub parent: EntityId,
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
}

#[derive(Display)]
#[strum(serialize_all = "snake_case")]
pub enum AnyContainer<'a> {
    Btrfs(&'a BtrfsContainerEntity),
    Restic(&'a ResticContainerEntity),
}

pub trait Entity: Debug {
    fn name(&self) -> &str;
    fn id(&self) -> EntityId;
    fn entity_type(&self) -> EntityType;
}

pub trait EntityStatic {
    fn entity_type_static() -> EntityType;
}

pub fn entity_by_name<'a, T: Entity>(vec: &'a [T], name: &str) -> Option<&'a T> {
    vec.iter().find(|e| e.name() == name)
}

pub fn entity_by_id_mut<T: Entity>(vec: &mut [T], id: EntityId) -> Option<&mut T> {
    vec.iter_mut().find(|e| e.id() == id)
}

pub fn entity_by_name_mut<'a, T: Entity>(vec: &'a mut Vec<T>, name: &str) -> Option<&'a mut T> {
    vec.iter_mut().find(|e| e.name() == name)
}

pub fn entity_by_id<'a, T: AsRef<dyn Entity + 'a>>(mut iter: impl Iterator<Item = T>, id: EntityId) -> Option<T> {
    iter.find(|e| e.as_ref().id() == id)
}

pub fn entity_by_name_or_id<'a, T: AsRef<dyn Entity + 'a> + EntityStatic>(
    iter: impl Iterator<Item = T>, name_or_id: &str,
) -> Result<T> {
    let mut matches = iter
        .filter(|e| e.as_ref().id().to_string().starts_with(name_or_id) || e.as_ref().name() == name_or_id)
        .collect::<Vec<_>>();
    match matches.len() {
        0 => Err(anyhow!("{} '{}' not found", T::entity_type_static(), name_or_id)),
        1 => Ok(matches.pop().expect("length verified can't fail")),
        _ => Err(anyhow!(
            "'{}' identifies multiple {}s",
            T::entity_type_static(),
            name_or_id
        )),
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, EnumString, Debug)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum BcLogLevel {
    Info,
    Debug,
    Trace,
    TraceXdebug,
    TraceXtrace,
}

impl Default for BcLogLevel {
    fn default() -> Self {
        BcLogLevel::Info
    }
}

impl From<usize> for BcLogLevel {
    fn from(count: usize) -> Self {
        match count {
            0 => BcLogLevel::Info,
            1 => BcLogLevel::Debug,
            2 => BcLogLevel::Trace,
            3 => BcLogLevel::TraceXdebug,
            _ => BcLogLevel::TraceXtrace,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ServerConfig {
    pub log_level: BcLogLevel,
}
