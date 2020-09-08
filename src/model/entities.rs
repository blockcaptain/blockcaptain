use super::{Entity, EntityType};
use crate::sys::fs::FsPathBuf;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::{default::Default, time::Duration, num::NonZeroU32};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BtrfsPoolEntity {
    pub id: Uuid,
    pub name: String,
    pub mountpoint_path: PathBuf,
    pub uuid: Uuid,
    pub uuid_subs: Vec<Uuid>,

    pub datasets: Vec<BtrfsDatasetEntity>,
    pub containers: Vec<BtrfsContainerEntity>,
}

impl BtrfsPoolEntity {
    pub fn new(name: String, mountpoint: PathBuf, uuid: Uuid, uuid_subs: Vec<Uuid>) -> Result<Self> {
        Ok(Self {
            id: Uuid::new_v4(),
            name: name,
            mountpoint_path: mountpoint,
            uuid: uuid,
            uuid_subs: uuid_subs,
            datasets: Vec::<BtrfsDatasetEntity>::default(),
            containers: Vec::<BtrfsContainerEntity>::default(),
        })
    }

    pub fn attach_dataset(&mut self, dataset: BtrfsDatasetEntity) -> Result<()> {
        self.subvolume_by_uuid(dataset.uuid()).map_or(Ok(()), |d| {
            Err(anyhow!("uuid already used by {} {}.", d.entity_type(), d.name()))
        })?;
        self.subvolume_by_path(dataset.path()).map_or(Ok(()), |d| {
            Err(anyhow!("path already used by {} {}.", d.entity_type(), d.name()))
        })?;

        self.datasets.push(dataset);
        Ok(())
    }

    pub fn attach_container(&mut self, container: BtrfsContainerEntity) -> Result<()> {
        self.subvolume_by_uuid(container.uuid()).map_or(Ok(()), |d| {
            Err(anyhow!("uuid already used by {} {}.", d.entity_type(), d.name()))
        })?;
        self.subvolume_by_path(container.path()).map_or(Ok(()), |d| {
            Err(anyhow!("path already used by {} {}.", d.entity_type(), d.name()))
        })?;

        self.containers.push(container);
        Ok(())
    }

    fn subvolume_by_uuid(&self, uuid: &Uuid) -> Option<&dyn SubvolumeEntity> {
        self.subvolumes().find(|d| d.uuid() == uuid)
    }

    fn subvolume_by_path(&self, path: &FsPathBuf) -> Option<&dyn SubvolumeEntity> {
        self.subvolumes().find(|d| d.path() == path)
    }

    fn subvolumes(&self) -> impl Iterator<Item = &dyn SubvolumeEntity> {
        let ds = self.datasets.iter().map(|x| x as &dyn SubvolumeEntity);
        let cs = self.containers.iter().map(|x| x as &dyn SubvolumeEntity);
        ds.chain(cs)
    }
}

impl Entity for BtrfsPoolEntity {
    fn name(&self) -> &str {
        &self.name
    }
    fn id(&self) -> Uuid {
        self.id
    }
    fn entity_type(&self) -> EntityType {
        EntityType::Pool
    }
}

// Why can't i make this generic over T?? I don't understand relationship
// of sized and generic trait bounds on references.
impl<'a> AsRef<dyn Entity + 'a> for BtrfsPoolEntity {
    fn as_ref(&self) -> &(dyn Entity + 'a) {
        self
    }
}

pub trait SubvolumeEntity: Entity {
    fn path(&self) -> &FsPathBuf;
    fn uuid(&self) -> &Uuid;
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BtrfsDatasetEntity {
    id: Uuid,
    pub name: String,
    pub path: FsPathBuf,
    pub uuid: Uuid,
    pub snapshot_frequency: Option<Duration>,
    pub pause_snapshotting: bool,
    pub snapshot_retention: Option<RetentionRuleset>,
    pub pause_pruning: bool,
}

impl SubvolumeEntity for BtrfsDatasetEntity {
    fn path(&self) -> &FsPathBuf {
        &self.path
    }
    fn uuid(&self) -> &Uuid {
        &self.uuid
    }
}

impl Entity for BtrfsDatasetEntity {
    fn name(&self) -> &str {
        &self.name
    }
    fn id(&self) -> Uuid {
        self.id
    }
    fn entity_type(&self) -> EntityType {
        EntityType::Dataset
    }
}

impl BtrfsDatasetEntity {
    pub fn new(name: String, subvolume_path: FsPathBuf, subvolume_uuid: Uuid) -> Result<Self> {
        Ok(Self {
            id: Uuid::new_v4(),
            name: name,
            path: subvolume_path,
            uuid: subvolume_uuid,
            snapshot_frequency: None,
            snapshot_retention: None,
            pause_pruning: false,
            pause_snapshotting: false,
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BtrfsContainerEntity {
    id: Uuid,
    name: String,
    path: FsPathBuf,
    uuid: Uuid,
}

impl BtrfsContainerEntity {
    pub fn new(name: String, subvolume_path: FsPathBuf, subvolume_uuid: Uuid) -> Result<Self> {
        Ok(Self {
            id: Uuid::new_v4(),
            name: name,
            path: subvolume_path,
            uuid: subvolume_uuid,
        })
    }
}

impl SubvolumeEntity for BtrfsContainerEntity {
    fn path(&self) -> &FsPathBuf {
        &self.path
    }
    fn uuid(&self) -> &Uuid {
        &self.uuid
    }
}

impl Entity for BtrfsContainerEntity {
    fn name(&self) -> &str {
        &self.name
    }
    fn id(&self) -> Uuid {
        self.id
    }
    fn entity_type(&self) -> EntityType {
        EntityType::Container
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SnapshotSyncEntity {
    id: Uuid,
    name: String,
    dataset: Uuid,
    container: Uuid,
}

impl SnapshotSyncEntity {
    pub fn dataset_id(&self) -> Uuid {
        self.dataset
    }
    pub fn container_id(&self) -> Uuid {
        self.container
    }
}

impl Entity for SnapshotSyncEntity {
    fn name(&self) -> &str {
        &self.name
    }
    fn id(&self) -> Uuid {
        self.id
    }
    fn entity_type(&self) -> EntityType {
        EntityType::SnapshotSync
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RetentionRuleset {
    pub interval: Vec<IntervalSpec>,
    pub newest_count: NonZeroU32,
}

impl Default for RetentionRuleset {
    fn default() -> Self {
        Self {
            interval: Default::default(),
            newest_count: NonZeroU32::new(1).unwrap(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IntervalSpec {
    pub repeat: u32,
    pub duration: Duration,
    pub keep: KeepSpec,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum KeepSpec {
    Newest(u32),
    All,
}
