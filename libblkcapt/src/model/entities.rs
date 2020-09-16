use super::{Entity, EntityType};
use crate::sys::fs::FsPathBuf;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::{default::Default, num::NonZeroU32, time::Duration};
use strum_macros::Display;
use strum_macros::EnumString;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BtrfsPoolEntity {
    id: Uuid,
    name: String,
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
            name,
            mountpoint_path: mountpoint,
            uuid,
            uuid_subs,
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

#[derive(Display, Copy, Clone)]
pub enum FeatureState {
    Unconfigured,
    Paused,
    Enabled,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BtrfsDatasetEntity {
    id: Uuid,
    name: String,
    pub path: FsPathBuf,
    pub uuid: Uuid,
    #[serde(with = "humantime_serde")]
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
            name,
            path: subvolume_path,
            uuid: subvolume_uuid,
            snapshot_frequency: None,
            snapshot_retention: None,
            pause_pruning: false,
            pause_snapshotting: false,
        })
    }

    pub fn snapshotting_state(&self) -> FeatureState {
        if self.snapshot_frequency.is_some() {
            if self.pause_snapshotting {
                FeatureState::Paused
            } else {
                FeatureState::Enabled
            }
        } else {
            FeatureState::Unconfigured
        }
    }

    pub fn pruning_state(&self) -> FeatureState {
        if self.snapshot_retention.is_some() {
            if self.pause_pruning {
                FeatureState::Paused
            } else {
                FeatureState::Enabled
            }
        } else {
            FeatureState::Unconfigured
        }
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
            name,
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
    pub repeat: NonZeroU32,
    #[serde(with = "humantime_serde")]
    pub duration: Duration,
    pub keep: KeepSpec,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
#[serde(rename_all = "snake_case")]
pub enum KeepSpec {
    Newest(NonZeroU32),
    All,
}

// ## Observer #######################################################################################################

#[derive(Serialize, Deserialize, Debug)]
pub struct HealthchecksObserverEntity {
    id: Uuid,
    name: String,
    pub custom_url: Option<String>,
    pub observations: Vec<HealthchecksObservation>,
}

impl HealthchecksObserverEntity {
    pub fn new(name: String, observations: Vec<HealthchecksObservation>) -> Self {
        Self {
            id: Uuid::new_v4(),
            name,
            custom_url: None,
            observations,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HealthchecksObservation {
    #[serde(flatten)]
    pub observation: Observation,
    pub healthcheck_id: Uuid,
}

impl Entity for HealthchecksObserverEntity {
    fn name(&self) -> &str {
        &self.name
    }
    fn id(&self) -> Uuid {
        self.id
    }
    fn entity_type(&self) -> EntityType {
        EntityType::Observer
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Observation {
    pub entity_id: Uuid,
    pub event: ObservableEvent,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, EnumString, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ObservableEvent {
    WorkerHeartbeat,
    DatasetSnapshot,
    DatasetPrune,
    ContainerPrune,
    SnapshotSync,
    PoolScrub,
}
