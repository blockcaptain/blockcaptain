use super::{Entity, EntityStatic, EntityType};
use crate::sys::fs::FsPathBuf;
use anyhow::{anyhow, bail, Context as AnyhowContext, Result};
use cron::Schedule;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, convert::TryFrom, convert::TryInto, path::PathBuf, str::FromStr};
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

impl EntityStatic for BtrfsPoolEntity {
    fn entity_type_static() -> EntityType {
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

#[derive(Display, Copy, Clone, Eq, PartialEq)]
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
    pub snapshot_schedule: Option<ScheduleModel>,
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

impl EntityStatic for BtrfsDatasetEntity {
    fn entity_type_static() -> EntityType {
        EntityType::Dataset
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ScheduleModel(String);

impl TryFrom<&ScheduleModel> for Schedule {
    type Error = anyhow::Error;

    fn try_from(value: &ScheduleModel) -> Result<Self, Self::Error> {
        Schedule::from_str(&value.0).map_err(|e| anyhow!(e.to_string()))
    }
}

impl TryFrom<ScheduleModel> for Schedule {
    type Error = anyhow::Error;

    fn try_from(value: ScheduleModel) -> Result<Self, Self::Error> {
        Self::try_from(&value)
    }
}

impl FromStr for ScheduleModel {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Schedule::from_str(s)
            .map(|_| ScheduleModel(s.to_owned()))
            .map_err(|e| match e.kind() {
                cron::error::ErrorKind::Msg(s) | cron::error::ErrorKind::Expression(s) => anyhow!(s.to_owned()),
            })
    }
}

impl TryFrom<&Duration> for ScheduleModel {
    type Error = anyhow::Error;

    fn try_from(value: &Duration) -> Result<Self, Self::Error> {
        let duration = chrono::Duration::from_std(*value)?;
        if duration.num_seconds() == 0 {
            bail!("minimum frequency for schedule is 1 second");
        }
        if duration.num_minutes() == 0 {
            if 60 % duration.num_seconds() != 0 {
                bail!("frequency in seconds must divide evenly in to a minute");
            }
            return Ok(ScheduleModel(format!("0/{} * * * * * *", duration.num_seconds())));
        }
        if duration.num_hours() == 0 {
            if 60 % duration.num_minutes() != 0 {
                bail!("frequency in minutes must divide evenly in to an hour");
            }
            if duration.num_seconds() % 60 != 0 {
                bail!("frequency in minutes must be in whole minutes");
            }
            return Ok(ScheduleModel(format!("0 0/{} * * * * *", duration.num_minutes())));
        }
        if duration.num_days() == 0 {
            if 24 % duration.num_hours() != 0 {
                bail!("frequency in hours must divide evenly in to a day");
            }
            if duration.num_minutes() % 60 != 0 || duration.num_seconds() % 60 != 0 {
                bail!("frequency in hours must be in whole hours");
            }
            return Ok(ScheduleModel(format!("0 0 0/{} * * * *", duration.num_hours())));
        }
        if duration == chrono::Duration::days(1) {
            return Ok(ScheduleModel(String::from("0 0 0 * * * *")));
        }

        bail!("frequency greater than 1 day can not be converted to a schedule");
    }
}

impl TryFrom<Duration> for ScheduleModel {
    type Error = anyhow::Error;

    fn try_from(value: Duration) -> Result<Self, Self::Error> {
        Self::try_from(&value)
    }
}

impl BtrfsDatasetEntity {
    pub fn new(name: String, subvolume_path: FsPathBuf, subvolume_uuid: Uuid) -> Result<Self> {
        Ok(Self {
            id: Uuid::new_v4(),
            name,
            path: subvolume_path,
            uuid: subvolume_uuid,
            snapshot_schedule: None,
            snapshot_retention: None,
            pause_pruning: false,
            pause_snapshotting: false,
        })
    }

    pub fn snapshotting_state(&self) -> FeatureState {
        if self.snapshot_schedule.is_some() {
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
    pub path: FsPathBuf,
    pub uuid: Uuid,
    pub snapshot_retention: Option<RetentionRuleset>,
    pub pause_pruning: bool,
}

impl BtrfsContainerEntity {
    pub fn new(name: String, subvolume_path: FsPathBuf, subvolume_uuid: Uuid) -> Result<Self> {
        Ok(Self {
            id: Uuid::new_v4(),
            name,
            path: subvolume_path,
            uuid: subvolume_uuid,
            snapshot_retention: None,
            pause_pruning: false,
        })
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

impl EntityStatic for BtrfsContainerEntity {
    fn entity_type_static() -> EntityType {
        EntityType::Container
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SnapshotSyncEntity {
    id: Uuid,
    name: String,
    dataset: Uuid,
    container: Uuid,
    pub sync_mode: SnapshotSyncMode,
}

impl<'a> AsRef<dyn Entity + 'a> for SnapshotSyncEntity {
    fn as_ref(&self) -> &(dyn Entity + 'a) {
        self
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotSyncMode {
    AllScheduled(ScheduleModel),
    LatestScheduled(ScheduleModel),
    AllImmediate,
    IntervalImmediate(#[serde(with = "humantime_serde")] Duration),
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

impl EntityStatic for SnapshotSyncEntity {
    fn entity_type_static() -> EntityType {
        EntityType::SnapshotSync
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RetentionRuleset {
    pub interval: Vec<IntervalSpec>,
    pub newest_count: NonZeroU32,
    pub evaluation_schedule: ScheduleModel,
}

impl Default for RetentionRuleset {
    fn default() -> Self {
        Self {
            interval: Default::default(),
            newest_count: NonZeroU32::new(1).unwrap(),
            evaluation_schedule: ScheduleModel::try_from(Duration::from_secs(3600 * 24)).unwrap(),
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
    pub heartbeat: Option<HealthchecksHeartbeat>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HealthchecksHeartbeat {
    #[serde(with = "humantime_serde")]
    pub frequency: Duration,
    pub healthcheck_id: Uuid,
}

impl HealthchecksHeartbeat {
    pub fn new(healthcheck_id: Uuid) -> Self {
        Self {
            frequency: Duration::from_secs(5 * 60),
            healthcheck_id,
        }
    }

    pub fn set_frequency(&mut self, duration: Duration) -> Result<()> {
        let _: ScheduleModel = duration
            .try_into()
            .context("heartbeat frequency must be trivially convertible to a cron schedule")?;

        self.frequency = duration;
        Ok(())
    }
}

impl HealthchecksObserverEntity {
    pub fn new(name: String, observations: Vec<HealthchecksObservation>) -> Self {
        Self {
            id: Uuid::new_v4(),
            name,
            custom_url: None,
            observations,
            heartbeat: None,
        }
    }

    pub fn heartbeat_state(&self) -> FeatureState {
        if self.heartbeat.is_some() {
            FeatureState::Enabled
        } else {
            FeatureState::Unconfigured
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
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

impl EntityStatic for HealthchecksObserverEntity {
    fn entity_type_static() -> EntityType {
        EntityType::Observer
    }
}

impl<'a> AsRef<dyn Entity + 'a> for HealthchecksObserverEntity {
    fn as_ref(&self) -> &(dyn Entity + 'a) {
        self
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Observation {
    pub entity_id: Uuid,
    pub event: ObservableEvent,
}

#[derive(Serialize, Deserialize, Clone, Copy, Display, Debug, EnumString, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ObservableEvent {
    DatasetSnapshot,
    DatasetPrune,
    ContainerPrune,
    SnapshotSync,
    PoolScrub,
}

impl ObservableEvent {
    pub fn entity_type(&self) -> EntityType {
        match self {
            ObservableEvent::DatasetSnapshot => EntityType::Dataset,
            ObservableEvent::DatasetPrune => EntityType::Dataset,
            ObservableEvent::ContainerPrune => EntityType::Container,
            ObservableEvent::SnapshotSync => EntityType::SnapshotSync,
            ObservableEvent::PoolScrub => EntityType::Pool,
        }
    }
}

// ## Restic #######################################################################################################

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ResticContainerEntity {
    id: Uuid,
    name: String,
    pub repository: ResticRepository,
    pub custom_environment: HashMap<String, String>,
    pub snapshot_retention: Option<RetentionRuleset>,
    pub pause_pruning: bool,
}

impl ResticContainerEntity {
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

impl Entity for ResticContainerEntity {
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

impl<'a> AsRef<dyn Entity + 'a> for ResticContainerEntity {
    fn as_ref(&self) -> &(dyn Entity + 'a) {
        self
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub enum ResticRepository {
    Custom(String),
}
