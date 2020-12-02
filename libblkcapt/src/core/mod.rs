pub mod localsndrcv;
pub mod restic;
pub mod retention;
pub mod system;
use crate::model::Entity;
use crate::sys::btrfs::{Filesystem, MountedFilesystem, Subvolume};
use crate::sys::fs::{lookup_mountentry, BlockDeviceIds, BtrfsMountEntry, FsPathBuf};
use crate::{
    model::entities::{
        BtrfsContainerEntity, BtrfsDatasetEntity, BtrfsPoolEntity, HealthchecksObservation, ObservableEvent,
        SubvolumeEntity,
    },
    sys::net::HttpsClient,
};
use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, NaiveDateTime, Timelike, Utc};
use derivative::Derivative;
use hyper::Uri;
use std::path::PathBuf;
use std::{convert::TryFrom, str::FromStr, sync::Arc};
use std::{fmt::Debug, fmt::Display, fs};
use uuid::Uuid;

use self::localsndrcv::{SnapshotReceiver, SnapshotSender};
//use thiserror::Error;

const BLKCAPT_FS_META_DIR: &str = ".blkcapt";

#[derive(Debug)]
pub struct BtrfsPool {
    model: BtrfsPoolEntity,
    filesystem: MountedFilesystem,
}

impl BtrfsPool {
    pub fn new(name: String, mountpoint: PathBuf) -> Result<Self> {
        let mountentry = lookup_mountentry(&mountpoint).context("Mountpoint does not exist.")?;

        if !BtrfsMountEntry::try_from(mountentry)?.is_toplevel_subvolume() {
            bail!("Mountpoint must be the fstree (top-level) subvolume.");
        }

        let btrfs_info = Filesystem::query_path(&mountpoint)
            .expect("Valid btrfs mount should have filesystem info.")
            .unwrap_mounted()
            .context("Validated top-level mount point didn't yield a mounted filesystem.")?;

        let device_infos = btrfs_info
            .filesystem
            .devices
            .iter()
            .map(|d| BlockDeviceIds::lookup(d))
            .collect::<Result<Vec<BlockDeviceIds>>>()
            .context("All devices for a btrfs filesystem should resolve with blkid.")?;

        let device_uuid_subs = device_infos
            .iter()
            .map(|d| {
                d.uuid_sub
                    .context("All devices for a btrfs filesystem should have a uuid_subs.")
            })
            .collect::<Result<Vec<Uuid>>>()?;

        let meta_dir = FsPathBuf::from(BLKCAPT_FS_META_DIR);
        let mounted_meta_dir = meta_dir.as_pathbuf(&mountpoint);
        if !mounted_meta_dir.exists() {
            slog_scope::info!("Attached to new filesystem. Creating blkcapt dir.");
            fs::create_dir(&mounted_meta_dir)?;
            btrfs_info.create_subvolume(&meta_dir.join("snapshots"))?;
        }

        Ok(Self {
            model: BtrfsPoolEntity::new(name, mountpoint, btrfs_info.filesystem.uuid, device_uuid_subs)?,
            filesystem: btrfs_info,
        })
    }

    pub fn validate(model: BtrfsPoolEntity) -> Result<Self> {
        let btrfs_info = Filesystem::query_uuid(&model.uuid)
            .expect("Valid btrfs mount should have filesystem info.")
            .unwrap_mounted()
            .context("No active top-level mount point found for existing pool.")?;

        Ok(Self {
            model,
            filesystem: btrfs_info,
        })
    }

    pub fn model(&self) -> &BtrfsPoolEntity {
        &self.model
    }

    pub fn take_model(self) -> BtrfsPoolEntity {
        self.model
    }
}

impl Display for BtrfsPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.model.name())
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct BtrfsDataset {
    model: BtrfsDatasetEntity,
    subvolume: Subvolume,
    #[derivative(Debug = "ignore")]
    pool: Arc<BtrfsPool>,
}

impl BtrfsDataset {
    pub fn new(pool: &Arc<BtrfsPool>, name: String, path: PathBuf) -> Result<Self> {
        let subvolume = Subvolume::from_path(&path).context("Path does not resolve to a subvolume.")?;

        let dataset = Self {
            model: BtrfsDatasetEntity::new(name, subvolume.path.clone(), subvolume.uuid)?,
            subvolume,
            pool: Arc::clone(pool),
        };

        let snapshot_path = dataset.snapshot_container_path();
        if !snapshot_path
            .as_pathbuf(&dataset.pool.filesystem.fstree_mountpoint)
            .exists()
        {
            slog_scope::info!("Attached to new dataset. Creating local snap container.");
            dataset.pool.filesystem.create_subvolume(&snapshot_path)?;
        }

        Ok(dataset)
    }

    pub fn create_local_snapshot(self: &Arc<Self>) -> Result<BtrfsDatasetSnapshot> {
        let now = Utc::now();
        let snapshot_path = self
            .snapshot_container_path()
            .join(now.format("%FT%H-%M-%SZ").to_string());
        self.pool.filesystem.create_snapshot(&self.subvolume, &snapshot_path)?;

        self.pool
            .filesystem
            .subvolume_by_path(&snapshot_path)
            .map(|s| BtrfsDatasetSnapshot {
                subvolume: s,
                datetime: now.date().and_hms(now.hour(), now.minute(), now.second()),
                dataset: Arc::clone(self),
            })
    }

    pub fn snapshots(self: &Arc<Self>) -> Result<Vec<BtrfsDatasetSnapshot>> {
        let mut snapshots = self
            .pool
            .filesystem
            .list_subvolumes(&self.snapshot_container_path())?
            .into_iter()
            .filter_map(|s| {
                match NaiveDateTime::parse_from_str(
                    &s.path
                        .file_name()
                        .expect("Snapshot path should never end in ..")
                        .to_string_lossy(),
                    "%FT%H-%M-%SZ",
                ) {
                    Ok(d) => Some(BtrfsDatasetSnapshot {
                        subvolume: s,
                        datetime: DateTime::<Utc>::from_utc(d, Utc),
                        dataset: Arc::clone(self),
                    }),
                    Err(_) => None,
                }
            })
            .collect::<Vec<_>>();
        snapshots.sort_unstable_by_key(|s| s.datetime);
        Ok(snapshots)
    }

    pub fn latest_snapshot(self: &Arc<Self>) -> Result<Option<BtrfsDatasetSnapshot>> {
        let mut snapshots = self.snapshots()?;
        Ok(snapshots.pop())
    }

    pub fn snapshot_container_path(&self) -> FsPathBuf {
        let mut builder = FsPathBuf::from(BLKCAPT_FS_META_DIR);
        builder.push("snapshots");
        builder.push(self.model.id().to_string());
        builder
    }

    pub fn uuid(&self) -> Uuid {
        self.subvolume.uuid
    }

    pub fn parent_uuid(&self) -> Option<Uuid> {
        self.subvolume.parent_uuid
    }

    pub fn validate(pool: &Arc<BtrfsPool>, model: BtrfsDatasetEntity) -> Result<Self> {
        let subvolume = pool
            .filesystem
            .subvolume_by_uuid(model.uuid())
            .context("Can't locate subvolume for existing dataset.")?;

        Ok(Self {
            model,
            subvolume,
            pool: Arc::clone(pool),
        })
    }

    pub fn state(&self) -> BtrfsDatasetState {
        match self.parent_uuid() {
            Some(parent_snapshot) => BtrfsDatasetState::Restored { parent_snapshot },
            None => BtrfsDatasetState::Original,
        }
    }

    pub fn model(&self) -> &BtrfsDatasetEntity {
        &self.model
    }

    pub fn take_model(self) -> BtrfsDatasetEntity {
        self.model
    }
}

impl Display for BtrfsDataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}/{}", self.pool, self.model().name(),))
    }
}

impl AsRef<BtrfsDataset> for BtrfsDataset {
    fn as_ref(&self) -> &Self {
        self
    }
}

pub trait Snapshot: Display {
    fn datetime(&self) -> DateTime<Utc>;
}

pub trait BtrfsSnapshot: Snapshot {
    fn uuid(&self) -> Uuid;
    fn delete(self) -> Result<()>;
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct BtrfsDatasetSnapshot {
    subvolume: Subvolume,
    datetime: DateTime<Utc>,
    #[derivative(Debug = "ignore")]
    dataset: Arc<BtrfsDataset>,
}

impl BtrfsDatasetSnapshot {
    pub fn path(&self) -> &FsPathBuf {
        &self.subvolume.path
    }

    pub fn canonical_path(&self) -> PathBuf {
        self.path().as_pathbuf(&self.dataset.pool.filesystem.fstree_mountpoint)
    }

    pub fn parent_uuid(&self) -> Option<Uuid> {
        self.subvolume.parent_uuid
    }

    pub fn received_uuid(&self) -> Option<Uuid> {
        self.subvolume.received_uuid
    }

    pub fn send(&self, parent: Option<&BtrfsDatasetSnapshot>) -> SnapshotSender {
        SnapshotSender::new(
            self.dataset
                .pool
                .filesystem
                .send_subvolume(self.path(), parent.map(|s| s.path())),
        )
    }

    pub fn state(&self) -> BtrfsDatasetSnapshotState {
        match self.received_uuid() {
            Some(source_snapshot) => BtrfsDatasetSnapshotState::Restored {
                source_snapshot,
                parent_snapshot: self.parent_uuid(),
            },
            None => BtrfsDatasetSnapshotState::Original {
                parent_dataset: self
                    .parent_uuid()
                    .expect("INVARIANT: Local snapshots always have a parent."),
            },
        }
    }
}

impl BtrfsSnapshot for BtrfsDatasetSnapshot {
    fn uuid(&self) -> Uuid {
        self.subvolume.uuid
    }

    fn delete(self) -> Result<()> {
        self.dataset.pool.filesystem.delete_subvolume(self.path())
        // .map_err(|e| SnapshotDeleteError {
        //     source: e,
        //     snapshot: self,
        // })
    }
}

impl Snapshot for BtrfsDatasetSnapshot {
    fn datetime(&self) -> DateTime<Utc> {
        self.datetime
    }
}

impl Display for BtrfsDatasetSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{}/{}",
            self.dataset,
            self.datetime.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ))
    }
}

impl AsRef<BtrfsDatasetSnapshot> for BtrfsDatasetSnapshot {
    fn as_ref(&self) -> &Self {
        self
    }
}

pub enum BtrfsDatasetState {
    Restored { parent_snapshot: Uuid },
    Original,
}

#[derive(Debug, Clone)]
pub enum BtrfsDatasetSnapshotState {
    Restored {
        source_snapshot: Uuid,
        parent_snapshot: Option<Uuid>,
    },
    Original {
        parent_dataset: Uuid,
    },
}

// #[derive(Error, Debug)]
// #[error("{source}")]
// pub struct SnapshotDeleteError<T: BtrfsSnapshot> {
//     #[source]
//     pub source: anyhow::Error,
//     pub snapshot: T,
// }

#[derive(Debug, Clone)]
pub struct SnapshotHandle {
    pub datetime: DateTime<Utc>,
    pub uuid: Uuid,
}

impl<T> From<&T> for SnapshotHandle
where
    T: BtrfsSnapshot,
{
    fn from(snapshot: &T) -> Self {
        Self {
            datetime: snapshot.datetime(),
            uuid: snapshot.uuid(),
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct BtrfsContainer {
    model: BtrfsContainerEntity,
    subvolume: Subvolume,
    #[derivative(Debug = "ignore")]
    pool: Arc<BtrfsPool>,
}

impl BtrfsContainer {
    pub fn new(pool: &Arc<BtrfsPool>, name: String, path: PathBuf) -> Result<Self> {
        let subvolume = Subvolume::from_path(&path).context("Path does not resolve to a subvolume.")?;

        let dataset = Self {
            model: BtrfsContainerEntity::new(name, subvolume.path.clone(), subvolume.uuid)?,
            subvolume,
            pool: Arc::clone(pool),
        };

        Ok(dataset)
    }

    pub fn source_dataset_ids(&self) -> Result<Vec<Uuid>> {
        Ok(self
            .pool
            .filesystem
            .list_subvolumes(&self.subvolume.path)?
            .into_iter()
            .filter_map(|s| Uuid::parse_str(&s.path.file_name().unwrap_or_default().to_string_lossy()).ok())
            .collect::<Vec<_>>())
    }

    pub fn snapshots(self: &Arc<Self>, dataset_id: Uuid) -> Result<Vec<BtrfsContainerSnapshot>> {
        let mut snapshots = self
            .pool
            .filesystem
            .list_subvolumes(&self.snapshot_container_path(dataset_id))?
            .into_iter()
            .filter(|s| s.path.extension() == Some("bcrcv".as_ref()))
            .filter_map(|s| self.new_child_snapshot(s).ok())
            .collect::<Vec<_>>();
        snapshots.sort_unstable_by_key(|s| s.datetime);
        Ok(snapshots)
    }

    pub fn snapshot_by_datetime(
        self: &Arc<Self>,
        dataset_id: Uuid,
        datetime: DateTime<Utc>,
    ) -> Result<BtrfsContainerSnapshot> {
        let name = datetime.format("%FT%H-%M-%SZ.bcrcv").to_string();
        self.snapshot_by_name(dataset_id, &name)
    }

    pub fn snapshot_container_path(&self, dataset_id: Uuid) -> FsPathBuf {
        self.subvolume.path.join(dataset_id.to_string())
    }

    pub fn receive(self: &Arc<Self>, dataset_id: Uuid) -> SnapshotReceiver {
        SnapshotReceiver::new(
            self.pool
                .filesystem
                .receive_subvolume(&self.snapshot_container_path(dataset_id)),
            dataset_id,
            Arc::clone(self),
        )
    }

    pub fn validate(pool: &Arc<BtrfsPool>, model: BtrfsContainerEntity) -> Result<Self> {
        let subvolume = pool
            .filesystem
            .subvolume_by_uuid(model.uuid())
            .context("Can't locate subvolume for existing dataset.")?;

        Ok(Self {
            model,
            subvolume,
            pool: Arc::clone(pool),
        })
    }

    pub fn model(&self) -> &BtrfsContainerEntity {
        &self.model
    }

    pub fn take_model(self) -> BtrfsContainerEntity {
        self.model
    }

    fn snapshot_by_name(self: &Arc<Self>, dataset_id: Uuid, name: &str) -> Result<BtrfsContainerSnapshot> {
        self.pool
            .filesystem
            .subvolume_by_path(&self.snapshot_container_path(dataset_id).join(name))
            .and_then(|s| self.new_child_snapshot(s))
    }

    fn new_child_snapshot(self: &Arc<Self>, subvolume: Subvolume) -> Result<BtrfsContainerSnapshot> {
        parse_snapshot_label(
            &subvolume
                .path
                .file_stem()
                .expect("Snapshot path always has filename.")
                .to_string_lossy(),
        )
        .map(|datetime| BtrfsContainerSnapshot {
            subvolume,
            datetime,
            container: Arc::clone(self),
        })
    }
}

fn parse_snapshot_label(value: &str) -> Result<DateTime<Utc>> {
    NaiveDateTime::parse_from_str(value, "%FT%H-%M-%SZ")
        .map(|naive_datetime| DateTime::<Utc>::from_utc(naive_datetime, Utc))
        .context("unable to parse snapshot label")
}

impl Display for BtrfsContainer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}/{}", self.pool, self.model().name(),))
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct BtrfsContainerSnapshot {
    subvolume: Subvolume,
    datetime: DateTime<Utc>,
    #[derivative(Debug = "ignore")]
    container: Arc<BtrfsContainer>,
}

impl BtrfsContainerSnapshot {
    pub fn path(&self) -> &FsPathBuf {
        &self.subvolume.path
    }

    pub fn parent_uuid(&self) -> Option<Uuid> {
        self.subvolume.parent_uuid
    }

    pub fn received_uuid(&self) -> Uuid {
        self.subvolume
            .received_uuid
            .expect("container snapshots are always received")
    }
}

impl BtrfsSnapshot for BtrfsContainerSnapshot {
    fn uuid(&self) -> Uuid {
        self.subvolume.uuid
    }

    fn delete(self) -> Result<()> {
        self.container.pool.filesystem.delete_subvolume(self.path())
    }
}

impl Snapshot for BtrfsContainerSnapshot {
    fn datetime(&self) -> DateTime<Utc> {
        self.datetime
    }
}

impl Display for BtrfsContainerSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{}/{}",
            self.container,
            self.datetime.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ))
    }
}

impl AsRef<BtrfsContainerSnapshot> for BtrfsContainerSnapshot {
    fn as_ref(&self) -> &Self {
        self
    }
}

// ## Observer #######################################################################################################

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObservableEventStage {
    Starting,
    Succeeded,
    Failed(String),
}

pub struct ObservationRouter {
    observerations: Vec<HealthchecksObservation>,
}

impl ObservationRouter {
    pub fn new(model: Vec<HealthchecksObservation>) -> Self {
        Self { observerations: model }
    }

    pub fn route(&self, source: Uuid, event: ObservableEvent) -> Vec<&HealthchecksObservation> {
        self.observerations
            .iter()
            .filter(|obs| obs.observation.entity_id == source && obs.observation.event == event)
            .collect()
    }
}

pub struct ObservationEmitter {
    http_client: HttpsClient,
    url: String,
}

impl ObservationEmitter {
    pub const DEFAULT_URL: &'static str = "https://hc-ping.com/";

    pub fn new(custom_url: String) -> Self {
        Self {
            http_client: HttpsClient::default(),
            url: custom_url,
        }
    }

    pub async fn emit(&self, healthcheck_id: Uuid, stage: ObservableEventStage) -> Result<()> {
        let suffix = match stage {
            ObservableEventStage::Starting => "/start",
            ObservableEventStage::Succeeded => "",
            ObservableEventStage::Failed(_) => "/fail",
        };
        let uri_string = format!("{}{}", &self.url, healthcheck_id.to_hyphenated());
        let uri = Uri::from_str((uri_string + suffix).as_str()).unwrap();

        slog_scope::trace!("Emitting health check to url: {}", uri);
        self.http_client
            .get(uri)
            .await
            .map_err(|e| anyhow!(e))
            .and_then(|r| match r.status() {
                http::status::StatusCode::OK => Ok(()),
                e => Err(anyhow!(e)),
            })
    }
}

impl Default for ObservationEmitter {
    fn default() -> Self {
        Self {
            http_client: HttpsClient::default(),
            url: String::from(Self::DEFAULT_URL),
        }
    }
}
