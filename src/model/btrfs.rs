use super::{Entity, EntityType};
use crate::btrfs::{Filesystem, QueriedFilesystem, Subvolume};
use crate::filesystem::{self, BlockDeviceIds, BtrfsMountEntry};
use anyhow::{anyhow, bail, Context, Error, Result};
use chrono::{DateTime, FixedOffset, NaiveDateTime, Utc};
use log::*;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

const BLKCAPT_FS_META_DIR: &str = ".blkcapt";

#[derive(Serialize, Deserialize, Debug)]
pub struct BtrfsPool {
    id: Uuid,
    pub name: String,
    pub mountpoint_path: PathBuf,
    pub uuid: Uuid,
    pub uuid_subs: Vec<Uuid>,

    pub datasets: Vec<BtrfsDataset>,
    pub containers: Vec<BtrfsContainer>,
}

impl BtrfsPool {
    pub fn new(name: String, mountpoint: PathBuf) -> Result<Self> {
        let mountentry = filesystem::lookup_mountentry(&mountpoint).context("Mountpoint does not exist.")?;

        if !BtrfsMountEntry::try_from(mountentry)?.is_toplevel_subvolume() {
            bail!("Mountpoint must be the fstree (top-level) subvolume.");
        }

        let btrfs_info = Filesystem::query_device(&mountpoint)
            .expect("Valid btrfs mount should have filesystem info.")
            .unwrap_mounted()
            .context("Validated top-level mount point didn't yield a mounted filesystem.")?;

        let device_infos = btrfs_info
            .filesystem
            .devices
            .iter()
            .map(|d| filesystem::BlockDeviceIds::lookup(d.to_str().expect("Device path should convert to string.")))
            .collect::<Result<Vec<BlockDeviceIds>>>()
            .context("All devices for a btrfs filesystem should resolve with blkid.")?;

        let device_uuid_subs = device_infos
            .iter()
            .map(|d| {
                d.uuid_sub
                    .context("All devices for a btrfs filesystem should have a uuid_subs.")
            })
            .collect::<Result<Vec<Uuid>>>()?;

        let meta_dir = mountpoint.join(BLKCAPT_FS_META_DIR);
        if !meta_dir.exists() {
            info!("Attached to new filesystem. Creating blkcapt dir.");
            fs::create_dir(&meta_dir)?;
            btrfs_info.create_subvolume(&meta_dir.join("snapshots"))?;
        }

        Ok(BtrfsPool {
            id: Uuid::new_v4(),
            name: name,
            mountpoint_path: mountpoint,
            uuid: btrfs_info.filesystem.uuid,
            uuid_subs: device_uuid_subs,
            datasets: Vec::<BtrfsDataset>::default(),
            containers: Vec::<BtrfsContainer>::default(),
        })
    }

    pub fn attach_dataset(&mut self, dataset: BtrfsDataset) -> Result<()> {
        self.subvolume_by_uuid(dataset.uuid()).map_or(Ok(()), |d| {
            Err(anyhow!("uuid already used by {} {}.", d.entity_type(), d.name()))
        })?;
        self.subvolume_by_path(dataset.path()).map_or(Ok(()), |d| {
            Err(anyhow!("path already used by {} {}.", d.entity_type(), d.name()))
        })?;

        self.datasets.push(dataset);
        Ok(())
    }

    pub fn attach_container(&mut self, container: BtrfsContainer) -> Result<()> {
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

    fn subvolume_by_path(&self, path: &Path) -> Option<&dyn SubvolumeEntity> {
        self.subvolumes().find(|d| d.path() == path)
    }

    fn subvolumes(&self) -> impl Iterator<Item = &dyn SubvolumeEntity> {
        let ds = self.datasets.iter().map(|x| x as &dyn SubvolumeEntity);
        let cs = self.containers.iter().map(|x| x as &dyn SubvolumeEntity);
        ds.chain(cs)
    }
}

impl Entity for BtrfsPool {
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

pub trait SubvolumeEntity: Entity {
    fn path(&self) -> &Path;
    fn uuid(&self) -> &Uuid;
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BtrfsDataset {
    id: Uuid,
    name: String,
    path: PathBuf,
    uuid: Uuid,
}

impl SubvolumeEntity for BtrfsDataset {
    fn path(&self) -> &Path {
        &self.path
    }
    fn uuid(&self) -> &Uuid {
        &self.uuid
    }
}

impl Entity for BtrfsDataset {
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

impl BtrfsDataset {
    pub fn new(name: String, path: PathBuf, mount_path: &PathBuf) -> Result<Self> {
        let subvolume = Subvolume::from_path(&path).context("Path does not resolve to a subvolume.")?;
        let filesystem = Filesystem::query_path(mount_path)?.unwrap_mounted()?;

        let dataset = Self {
            id: Uuid::new_v4(),
            name: name,
            path: subvolume.path,
            uuid: subvolume.uuid,
        };

        let snapshot_path = dataset.snapshot_container_path();
        if !filesystem.fstree_mountpoint.join(&snapshot_path).exists() {
            info!("Attached to new dataset. Creating local snap container.");
            filesystem.create_subvolume(&snapshot_path)?;
        }

        Ok(dataset)
    }

    pub fn latest_snapshot(&self, subvolume: &Subvolume) -> Result<Option<DateTime<Utc>>> {
        let mut paths = subvolume.snapshot_paths.iter().collect::<Vec<_>>();
        paths.sort();
        match paths.last() {
            Some(v) => match NaiveDateTime::parse_from_str(
                &v.file_name()
                    .expect("Snapshot path should never end in ..")
                    .to_string_lossy(),
                "%FT%H-%M-%SZ",
            ) {
                Ok(d) => Ok(Some(DateTime::<Utc>::from_utc(d, Utc))),
                Err(e) => Err(Error::new(e)),
            },
            None => Ok(None),
        }
    }

    pub fn snapshot_container_path(&self) -> PathBuf {
        let mut builder = PathBuf::from(BLKCAPT_FS_META_DIR);
        builder.push("snapshots");
        builder.push(self.uuid().to_string());
        builder
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BtrfsContainer {
    id: Uuid,
    name: String,
    path: PathBuf,
    uuid: Uuid,
}

impl BtrfsContainer {
    pub fn new(name: String, path: PathBuf) -> Result<Self> {
        let subvol = Subvolume::from_path(&path).context("Path does not resolve to a subvolume.")?;

        Ok(Self {
            id: Uuid::new_v4(),
            name: name,
            path: subvol.path,
            uuid: subvol.uuid,
        })
    }
}

impl SubvolumeEntity for BtrfsContainer {
    fn path(&self) -> &Path {
        &self.path
    }
    fn uuid(&self) -> &Uuid {
        &self.uuid
    }
}

impl Entity for BtrfsContainer {
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

pub fn full_path(pool: &BtrfsPool, dataset: &impl SubvolumeEntity) -> PathBuf {
    pool.mountpoint_path.join(dataset.path())
}
