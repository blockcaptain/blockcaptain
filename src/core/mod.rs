use crate::model::entities::{BtrfsContainerEntity, BtrfsDatasetEntity, BtrfsPoolEntity, SubvolumeEntity};
use crate::sys::btrfs::{Filesystem, MountedFilesystem, QueriedFilesystem, Subvolume};
use crate::sys::fs::{lookup_mountentry, BlockDeviceIds, BtrfsMountEntry};
use chrono::{DateTime, NaiveDateTime, Utc};
use anyhow::{anyhow, bail, Context, Error, Result};
use log::*;
use std::fs;
use std::path::{Path, PathBuf};
use std::{convert::TryFrom, mem, rc::Rc};
use uuid::Uuid;

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

        let btrfs_info = Filesystem::query_device(&mountpoint)
            .expect("Valid btrfs mount should have filesystem info.")
            .unwrap_mounted()
            .context("Validated top-level mount point didn't yield a mounted filesystem.")?;

        let device_infos = btrfs_info
            .filesystem
            .devices
            .iter()
            .map(|d| BlockDeviceIds::lookup(d.to_str().expect("Device path should convert to string.")))
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
            model: model,
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

#[derive(Debug)]
pub struct BtrfsDataset {
    model: BtrfsDatasetEntity,
    subvolume: Subvolume,
    pool: Rc<BtrfsPool>,
}

impl BtrfsDataset {
    pub fn new(pool: Rc<BtrfsPool>, name: String, path: PathBuf) -> Result<Self> {
        let subvolume = Subvolume::from_path(&path).context("Path does not resolve to a subvolume.")?;

        let dataset = Self {
            model: BtrfsDatasetEntity::new(name, subvolume.path.clone(), subvolume.uuid)?,
            subvolume: subvolume,
            pool: pool
        };

        let snapshot_path = dataset.snapshot_container_path();
        if !dataset.pool.filesystem.fstree_mountpoint.join(&snapshot_path).exists() {
            info!("Attached to new dataset. Creating local snap container.");
            dataset.pool.filesystem.create_subvolume(&snapshot_path)?;
        }

        Ok(dataset)
    }

    pub fn create_local_snapshot(&self) -> Result<()> {
        let now = Utc::now();
        let snapshot_path = self.snapshot_container_path().join(now.format("%FT%H-%M-%SZ").to_string());
        self.pool.filesystem.snapshot_subvolume(&self.subvolume, &snapshot_path)?;
        Ok(())
    }
    
    pub fn latest_snapshot(&self) -> Result<Option<DateTime<Utc>>> {
        let subvolume = self.pool.filesystem.subvolume_by_uuid(self.model.uuid()).context("Can't locate subvolume for existing dataset.")?;
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
        builder.push(self.model.uuid().to_string());
        builder
    }

    pub fn validate(pool: Rc<BtrfsPool>, model: BtrfsDatasetEntity) -> Result<Self> {
        let subvolume = pool.filesystem.subvolume_by_uuid(model.uuid()).context("Can't locate subvolume for existing dataset.")?;
        Ok(Self {
            model: model,
            subvolume: subvolume,
            pool: pool,
        })
    }

    pub fn model(&self) -> &BtrfsDatasetEntity {
        &self.model
    }

    pub fn take_model(self) -> BtrfsDatasetEntity {
        self.model
    }
}

#[derive(Debug)]
pub struct BtrfsContainer {
    model: BtrfsContainerEntity,
    subvolume: Subvolume,
}

impl BtrfsContainer {
    pub fn new(name: String, path: PathBuf) -> Result<Self> {
        let subvolume = Subvolume::from_path(&path).context("Path does not resolve to a subvolume.")?;

        let dataset = Self {
            model: BtrfsContainerEntity::new(name, subvolume.path.clone(), subvolume.uuid)?,
            subvolume: subvolume,
        };

        Ok(dataset)
    }

    pub fn model(&self) -> &BtrfsContainerEntity {
        &self.model
    }

    pub fn take_model(self) -> BtrfsContainerEntity {
        self.model
    }
}