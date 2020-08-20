use super::{Entity, EntityType};
use crate::btrfs::{Filesystem, Subvolume};
use crate::filesystem::{self, BlockDeviceIds, BtrfsMountEntry};
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::path::{Path, PathBuf};
use uuid::Uuid;

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
            return Err(anyhow!("Mountpoint must be the fstree (top-level) subvolume."));
        }

        let btrfs_info = Filesystem::query_device(&mountpoint).expect("Valid btrfs mount should have filesystem info.");

        let device_infos = btrfs_info
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

        Ok(BtrfsPool {
            id: Uuid::new_v4(),
            name: name,
            mountpoint_path: mountpoint,
            uuid: btrfs_info.uuid,
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
        self.subvolumes().filter(|d| d.uuid() == uuid).next()
    }

    fn subvolume_by_path(&self, path: &Path) -> Option<&dyn SubvolumeEntity> {
        self.subvolumes().filter(|d| d.path() == path).next()
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