use crate::btrfs::{Filesystem, Subvolume};
use crate::filesystem::{self, BlockDeviceIds, BtrfsMountEntry};
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::path::{PathBuf, Path};
use uuid::Uuid;
use super::Entity;

#[derive(Serialize, Deserialize, Debug)]
pub struct BtrfsPool {
    pub name: String,
    pub mountpoint_path: PathBuf,
    pub uuid: Uuid,
    pub uuid_subs: Vec<Uuid>,

    pub datasets: Vec<BtrfsDataset>,
    pub containers: Vec<BtrfsContainer>,
}

impl BtrfsPool {
    pub fn new(name: String, mountpoint: PathBuf) -> Result<Self> {
        let mountentry = filesystem::lookup_mountentry(&mountpoint)
            .context("Mountpoint does not exist.")?;

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
            name: name,
            mountpoint_path: mountpoint,
            uuid: btrfs_info.uuid,
            uuid_subs: device_uuid_subs,
            datasets: Vec::<BtrfsDataset>::default(),
            containers: Vec::<BtrfsContainer>::default(),
        })
    }

    pub fn attach_dataset(&mut self, dataset: BtrfsDataset) -> Result<()> {
        self.dataset_by_uuid(dataset.uuid())
            .map_or(Ok(()), |d| Err(anyhow!("uuid already used by dataset {}.", d.name())))?;
        self.dataset_by_path(dataset.path())
            .map_or(Ok(()), |d| Err(anyhow!("path already used by dataset {}.", d.name())))?;

        self.datasets.push(dataset);
        Ok(())
    }

    fn dataset_by_uuid(&self, uuid: &Uuid) -> Option<&BtrfsDataset> {
        self.datasets.iter().filter(|d| d.uuid() == uuid).next()
    }

    fn dataset_by_path(&self, path: &Path) -> Option<&BtrfsDataset> {
        self.datasets.iter().filter(|d| d.path() == path).next()
    }

    fn subvolumes(&self) -> impl Iterator<Item = &dyn SubvolumeEntity> {
        let ds = self.datasets.iter().map(|x| x as &dyn SubvolumeEntity);
        let cs = self.containers.iter().map(|x| x as &dyn SubvolumeEntity);
        ds.chain(cs)
    }
}

trait SubvolumeEntity: Entity {
    fn path(&self) -> &Path;
    fn uuid(&self) -> &Uuid;
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BtrfsDataset {
    _name: String,
    _path: PathBuf,
    _uuid: Uuid,
}

impl SubvolumeEntity for BtrfsDataset {
    fn path(&self) -> &Path { &self._path }
    fn uuid(&self) -> &Uuid { &self._uuid }
}

impl Entity for BtrfsDataset {
    fn name(&self) -> &str { &self._name }
}

impl BtrfsDataset {
    pub fn new(name: String, path: PathBuf) -> Result<Self> {
        let subvol = Subvolume::from_path(&path).context("Path does not resolve to a subvolume.")?;

        Ok(Self {
            _name: name,
            _path: subvol.path,
            _uuid: subvol.uuid,
        })
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BtrfsContainer {
    _name: String,
    _path: PathBuf,
    _uuid: Uuid,
}

impl BtrfsContainer {
    pub fn new(name: String, path: PathBuf) -> Result<Self> {
        let subvol = Subvolume::from_path(&path).context("Path does not resolve to a subvolume.")?;

        Ok(Self {
            _name: name,
            _path: subvol.path,
            _uuid: subvol.uuid,
        })
    }
}

impl SubvolumeEntity for BtrfsContainer {
    fn path(&self) -> &Path { &self._path }
    fn uuid(&self) -> &Uuid { &self._uuid }
}

impl Entity for BtrfsContainer {
    fn name(&self) -> &str { &self._name }
}