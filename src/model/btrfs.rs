use serde::{Deserialize, Serialize};
use std::path::{PathBuf};
use uuid::Uuid;
use anyhow::{anyhow, Context, Result};
use crate::btrfs::{Filesystem};
use crate::filesystem::{self, BlockDeviceIds, BtrfsMountEntry};
use std::convert::TryFrom;

#[derive(Serialize, Deserialize, Debug)]
pub struct BtrfsPool {
    pub name: String,
    pub mountpoint_path: PathBuf,
    pub uuid: Uuid,
    pub uuid_subs: Vec<Uuid>,

    pub datasets: Vec<BtrfsDataset>,
}

impl BtrfsPool {
    pub fn new(name: String, mountpoint: PathBuf) -> Result<Self> {
        let mountentry = filesystem::lookup_mountentry(&mountpoint)
            .expect("All mount points are parsable.")
            .context("Mountpoint does not exist.")?;

        if BtrfsMountEntry::try_from(mountentry)?.is_toplevel_subvolume() {
            return Err(anyhow!("Mountpoint must be the fstree (top-level) subvolume."));
        }
       
        let btrfs_info =
            Filesystem::query_device(&mountpoint).expect("Valid btrfs mount should have filesystem info.");

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
            datasets: Vec::<BtrfsDataset>::default()
        })
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BtrfsDataset {
    pub name: PathBuf,
    pub uuid: Uuid,
}
