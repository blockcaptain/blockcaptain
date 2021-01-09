use crate::parsing::{parse_key_value_data, StringPair};
#[mockall_double::double]
use crate::sys::process::double as process_double;
use crate::sys::process::output_as_result;
use anyhow::{anyhow, Context, Error, Result};
use mnt::{MountEntry, MountIter};
use nix::mount::{mount, MsFlags};
use process_double::{run_command, run_command_as_result};
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;
use std::{collections::HashMap, process::Command};
use std::{convert::TryFrom, fmt::Display};
use uuid::Uuid;

// ## Filesystem Relative PathBuf ####################################################################################

// File-system relative path. PathBufs are considered root relative.
#[derive(Serialize, Deserialize, Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct FsPathBuf(PathBuf);

impl FsPathBuf {
    pub fn as_pathbuf(&self, mount_path: &Path) -> PathBuf {
        mount_path.join(&self.0)
    }

    pub fn file_name(&self) -> Option<&OsStr> {
        self.0.file_name()
    }

    pub fn file_stem(&self) -> Option<&OsStr> {
        self.0.file_stem()
    }

    pub fn extension(&self) -> Option<&OsStr> {
        self.0.extension()
    }

    pub fn join<P: AsRef<Path>>(&self, path: P) -> Self {
        Self(self.0.join(path))
    }

    pub fn push<P: AsRef<Path>>(&mut self, path: P) {
        self.0.push(path);
    }
}

impl<T: ?Sized + AsRef<OsStr>> From<&T> for FsPathBuf {
    fn from(s: &T) -> Self {
        Self(PathBuf::from(s))
    }
}

impl FromStr for FsPathBuf {
    type Err = <PathBuf as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(FsPathBuf::from(s))
    }
}

// ## Device PathBuf #################################################################################################

#[derive(Serialize, Deserialize, Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct DevicePathBuf(PathBuf);

impl DevicePathBuf {
    pub fn as_pathbuf(&self) -> PathBuf {
        self.0.clone()
    }

    // Not using trait due to https://github.com/rust-lang/rust/issues/50133
    // Conflict with the blanket impl of into for tryfrom
    pub fn try_from<T: ?Sized + AsRef<Path>>(s: &T) -> Result<Self> {
        let mut iter = s.as_ref().components();
        if let Some(c) = iter.next() {
            if let Component::RootDir = c {
                if let Some(c) = iter.next() {
                    if let Component::Normal(d) = c {
                        if d == "dev" && iter.next().is_some() {
                            return Ok(Self(s.as_ref().to_owned()));
                        }
                    }
                }
            }
        }
        Err(anyhow!("Device path must be within /dev/."))
    }
}

impl Display for DevicePathBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.to_string_lossy())
    }
}

impl AsRef<OsStr> for DevicePathBuf {
    fn as_ref(&self) -> &OsStr {
        self.0.as_os_str()
    }
}

impl FromStr for DevicePathBuf {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

// ## Filesystem Mounting ############################################################################################
pub use double::*;
const MOUNT_EXPECTATION: &str = "All entries in mount list must be parsable.";

#[cfg_attr(test, mockall::automock)]
pub mod double {
    use super::*;

    /// Lookup an exact mount entry at target.
    pub fn lookup_mountentry(target: &Path) -> Option<MountEntry> {
        let mut iter = MountIter::new_from_proc().expect(MOUNT_EXPECTATION);
        iter.find_map(|m| match m.expect(MOUNT_EXPECTATION) {
            m if m.file == target => Some(m),
            _ => None,
        })
    }

    pub fn lookup_mountentries_by_devices(devices: &[DevicePathBuf]) -> impl Iterator<Item = MountEntry> {
        let iter = MountIter::new_from_proc().expect(MOUNT_EXPECTATION);
        iter.filter_map(move |m| match m.expect(MOUNT_EXPECTATION) {
            m if DevicePathBuf::try_from(&m.spec)
                .map(|dp| devices.contains(&dp))
                .unwrap_or_default() =>
            {
                Some(m)
            }
            _ => None,
        })
        .collect::<Vec<_>>()
        .into_iter()
    }

    /// Find the mount entry at target or the mount that contains target.
    pub fn find_mountentry(target: &Path) -> Option<MountEntry> {
        mnt::get_mount(target).expect(MOUNT_EXPECTATION)
    }
}

pub fn bind_mount(from: &Path, to: &Path) -> Result<()> {
    let none: Option<&str> = None;
    mount(Some(from), to, none, MsFlags::MS_BIND, none).context("bind mount syscall failed")
}

pub fn unmount(path: &Path) -> Result<()> {
    nix::mount::umount(path).context("unmount syscall failed")
}
#[derive(Debug)]
pub struct BtrfsMountEntry(MountEntry);

impl BtrfsMountEntry {
    pub fn mount_entry(&self) -> &MountEntry {
        &self.0
    }

    pub fn subvolume_id(&self) -> Option<u32> {
        self.keyed_option("subvolid")
    }

    pub fn subvolume_path(&self) -> Option<String> {
        self.keyed_option("subvol")
    }

    pub fn is_toplevel_subvolume(&self) -> bool {
        let subvol_id = self.subvolume_id();
        let subvol_path = self.subvolume_path();

        (subvol_id.is_none() && subvol_path.is_none())
            || subvol_id.unwrap_or_default() == 5
            || subvol_path.unwrap_or_default() == "/"
    }

    pub fn keyed_option<T>(&self, key: &str) -> Option<T>
    where
        T: FromStr,
        T::Err: std::fmt::Debug,
    {
        let prefix = format!("{}=", key);
        self.0.mntops.iter().find_map(|x| match x {
            mnt::MntOps::Extra(extra) if extra.starts_with(prefix.as_str()) => {
                extra.splitn(2, '=').nth(1).and_then(|s| s.parse::<T>().ok())
            }
            _ => None,
        })
    }
}

impl TryFrom<MountEntry> for BtrfsMountEntry {
    type Error = anyhow::Error;

    fn try_from(other: MountEntry) -> Result<Self, Self::Error> {
        match other.vfstype.as_str() {
            "btrfs" => Ok(BtrfsMountEntry { 0: other }),
            x => Err(anyhow!(
                "{} is not a btrfs mount (it's {}).",
                other.file.to_string_lossy(),
                x
            )),
        }
    }
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct BlockDeviceInfo {
    #[serde(rename = "devname")]
    pub name: String,
    pub devtype: String,
    #[serde(rename = "id_model")]
    pub model: Option<String>,
    #[serde(rename = "id_serial")]
    pub serial: Option<String>,
    #[serde(rename = "id_serial_short")]
    pub serial_short: Option<String>,
}

impl BlockDeviceInfo {
    pub fn lookup(device_name: &DevicePathBuf) -> Result<Self> {
        const PROCESS_NAME: &str = "udevadm";
        let output_data = run_command_as_result({
            let mut command = Command::new(PROCESS_NAME);
            command.args(&["info", "--query=property", &format!("--name={}", device_name)]);
            command
        })
        .context(format!("Failed to run {} to get device information.", PROCESS_NAME))?;

        let kvps = parse_key_value_data::<HashMap<String, String>>(&output_data)
            .context(format!("Failed to parse output of {}", PROCESS_NAME))?;

        kvps.get("SUBSYSTEM")
            .filter(|&s| s == "block")
            .ok_or_else(|| anyhow!("Not a block device."))?;

        let mut device_info = envy::from_iter::<_, Self>(kvps.clone()).context(format!(
            "Failed loading the device information from {} output.",
            PROCESS_NAME
        ))?;

        if device_info.model.is_none() && kvps.get("DEVPATH").cloned().unwrap_or_default().contains("/virtio") {
            device_info.model = Some("VirtIO".to_string());
            if device_info.serial.is_none() && device_info.serial_short.is_none() {
                device_info.serial = Some("None".to_string())
            }
        }

        Ok(device_info)
    }
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct BlockDeviceIds {
    #[serde(rename = "devname")]
    pub name: String,
    pub uuid: Option<Uuid>,
    pub uuid_sub: Option<Uuid>,
    pub label: Option<String>,
}

impl BlockDeviceIds {
    pub fn lookup(device_name: &DevicePathBuf) -> Result<Option<Self>> {
        const PROCESS_NAME: &str = "blkid";
        run_command({
            let mut command = Command::new(PROCESS_NAME);
            command.args(&["-o", "export"]).arg(device_name.as_pathbuf());
            command
        })
        .map_err(|e| anyhow!(e))
        .and_then(|output| {
            if output.status.code().unwrap_or_default() == 2 {
                return Ok(None);
            }

            let output_data = output_as_result(output)?;
            let kvps = parse_key_value_data::<Vec<StringPair>>(&output_data)
                .context(format!("Failed to parse output of {}", PROCESS_NAME))?;

            envy::from_iter::<_, Self>(kvps).map(Some).context(format!(
                "Failed loading the device information from {} output.",
                PROCESS_NAME
            ))
        })
        .context(format!("Failed to run {} to get device information.", PROCESS_NAME))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;
    use serial_test::serial;

    #[test]
    fn fail_if_not_btrfs() {
        let non_btrfs_mount: MountEntry = "/dev/vda / ext4 rw 0 0".parse().unwrap();
        assert!(BtrfsMountEntry::try_from(non_btrfs_mount)
            .unwrap_err()
            .to_string()
            .contains("not a btrfs mount"))
    }

    #[test]
    fn no_subvol_options_is_toplevel() {
        assert!(btrfs_without_subvol_opts().is_toplevel_subvolume())
    }

    #[test]
    fn top_subvol_options_is_toplevel() {
        assert!(btrfs_with_top_subvol_opts().is_toplevel_subvolume())
    }

    #[test]
    fn child_subvol_options_is_not_toplevel() {
        assert!(!btrfs_with_child_subvol_opts().is_toplevel_subvolume())
    }

    #[test]
    fn no_subvol_options_parsed() {
        let mount = btrfs_without_subvol_opts();
        assert!(mount.subvolume_id().is_none());
        assert!(mount.subvolume_path().is_none());
    }

    #[test]
    fn subvol_options_parsed() {
        let mount = btrfs_with_child_subvol_opts();
        assert_eq!(mount.subvolume_id().unwrap(), 257);
        assert_eq!(mount.subvolume_path().unwrap(), "/testsub");
    }

    fn btrfs_with_top_subvol_opts() -> BtrfsMountEntry {
        let mount: MountEntry = "/dev/vda / btrfs rw,noatime,subvolid=5,subvol=/ 0 0".parse().unwrap();
        BtrfsMountEntry::try_from(mount).unwrap()
    }

    fn btrfs_with_child_subvol_opts() -> BtrfsMountEntry {
        let mount: MountEntry = "/dev/vda / btrfs rw,noatime,subvolid=257,subvol=/testsub 0 0"
            .parse()
            .unwrap();
        BtrfsMountEntry::try_from(mount).unwrap()
    }

    fn btrfs_without_subvol_opts() -> BtrfsMountEntry {
        let mount: MountEntry = "/dev/vda / btrfs rw,noatime 0 0".parse().unwrap();
        BtrfsMountEntry::try_from(mount).unwrap()
    }

    #[test]
    #[serial(fakecmd)]
    fn nonblock_device_info_fails() {
        const UDEVADM_DATA: &str = indoc!(
            r#"
            DEVPATH=/devices/pci0000:00/0000:00:01.1/0000:01:00.0/nvme/nvme0
            DEVNAME=/dev/nvme0
            NVME_TRTYPE=pcie
            MAJOR=239
            MINOR=0
            SUBSYSTEM=nvme
            USEC_INITIALIZED=4102065
            ID_PCI_CLASS_FROM_DATABASE=Mass storage controller
            ID_PCI_SUBCLASS_FROM_DATABASE=Non-Volatile memory controller
            ID_PCI_INTERFACE_FROM_DATABASE=NVM Express
            ID_VENDOR_FROM_DATABASE=Samsung Electronics Co Ltd
            ID_MODEL_FROM_DATABASE=NVMe SSD Controller SM981/PM981/PM983"#
        );
        let ctx = process_double::run_command_as_result_context();
        ctx.expect().returning(|_| Ok(UDEVADM_DATA.to_string()));

        assert!(BlockDeviceInfo::lookup(&"/dev/nvme0".parse().unwrap())
            .unwrap_err()
            .to_string()
            .contains("Not a block"))
    }

    #[test]
    #[serial(fakecmd)]
    fn block_device_info() {
        const UDEVADM_DATA: &str = indoc!(
            r#"
            DEVPATH=/devices/pci0000:00/0000:00:01.1/0000:01:00.0/nvme/nvme0/nvme0n1
            DEVNAME=/dev/nvme0n1
            DEVTYPE=disk
            MAJOR=259
            MINOR=0
            SUBSYSTEM=block
            USEC_INITIALIZED=3085450
            ID_SERIAL_SHORT=S000000000N
            ID_WWN=eui.0000000000ac
            ID_MODEL=Samsung SSD 970 EVO Plus 500GB
            ID_REVISION=2B2QEXM7
            ID_SERIAL=Samsung SSD 970 EVO Plus 500GB_S000000000N
            ID_PATH=pci-0000:01:00.0-nvme-1
            ID_PATH_TAG=pci-0000_01_00_0-nvme-1
            ID_PART_TABLE_UUID=89d86f9e-7c57-4abb-afc7-28d31b1ceac5
            ID_PART_TABLE_TYPE=gpt
            DEVLINKS=/dev/disk/by-id/nvme-eui.00253851014037ac /dev/disk/by-id/nvme-Samsung_SSD_970_EVO_Plus_500GB_S58SNJ0N104090N /dev/disk/by-path/pci-0000:01:00.0-nvme-1"#
        );
        let ctx = process_double::run_command_as_result_context();
        ctx.expect().returning(|_| Ok(UDEVADM_DATA.to_string()));

        assert_eq!(
            BlockDeviceInfo::lookup(&"/dev/nvme0".parse().unwrap()).unwrap(),
            BlockDeviceInfo {
                name: String::from("/dev/nvme0n1"),
                devtype: String::from("disk"),
                model: Some(String::from("Samsung SSD 970 EVO Plus 500GB"),),
                serial: Some(String::from("Samsung SSD 970 EVO Plus 500GB_S000000000N"),),
                serial_short: Some(String::from("S000000000N"),),
            }
        )
    }

    #[test]
    #[serial(fakecmd)]
    fn block_device_partition_info() {
        const UDEVADM_DATA: &str = indoc!(
            r#"
            DEVPATH=/devices/pci0000:00/0000:00:01.1/0000:01:00.0/nvme/nvme0/nvme0n1/nvme0n1p1
            DEVNAME=/dev/nvme0n1p1
            DEVTYPE=partition
            PARTN=1
            MAJOR=259
            MINOR=1
            SUBSYSTEM=block
            USEC_INITIALIZED=3089303
            ID_SERIAL_SHORT=S000000000N
            ID_MODEL=Samsung SSD 970 EVO Plus 500GB
            ID_REVISION=2B2QEXM7
            ID_SERIAL=Samsung SSD 970 EVO Plus 500GB_S000000000N
            ID_PATH=pci-0000:01:00.0-nvme-1
            ID_PATH_TAG=pci-0000_01_00_0-nvme-1
            ID_PART_TABLE_UUID=89d86f9e-7c57-4abb-afc7-28d31b1ceac5
            ID_PART_TABLE_TYPE=gpt
            ID_FS_UUID=3FE0-C9D7
            ID_FS_UUID_ENC=3FE0-C9D7
            ID_FS_VERSION=FAT32
            ID_FS_TYPE=vfat
            ID_FS_USAGE=filesystem
            ID_PART_ENTRY_SCHEME=gpt
            ID_PART_ENTRY_UUID=dce5a86a-ca34-48e6-904d-aa3020ba5afb
            ID_PART_ENTRY_TYPE=c12a7328-f81f-11d2-ba4b-00a0c93ec93b
            ID_PART_ENTRY_NUMBER=1
            ID_PART_ENTRY_OFFSET=2048
            ID_PART_ENTRY_SIZE=262144
            ID_PART_ENTRY_DISK=259:0
            UDISKS_IGNORE=1
            DEVLINKS=/dev/disk/by-uuid/3FE0-C9D7 /dev/disk/by-id/nvme-eui.00253851014037ac-part1 /dev/disk/by-partuuid/dce5a86a-ca34-48e6-904d-aa3020ba5afb /dev/disk/by-path/pci-0000:01:00.0-nvme-1-part1 /dev/disk/by-id/nvme-Samsung_SSD_970_EVO_Plus_500GB_S58SNJ0N104090N-part1
            TAGS=:systemd:"#
        );
        let ctx = process_double::run_command_as_result_context();
        ctx.expect().returning(|_| Ok(UDEVADM_DATA.to_string()));

        assert_eq!(
            BlockDeviceInfo::lookup(&"/dev/nvme0".parse().unwrap()).unwrap(),
            BlockDeviceInfo {
                name: String::from("/dev/nvme0n1p1"),
                devtype: String::from("partition"),
                model: Some(String::from("Samsung SSD 970 EVO Plus 500GB"),),
                serial: Some(String::from("Samsung SSD 970 EVO Plus 500GB_S000000000N"),),
                serial_short: Some(String::from("S000000000N"),),
            }
        )
    }

    #[test]
    #[serial(fakecmd)]
    fn block_device_partition_ids() {
        const BLKID_DATA: &str = indoc!(
            r#"
            DEVNAME=/dev/nvme0n1p2
            LABEL=default
            UUID=da43bcae-1497-45e7-b17c-512979097fcc
            UUID_SUB=000247c0-4d96-4e55-8955-05eea1d8d121
            TYPE=btrfs
            PARTUUID=725de4b5-9235-4d5d-b7e0-73d87d9c11fd"#
        );
        let ctx = process_double::run_command_as_result_context();
        ctx.expect().returning(|_| Ok(BLKID_DATA.to_string()));
        assert_eq!(
            BlockDeviceIds::lookup(&DevicePathBuf::try_from("/dev/nvme0n1p2").unwrap())
                .unwrap()
                .unwrap(),
            BlockDeviceIds {
                name: String::from("/dev/nvme0n1p2"),
                label: Some(String::from("default"),),
                uuid: Some(Uuid::parse_str("da43bcae-1497-45e7-b17c-512979097fcc").unwrap()),
                uuid_sub: Some(Uuid::parse_str("000247c0-4d96-4e55-8955-05eea1d8d121").unwrap()),
            }
        )
    }
}
