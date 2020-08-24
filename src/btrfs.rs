use crate::filesystem;
use crate::parsing::{parse_key_value_pair_lines, StringPair};
use crate::process::read_with_stderr_context;
use anyhow::{anyhow, bail, Context, Result};
use duct;
use filesystem::BtrfsMountEntry;
use lazy_static::lazy_static;
use regex::Regex;
use serde::Deserialize;
use std::convert::TryFrom;
use std::string::String;
use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
};
use uuid::Uuid;

macro_rules! btrfs_cmd {
    ( $( $arg:expr ),+ ) => {
        read_with_stderr_context(duct_cmd!("btrfs", $($arg),+).stdout_capture())
    };
}

#[derive(Debug, PartialEq)]
pub struct Filesystem {
    pub uuid: Uuid,
    pub devices: Vec<PathBuf>,
}

#[derive(Debug, PartialEq)]
pub struct MountedFilesystem {
    pub filesystem: Filesystem,
    pub fstree_mountpoint: PathBuf,
}

pub enum QueriedFilesystem {
    Unmounted(Filesystem),
    Mounted(MountedFilesystem),
}

impl QueriedFilesystem {
    pub fn unwrap_mounted(self) -> Result<MountedFilesystem> {
        match self {
            QueriedFilesystem::Mounted(m) => Ok(m),
            QueriedFilesystem::Unmounted(u) => Err(anyhow!("Filesystem {} exists, but has no fstree mount.", u.uuid)),
        }
    }
}

impl Filesystem {
    pub fn query_device(device: &Path) -> Result<QueriedFilesystem> {
        Self::query_raw(device.as_os_str())
    }

    pub fn query_uuid(uuid: &Uuid) -> Result<QueriedFilesystem> {
        Self::query_raw(uuid.to_string().as_ref())
    }

    pub fn query_path(path: &Path) -> Result<QueriedFilesystem> {
        Self::query_raw(path.as_os_str())
    }

    fn query_raw(identifier: &OsStr) -> Result<QueriedFilesystem> {
        let output_data = btrfs_cmd!("filesystem", "show", "--raw", identifier)?;

        lazy_static! {
            static ref RE_UUID: Regex = Regex::new(r"(?m)\buuid:\s+(.*?)\s*$").unwrap();
            static ref RE_DEVS: Regex = Regex::new(r"(?m)^\s+devid\b.+\bpath\s+(.*?)\s*$").unwrap();
        }
        let uuid_match = RE_UUID.captures(&output_data);
        let device_matches = RE_DEVS.captures_iter(&output_data);

        let devices = device_matches
            .map(|m| {
                m.get(1)
                    .unwrap()
                    .as_str()
                    .parse()
                    .expect("Device node path should parse.")
            })
            .collect::<Vec<_>>();

        let fstree_mountpoint =
            filesystem::lookup_mountentries_by_devices(&devices).find_map(|m| match BtrfsMountEntry::try_from(m) {
                Ok(bm) if bm.is_toplevel_subvolume() => Some(bm.mount_entry().file.to_owned()),
                _ => None,
            });

        let filesystem = Filesystem {
            uuid: uuid_match
                .expect("Successful btrfs fi show should have UUID output.")
                .get(1)
                .unwrap()
                .as_str()
                .parse()
                .expect("UUID should parse."),
            devices: devices,
        };

        Ok(match fstree_mountpoint {
            Some(fstree_mountpoint) => QueriedFilesystem::Mounted(MountedFilesystem {
                filesystem,
                fstree_mountpoint,
            }),
            None => QueriedFilesystem::Unmounted(filesystem),
        })
    }
}

impl MountedFilesystem {
    pub fn subvolume_by_uuid(&self, uuid: &Uuid) -> Result<Subvolume> {
        let output_data = String::from("path: ")
            + btrfs_cmd!(
                "subvolume",
                "show",
                "--raw",
                "-u",
                uuid.to_string(),
                &self.fstree_mountpoint
            )?
            .as_ref();
        Subvolume::_parse(output_data)
    }

    pub fn subvolume_by_path(&self, path: &Path) -> Result<Subvolume> {
        Subvolume::from_path(&self.fstree_mountpoint.join(path))
    }

    pub fn snapshot_subvolume(&self, subvolume: &Subvolume, path: &Path) -> Result<()> {
        let target_path = self.fstree_mountpoint.join(path);
        if target_path.exists() {
            bail!("Path to new snapshot, {:?}, already exists!", &target_path)
        }
        let _ = btrfs_cmd!(
            "subvolume",
            "snapshot",
            "-r",
            self.fstree_mountpoint.join(&subvolume.path),
            target_path
        )
        .context(format!("Failed to create btrfs snapshot at {:?}.", path))?;
        Ok(())
    }

    pub fn create_subvolume(&self, path: &Path) -> Result<()> {
        let target_path = self.fstree_mountpoint.join(path);
        if target_path.exists() {
            bail!("Path to new subvolume, {:?}, already exists!", &target_path)
        }
        let _ = btrfs_cmd!("subvolume", "create", target_path)
            .context(format!("Failed to create btrfs subvolume at {:?}.", path))?;
        Ok(())
    }
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct Subvolume {
    pub uuid: Uuid,
    pub name: String,
    pub path: PathBuf,
    #[serde(rename = "parent uuid")]
    pub parent_uuid: Option<Uuid>,
    #[serde(skip_deserializing)]
    pub snapshot_paths: Vec<PathBuf>,
}

impl Subvolume {
    pub fn from_path(path: &Path) -> Result<Self> {
        let output_data = String::from("path: ") + btrfs_cmd!("subvolume", "show", "--raw", path)?.as_ref();
        Self::_parse(output_data)
    }

    fn _parse(data: String) -> Result<Self> {
        let kvps = parse_key_value_pair_lines::<_, Vec<StringPair>>(data.lines().take(6), ":")
            .context("Failed to parse output of btrfs subvolume.")?;

        let mut subvolume = envy::from_iter::<_, Self>(kvps.into_iter().filter_map(|x| {
            if x.1 != "-" {
                Some((x.0.to_uppercase(), x.1))
            } else {
                None
            }
        }))
        .context("Failed loading information from btrfs subvolume output.")?;

        let snapshot_lines = data.lines().skip_while(|s| !s.contains("Snapshot(s):")).skip(1);
        subvolume.snapshot_paths = snapshot_lines.map(|s| PathBuf::from(s.trim())).collect();
        Ok(subvolume)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process::mocks::MockFakeCmd;
    use indoc::indoc;
    use serial_test::serial;

    #[test]
    #[serial(fakecmd)]
    fn btrfs_filesystem_show_parses() {
        const BTRFS_DATA: &str = indoc!(
            r#"
            Label: 'nas_mirrored'  uuid: 338a0b41-e857-4e5b-6544-6fd617277722
            	Total devices 2 FS bytes used 359263784960
            	devid    1 size 2000398934016 used 381220290560 path /dev/sdb 
            	devid    2 size 2000398934016 used 381220290560 path /dev/sdd"#
        );
        let ctx = MockFakeCmd::data_context();
        ctx.expect().returning(|| BTRFS_DATA.to_string());

        if let QueriedFilesystem::Unmounted(fs) = Filesystem::query_device(&PathBuf::from("/dev/sdb")).unwrap() {
            assert_eq!(
                fs,
                Filesystem {
                    uuid: Uuid::parse_str("338a0b41-e857-4e5b-6544-6fd617277722").unwrap(),
                    devices: vec![PathBuf::from("/dev/sdb"), PathBuf::from("/dev/sdd")]
                }
            );
        } else {
            assert!(false, "Test fs should be unmounted.")
        }
    }

    #[test]
    #[serial(fakecmd)]
    fn btrfs_subvolume_show_parses() {
        const BTRFS_DATA: &str = indoc!(
            r#"
            @
                Name: 			@
                UUID: 			0c61d287-c754-2944-a71e-ee6f0cbfb40e
                Parent UUID: 		-
                Received UUID: 		-
                Creation time: 		2020-08-06 04:14:17 +0000
                Subvolume ID: 		256
                Generation: 		587
                Gen at creation: 	6
                Parent ID: 		5
                Top level ID: 		5
                Flags: 			-
                Snapshot(s):
                            .blkcapt/snapshots/8a7ae0b5-b28c-b240-8c07-0015431d58d8/2020-08-23T17-20-10Z
				            .blkcapt/snapshots/8a7ae0b5-b28c-b240-8c07-0015431d58d8/2020-08-23T17-24-02Z
				            .blkcapt/snapshots/8a7ae0b5-b28c-b240-8c07-0015431d58d8/2020-08-23T20-14-53Z"#
        );
        let ctx = MockFakeCmd::data_context();
        ctx.expect().returning(|| BTRFS_DATA.to_string());

        assert_eq!(
            Subvolume::from_path(&PathBuf::from("/mnt/os_pool")).unwrap(),
            Subvolume {
                name: String::from("@"),
                path: PathBuf::from("@"),
                uuid: Uuid::parse_str("0c61d287-c754-2944-a71e-ee6f0cbfb40e").unwrap(),
                parent_uuid: None,
                snapshot_paths: vec![
                    PathBuf::from(".blkcapt/snapshots/8a7ae0b5-b28c-b240-8c07-0015431d58d8/2020-08-23T17-20-10Z"),
                    PathBuf::from(".blkcapt/snapshots/8a7ae0b5-b28c-b240-8c07-0015431d58d8/2020-08-23T17-24-02Z"),
                    PathBuf::from(".blkcapt/snapshots/8a7ae0b5-b28c-b240-8c07-0015431d58d8/2020-08-23T20-14-53Z"),
                ]
            }
        );
    }
}
