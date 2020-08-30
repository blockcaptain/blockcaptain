use super::fs::{self, BtrfsMountEntry};
use crate::parsing::{parse_key_value_pair_lines, StringPair};
use crate::process::read_with_stderr_context;
use anyhow::{anyhow, bail, Context, Result};
use duct;
use lazy_static::lazy_static;
use regex::{Captures, Regex};
use serde::Deserialize;
use std::convert::TryFrom;
use std::string::String;
use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
};
use uuid::Uuid;
use fs::{DevicePathBuf, FsPathBuf};

macro_rules! btrfs_cmd {
    ( $( $arg:expr ),+ ) => {
        read_with_stderr_context(duct_cmd!("btrfs", $($arg),+).stdout_capture())
    };
}

#[derive(Debug, PartialEq)]
pub struct Filesystem {
    pub uuid: Uuid,
    pub devices: Vec<DevicePathBuf>,
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
    pub fn query_device(device: &DevicePathBuf) -> Result<QueriedFilesystem> {
        Self::query_raw(device.as_pathbuf().as_os_str())
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
            fs::lookup_mountentries_by_devices(&devices).find_map(|m| match BtrfsMountEntry::try_from(m) {
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
            subvolume.path.as_pathbuf(&self.fstree_mountpoint),
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

#[derive(Deserialize, Debug, Clone, PartialEq)]
pub struct Subvolume {
    pub uuid: Uuid,
    pub path: FsPathBuf,
    #[serde(rename = "parent uuid")]
    pub parent_uuid: Option<Uuid>,
    #[serde(rename = "received uuid")]
    pub received_uuid: Option<Uuid>,
}

impl Subvolume {
    pub fn from_path(path: &Path) -> Result<Self> {
        let output_data = String::from("path: ") + btrfs_cmd!("subvolume", "show", "--raw", path)?.as_ref();
        Self::_parse(output_data)
    }

    pub fn list_subvolumes(path: &Path) -> Result<Vec<Subvolume>> {
        lazy_static! {
            static ref RE_PATHS: Regex =
                Regex::new(r"(?m)\bparent_uuid\s+(.*?)\s+received_uuid\s+(.*?)\s+uuid\s+(.*?)\s+path\s+(.*?)\s*$")
                    .unwrap();
        }

        let output_data = btrfs_cmd!("subvolume", "list", "-uqRo", path)?;
        let path_matches = RE_PATHS.captures_iter(&output_data);
        let parse_uuid = |m| Uuid::parse_str(m).expect("Should always have parsable UUID in btrfs list.");
        Ok(path_matches
            .map(|m| Self {
                uuid: parse_uuid(m.get(3).unwrap().as_str()),
                path: FsPathBuf::from(m.get(4).unwrap().as_str()),
                parent_uuid: match m.get(1).unwrap().as_str() {
                    "-" => None,
                    s => Some(parse_uuid(s)),
                },
                received_uuid: match m.get(2).unwrap().as_str() {
                    "-" => None,
                    s => Some(parse_uuid(s)),
                },
            })
            .collect::<Vec<_>>())
    }

    fn _parse(data: String) -> Result<Self> {
        let kvps = parse_key_value_pair_lines::<_, Vec<StringPair>>(data.lines().take(6), ":")
            .context("Failed to parse output of btrfs subvolume.")?;

        let subvolume = envy::from_iter::<_, Self>(kvps.into_iter().filter_map(|x| {
            if x.1 != "-" {
                Some((x.0.to_uppercase(), x.1))
            } else {
                None
            }
        }))
        .context("Failed loading information from btrfs subvolume output.")?;
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

        if let QueriedFilesystem::Unmounted(fs) = Filesystem::query_device(&DevicePathBuf::try_from("/dev/sdb").unwrap()).unwrap() {
            assert_eq!(
                fs,
                Filesystem {
                    uuid: Uuid::parse_str("338a0b41-e857-4e5b-6544-6fd617277722").unwrap(),
                    devices: vec![DevicePathBuf::try_from("/dev/sdb").unwrap(), DevicePathBuf::try_from("/dev/sdd").unwrap()]
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
                path: FsPathBuf::from("@"),
                uuid: Uuid::parse_str("0c61d287-c754-2944-a71e-ee6f0cbfb40e").unwrap(),
                parent_uuid: None,
                received_uuid: None,
            }
        );
    }

    #[test]
    #[serial(fakecmd)]
    fn btrfs_subvolume_list_parses() {
        const BTRFS_DATA: &str = indoc!(
            r#"
            ID 260 gen 48 cgen 8 parent 5 top level 5 parent_uuid -                                    received_uuid -                                    uuid 8a7ae0b5-b28c-b240-8c07-0015431d58d8 path test4
            ID 261 gen 9 cgen 9 parent 260 top level 260 parent_uuid -                                    received_uuid -                                    uuid ed4c840e-934f-9c49-bcac-fa8a1be864ff path test4/test5
            ID 273 gen 47 cgen 33 parent 5 top level 5 parent_uuid -                                    received_uuid -                                    uuid 45700e9d-9cba-f840-bf2b-b165b87623b7 path .blkcapt/snapshots
            ID 284 gen 50 cgen 47 parent 273 top level 273 parent_uuid -                                    received_uuid -                                    uuid 0cdd2cd3-8e63-4749-adb5-e63a1050b3ea path .blkcapt/snapshots/b99a584c-72c0-4cbe-9c6d-0c32274563f7
            ID 285 gen 48 cgen 48 parent 284 top level 284 parent_uuid 8a7ae0b5-b28c-b240-8c07-0015431d58d8 received_uuid -                                    uuid 269b40d7-e072-954e-9138-04cbef62a13f path .blkcapt/snapshots/b99a584c-72c0-4cbe-9c6d-0c32274563f7/2020-08-26T21-25-26Z"#
        );
        let ctx = MockFakeCmd::data_context();
        ctx.expect().returning(|| BTRFS_DATA.to_string());

        assert_eq!(
            Subvolume::list_subvolumes(&PathBuf::from("/mnt/data_pool")).unwrap(),
            vec![
                Subvolume {
                    path: FsPathBuf::from("test4"),
                    uuid: Uuid::parse_str("8a7ae0b5-b28c-b240-8c07-0015431d58d8").unwrap(),
                    parent_uuid: None,
                    received_uuid: None,
                },
                Subvolume {
                    path: FsPathBuf::from("test4/test5"),
                    uuid: Uuid::parse_str("ed4c840e-934f-9c49-bcac-fa8a1be864ff").unwrap(),
                    parent_uuid: None,
                    received_uuid: None,
                },
                Subvolume {
                    path: FsPathBuf::from(".blkcapt/snapshots"),
                    uuid: Uuid::parse_str("45700e9d-9cba-f840-bf2b-b165b87623b7").unwrap(),
                    parent_uuid: None,
                    received_uuid: None,
                },
                Subvolume {
                    path: FsPathBuf::from(".blkcapt/snapshots/b99a584c-72c0-4cbe-9c6d-0c32274563f7"),
                    uuid: Uuid::parse_str("0cdd2cd3-8e63-4749-adb5-e63a1050b3ea").unwrap(),
                    parent_uuid: None,
                    received_uuid: None,
                },
                Subvolume {
                    path: FsPathBuf::from(".blkcapt/snapshots/b99a584c-72c0-4cbe-9c6d-0c32274563f7/2020-08-26T21-25-26Z"),
                    uuid: Uuid::parse_str("269b40d7-e072-954e-9138-04cbef62a13f").unwrap(),
                    parent_uuid: Some(Uuid::parse_str("8a7ae0b5-b28c-b240-8c07-0015431d58d8").unwrap()),
                    received_uuid: None,
                }
            ]
        );
    }
}
