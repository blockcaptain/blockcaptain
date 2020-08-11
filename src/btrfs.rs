use crate::parsing::{parse_key_value_pair_lines, StringPair};
use anyhow::{anyhow, Context, Result};
use duct;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{de, Deserialize, Deserializer};
use std::{
    ffi::OsStr,
    fmt,
    path::{Path, PathBuf},
};
use uuid::Uuid;

macro_rules! btrfs_cmd {
    ( $( $arg:expr ),+ ) => {
        duct_cmd!("btrfs", $($arg),+)
            .read()
            .context(format!("Failed to run btrfs command."))
    };
}

#[derive(Debug, PartialEq)]
pub struct Filesystem {
    pub uuid: Uuid,
    pub devices: Vec<PathBuf>,
}

impl Filesystem {
    pub fn query_device(device: &dyn AsRef<Path>) -> Result<Self> {
        Self::query_raw(device.as_ref().as_os_str())
    }

    pub fn query_uuid(uuid: &Uuid) -> Result<Self> {
        Self::query_raw(uuid.to_string().as_ref())
    }

    fn query_raw(identifier: &OsStr) -> Result<Self> {
        let output_data = btrfs_cmd!("filesystem", "show", "--raw", identifier)?;

        lazy_static! {
            static ref RE_UUID: Regex = Regex::new(r"(?m)\buuid:\s+(.*?)\s*$").unwrap();
            static ref RE_DEVS: Regex = Regex::new(r"(?m)^\s+devid\b.+\bpath\s+(.*?)\s*$").unwrap();
        }
        let uuid_match = RE_UUID.captures(&output_data);
        let device_matches = RE_DEVS.captures_iter(&output_data);

        Ok(Filesystem {
            uuid: uuid_match
                .expect("Successful btrfs fi show should have UUID output.")
                .get(1)
                .unwrap()
                .as_str()
                .parse()
                .expect("UUID should parse."),
            devices: device_matches
                .map(|m| {
                    m.get(1)
                        .unwrap()
                        .as_str()
                        .parse()
                        .expect("Device node path should parse.")
                })
                .collect(),
        })
    }
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct Subvolume {
    pub uuid: Uuid,
    pub name: PathBuf,
    #[serde(rename = "parent uuid")]
    pub parent_uuid: Option<Uuid>,
}

impl Subvolume {
    pub fn from_path<T: AsRef<Path>>(path: T) -> Result<Self> {
        let output_data = btrfs_cmd!("subvolume", "show", "--raw", path.as_ref())?;
        let kvps = parse_key_value_pair_lines::<_, Vec<StringPair>>(output_data.lines().skip(1).take(5), ":")
            .context("Failed to parse output of btrfs subvolume.")?;

        envy::from_iter::<_, Self>(kvps.into_iter().filter_map(|x| {
            if x.1 != "-" {
                Some((x.0.to_uppercase(), x.1))
            } else {
                None
            }
        }))
        .context("Failed loading information from btrfs subvolume output.")
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

        assert_eq!(
            Filesystem::query_device(&PathBuf::from("/dev/sdb")).unwrap(),
            Filesystem {
                uuid: Uuid::parse_str("338a0b41-e857-4e5b-6544-6fd617277722").unwrap(),
                devices: vec![PathBuf::from("/dev/sdb"), PathBuf::from("/dev/sdd")]
            }
        );
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
                Snapshot(s): "#
        );
        let ctx = MockFakeCmd::data_context();
        ctx.expect().returning(|| BTRFS_DATA.to_string());

        assert_eq!(
            Subvolume::from_path(PathBuf::from("/mnt/os_pool")).unwrap(),
            Subvolume {
                name: PathBuf::from("@"),
                uuid: Uuid::parse_str("0c61d287-c754-2944-a71e-ee6f0cbfb40e").unwrap(),
                parent_uuid: None
            }
        );
    }
}
