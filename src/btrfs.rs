use anyhow::{anyhow, Context, Result};
use duct;
use lazy_static::lazy_static;
use regex::Regex;
use std::{ffi::OsStr, path::{Path, PathBuf}};
use uuid::Uuid;

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
        const PROCESS_NAME: &str = "btrfs";
        let output_data = duct_cmd!(PROCESS_NAME, "filesystem", "show", "--raw", identifier)
            .read()
            .context(format!("Failed to run {} to get filesystem information.", PROCESS_NAME))?;

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
}
