use super::fs::{BtrfsMountEntry, DevicePathBuf, FsPathBuf};
use crate::parsing::{parse_key_value_pair_lines, parse_uuid, StringPair};
#[mockall_double::double]
use crate::sys::{fs::double as fs_double, process::double as process_double};
use anyhow::{anyhow, bail, Context, Result};
use fs_double::lookup_mountentries_by_devices;
pub use operations::*;
use process_double::run_command_as_result;
use serde::Deserialize;
use std::string::String;
use std::{convert::TryFrom, process::Command};
use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
};
use uuid::Uuid;

fn btrfs_command() -> Command {
    Command::new("btrfs")
}

macro_rules! once_regex {
    ($re:literal $(,)?) => {{
        static RE: once_cell::sync::OnceCell<regex::Regex> = once_cell::sync::OnceCell::new();
        RE.get_or_init(|| regex::Regex::new($re).unwrap())
    }};
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

#[derive(Debug, PartialEq)]
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
        let output_data = run_command_as_result({
            let mut command = btrfs_command();
            command.args(&["filesystem", "show", "--raw"]).arg(identifier);
            command
        })?;

        let uuid_regex = once_regex!(r"(?m)\buuid:\s+(.*?)\s*$");
        let devs_regex = once_regex!(r"(?m)^\s+devid\b.+\bpath\s+(.*?)\s*$");

        let uuid_match = uuid_regex.captures(&output_data);
        let device_matches = devs_regex.captures_iter(&output_data);

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
            lookup_mountentries_by_devices(&devices).find_map(|m| match BtrfsMountEntry::try_from(m) {
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
            devices,
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
        let output_data = run_command_as_result({
            let mut command = btrfs_command();
            command
                .args(&["subvolume", "show", "--raw", "-u"])
                .arg(uuid.to_string())
                .arg(&self.fstree_mountpoint);
            command
        })?;
        Subvolume::_parse(String::from("path: ") + &output_data)
    }

    pub fn subvolume_by_path(&self, path: &FsPathBuf) -> Result<Subvolume> {
        Subvolume::from_path(&path.as_pathbuf(&self.fstree_mountpoint))
    }

    pub fn create_snapshot(&self, subvolume: &Subvolume, path: &FsPathBuf) -> Result<()> {
        let target_path = path.as_pathbuf(&self.fstree_mountpoint);
        if target_path.exists() {
            bail!("Path to new snapshot, {:?}, already exists!", &target_path)
        }
        run_command_as_result({
            let mut command = btrfs_command();
            command
                .args(&["subvolume", "snapshot", "-r"])
                .arg(subvolume.path.as_pathbuf(&self.fstree_mountpoint))
                .arg(target_path);
            command
        })
        .context(format!("Failed to create btrfs snapshot at {:?}.", path))
        .map(|_| ())
    }

    pub fn create_subvolume(&self, path: &FsPathBuf) -> Result<()> {
        let target_path = path.as_pathbuf(&self.fstree_mountpoint);
        if target_path.exists() {
            bail!("Path to new subvolume, {:?}, already exists!", &target_path)
        }
        run_command_as_result({
            let mut command = btrfs_command();
            command.args(&["subvolume", "create"]).arg(target_path);
            command
        })
        .context(format!("Failed to create btrfs subvolume at {:?}.", path))
        .map(|_| ())
    }

    pub fn delete_subvolume(&self, path: &FsPathBuf) -> Result<()> {
        let target_path = path.as_pathbuf(&self.fstree_mountpoint);
        if !target_path.exists() {
            bail!("Path to subvolume, {:?}, is non-existant!", &target_path)
        }
        run_command_as_result({
            let mut command = btrfs_command();
            command.args(&["subvolume", "delete"]).arg(target_path);
            command
        })
        .context(format!("Failed to delete btrfs subvolume at {:?}.", path))
        .map(|_| ())
    }

    pub fn send_subvolume(&self, path: &FsPathBuf, parent: Option<&FsPathBuf>) -> SnapshotSender {
        let mut command = tokio::process::Command::new("btrfs");
        let source_snap_path = path.as_pathbuf(&self.fstree_mountpoint);
        match parent {
            Some(parent_snapshot) => {
                let parent_snap_path = parent_snapshot.as_pathbuf(&self.fstree_mountpoint);
                command
                    .arg("send")
                    .arg("-p")
                    .arg(parent_snap_path)
                    .arg(source_snap_path)
            }
            None => command.arg("send").arg(source_snap_path),
        };
        SnapshotSender::new(command)
    }

    pub fn receive_subvolume(&self, into_path: &FsPathBuf) -> SnapshotReceiver {
        let mut command = tokio::process::Command::new("btrfs");
        let target_into_path = into_path.as_pathbuf(&self.fstree_mountpoint);
        command.arg("receive").arg(target_into_path);
        SnapshotReceiver::new(command)
    }

    pub fn list_subvolumes(&self, path: &FsPathBuf) -> Result<Vec<Subvolume>> {
        let target_path = path.as_pathbuf(&self.fstree_mountpoint);
        Subvolume::list_subvolumes(&target_path)
    }

    pub fn scrub(&self) -> PoolScrub {
        let mut command = tokio::process::Command::new("btrfs");
        command.args(&["scrub", "start", "-BRd"]).arg(&self.fstree_mountpoint);
        PoolScrub::new(command)
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
        let output_data = run_command_as_result({
            let mut command = btrfs_command();
            command.args(&["subvolume", "show", "--raw"]).arg(path);
            command
        })?;
        Self::_parse(String::from("path: ") + &output_data)
    }

    pub fn list_subvolumes(path: &Path) -> Result<Vec<Subvolume>> {
        let paths_regex =
            once_regex!(r"(?m)\bparent_uuid\s+(.*?)\s+received_uuid\s+(.*?)\s+uuid\s+(.*?)\s+path\s+(.*?)\s*$");
        let output_data = run_command_as_result({
            let mut command = btrfs_command();
            command.args(&["subvolume", "list", "-uqRo"]).arg(path);
            command
        })?;
        let path_matches = paths_regex.captures_iter(&output_data);
        let parse_uuid = |m| parse_uuid(m).expect("Should always have parsable UUID in btrfs list.");
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

mod operations {
    use crate::sys::process::exit_status_as_result;
    use anyhow::{anyhow, Context as AnyhowContext, Result};
    use std::process::Stdio;
    use tokio::{
        io::{AsyncBufReadExt, AsyncRead, AsyncWrite, BufReader},
        process::{Child, Command},
        task::JoinHandle,
    };

    pub struct SnapshotSender {
        command: Command,
    }

    impl SnapshotSender {
        pub(super) fn new(mut command: Command) -> Self {
            command.stdout(Stdio::piped());
            command.stderr(Stdio::null());
            Self { command }
        }

        pub fn start(mut self) -> Result<StartedSnapshotSender> {
            self.command
                .spawn()
                .map(|process| StartedSnapshotSender { process })
                .map_err(|e| anyhow!(e))
        }
    }

    pub struct StartedSnapshotSender {
        process: Child,
    }

    impl StartedSnapshotSender {
        pub fn reader(&mut self) -> impl AsyncRead {
            self.process
                .stdout
                .take()
                .expect("child did not have a handle to stdout")
        }

        pub async fn wait(self) -> Result<()> {
            exit_status_as_result(self.process.wait().await?)
        }
    }

    pub struct SnapshotReceiver {
        command: Command,
    }

    impl SnapshotReceiver {
        pub(super) fn new(mut command: Command) -> Self {
            command.stdin(Stdio::piped());
            command.stdout(Stdio::piped());
            command.stderr(Stdio::piped());
            Self { command }
        }

        pub fn start(mut self) -> Result<StartedSnapshotReceiver> {
            self.command.spawn().map_err(|e| anyhow!(e)).map(|mut process| {
                let name_reader_stdout = Self::spawn_name_reader(process.stdout.take().expect("only taken once"));
                let name_reader_stderr = Self::spawn_name_reader(process.stderr.take().expect("only taken once"));
                StartedSnapshotReceiver {
                    process,
                    name_reader_stdout,
                    name_reader_stderr,
                }
            })
        }

        fn spawn_name_reader(handle: impl AsyncRead + Unpin + Send + 'static) -> JoinHandle<Result<Option<String>>> {
            tokio::spawn(async move {
                const PREFIX1: &str = "At subvol ";
                const PREFIX1_LEN: usize = PREFIX1.len();
                const PREFIX2: &str = "At snapshot ";
                const PREFIX2_LEN: usize = PREFIX2.len();
                let mut reader = BufReader::new(handle);
                let mut buffer = String::new();
                let mut result = None;
                while reader.read_line(&mut buffer).await? > 0 {
                    if result.is_none() {
                        if buffer.starts_with(PREFIX1) && buffer.len() > PREFIX1_LEN {
                            result = Some(buffer[PREFIX1_LEN..].trim().to_string());
                        } else if buffer.starts_with(PREFIX2) && buffer.len() > PREFIX2_LEN {
                            result = Some(buffer[PREFIX2_LEN..].trim().to_string());
                        }
                    }
                    buffer.clear();
                }
                Ok(result)
            })
        }
    }

    pub struct StartedSnapshotReceiver {
        process: Child,
        name_reader_stdout: JoinHandle<Result<Option<String>>>,
        name_reader_stderr: JoinHandle<Result<Option<String>>>,
    }

    impl StartedSnapshotReceiver {
        pub fn writer(&mut self) -> impl AsyncWrite {
            self.process
                .stdin
                .take()
                .expect("child did not have a handle to stdout")
        }

        pub async fn wait(self) -> Result<String> {
            exit_status_as_result(self.process.wait().await?)?;
            let stdout_result = self.name_reader_stdout.await.expect("task doesn't panic")?;
            let stderr_result = self.name_reader_stderr.await.expect("task doesn't panic")?;
            let incoming_snapshot_name = stdout_result
                .or(stderr_result)
                .context("failed to find incoming subvol name")?;
            Ok(incoming_snapshot_name)
        }
    }

    pub struct PoolScrub {
        command: Command,
    }

    impl PoolScrub {
        pub fn new(mut command: Command) -> Self {
            command.stdout(Stdio::piped());
            command.stderr(Stdio::null());
            Self { command }
        }

        pub fn start(mut self) -> Result<StartedPoolScrub> {
            self.command
                .spawn()
                .map(|process| StartedPoolScrub { process })
                .context("failed to spawn btrfs scrub process")
        }
    }

    pub struct StartedPoolScrub {
        process: Child,
    }

    impl StartedPoolScrub {
        pub async fn wait(self) -> Result<(), ScrubError> {
            let exit_status = self.process.wait().await.map_err(|_| ScrubError::Unknown)?;
            match exit_status.code() {
                Some(code) => match code {
                    0 => Ok(()),
                    3 => Err(ScrubError::UncorrectableErrors),
                    _ => Err(ScrubError::Unknown),
                },
                None => {
                    slog_scope::error!("scrub process terminated");
                    Err(ScrubError::Unknown)
                }
            }
        }
    }
    #[derive(thiserror::Error, Debug)]
    pub enum ScrubError {
        #[error("scrub process failed to complete")]
        Unknown,
        #[error("uncorrectable errors were found during scrub")]
        UncorrectableErrors,
    }
}

#[cfg(test)]
mod filesystem_tests {
    use super::*;
    use crate::tests::prelude::*;

    #[test]
    #[serial(fakecmd)]
    fn filesystem_query_unmounted() {
        let _process_ctx = process_context();
        let fs_ctx = fs_double::lookup_mountentries_by_devices_context();
        fs_ctx.expect().returning(|_| Box::new(Vec::default().into_iter()));

        let result = Filesystem::query_device(&DevicePathBuf::try_from("/dev/sdb").unwrap()).unwrap();
        assert_eq!(result, QueriedFilesystem::Unmounted(expected_filesystem()));
    }

    #[test]
    #[serial(fakecmd)]
    fn filesystem_query_mounted_subvol() {
        let _process_ctx = process_context();
        let fs_ctx = fs_double::lookup_mountentries_by_devices_context();
        fs_ctx.expect().returning(|_| {
            Box::new(
                vec!["/dev/sdd /mnt/test btrfs rw,noatime,subvolid=360,subvol=/data 0 0"
                    .parse()
                    .unwrap()]
                .into_iter(),
            )
        });

        let result = Filesystem::query_device(&DevicePathBuf::try_from("/dev/sdb").unwrap()).unwrap();
        assert_eq!(result, QueriedFilesystem::Unmounted(expected_filesystem()));
    }

    #[test]
    #[serial(fakecmd)]
    fn filesystem_query_mounted_entire_fs() {
        let _process_ctx = process_context();
        let fs_ctx = fs_double::lookup_mountentries_by_devices_context();
        fs_ctx.expect().returning(|_| {
            Box::new(
                vec!["/dev/sdd /mnt/test btrfs rw,noatime,subvolid=5,subvol=/ 0 0"
                    .parse()
                    .unwrap()]
                .into_iter(),
            )
        });

        let result = Filesystem::query_device(&DevicePathBuf::try_from("/dev/sdb").unwrap()).unwrap();
        assert_eq!(
            result,
            QueriedFilesystem::Mounted(MountedFilesystem {
                filesystem: expected_filesystem(),
                fstree_mountpoint: "/mnt/test".into(),
            })
        );
    }

    fn process_context() -> process_double::__run_command_as_result::Context {
        const BTRFS_DATA: &str = indoc!(
            r#"
            Label: 'nas_mirrored'  uuid: 338a0b41-e857-4e5b-6544-6fd617277722
            	Total devices 2 FS bytes used 359263784960
            	devid    1 size 2000398934016 used 381220290560 path /dev/sdb 
            	devid    2 size 2000398934016 used 381220290560 path /dev/sdd"#
        );
        let process_ctx = process_double::run_command_as_result_context();
        process_ctx.expect().returning(|_| Ok(BTRFS_DATA.to_string()));
        process_ctx
    }

    fn expected_filesystem() -> Filesystem {
        Filesystem {
            uuid: Uuid::parse_str("338a0b41-e857-4e5b-6544-6fd617277722").unwrap(),
            devices: vec![
                DevicePathBuf::try_from("/dev/sdb").unwrap(),
                DevicePathBuf::try_from("/dev/sdd").unwrap(),
            ],
        }
    }
}

#[cfg(test)]
mod subvolume_tests {
    use super::*;
    use crate::tests::prelude::*;

    #[test]
    #[serial(fakecmd)]
    fn subvolume_from_path() {
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
        let ctx = process_double::run_command_as_result_context();
        ctx.expect().returning(|_| Ok(BTRFS_DATA.to_string()));

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
    fn subvolume_list() {
        const BTRFS_DATA: &str = indoc!(
            r#"
            ID 260 gen 48 cgen 8 parent 5 top level 5 parent_uuid -                                    received_uuid -                                    uuid 8a7ae0b5-b28c-b240-8c07-0015431d58d8 path test4
            ID 261 gen 9 cgen 9 parent 260 top level 260 parent_uuid -                                    received_uuid -                                    uuid ed4c840e-934f-9c49-bcac-fa8a1be864ff path test4/test5
            ID 273 gen 47 cgen 33 parent 5 top level 5 parent_uuid -                                    received_uuid -                                    uuid 45700e9d-9cba-f840-bf2b-b165b87623b7 path .blkcapt/snapshots
            ID 284 gen 50 cgen 47 parent 273 top level 273 parent_uuid -                                    received_uuid -                                    uuid 0cdd2cd3-8e63-4749-adb5-e63a1050b3ea path .blkcapt/snapshots/b99a584c-72c0-4cbe-9c6d-0c32274563f7
            ID 285 gen 48 cgen 48 parent 284 top level 284 parent_uuid 8a7ae0b5-b28c-b240-8c07-0015431d58d8 received_uuid -                                    uuid 269b40d7-e072-954e-9138-04cbef62a13f path .blkcapt/snapshots/b99a584c-72c0-4cbe-9c6d-0c32274563f7/2020-08-26T21-25-26Z"#
        );
        let ctx = process_double::run_command_as_result_context();
        ctx.expect().returning(|_| Ok(BTRFS_DATA.to_string()));

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
                    path: FsPathBuf::from(
                        ".blkcapt/snapshots/b99a584c-72c0-4cbe-9c6d-0c32274563f7/2020-08-26T21-25-26Z"
                    ),
                    uuid: Uuid::parse_str("269b40d7-e072-954e-9138-04cbef62a13f").unwrap(),
                    parent_uuid: Some(Uuid::parse_str("8a7ae0b5-b28c-b240-8c07-0015431d58d8").unwrap()),
                    received_uuid: None,
                }
            ]
        );
    }
}
