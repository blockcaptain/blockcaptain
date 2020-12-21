use anyhow::{anyhow, bail, Context as AnyhowContext, Result};
use std::{fs, process::Stdio, sync::Arc};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, BufReader},
    process::{Child, Command},
    task::JoinHandle,
};
use uuid::Uuid;

use super::{BtrfsContainer, BtrfsContainerSnapshot};

pub struct SnapshotSender {
    command: Command,
}

impl SnapshotSender {
    pub fn new(mut command: Command) -> Self {
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
        self.process.await.unwrap();
        Ok(()) // FIXME
    }
}

pub struct SnapshotReceiver {
    command: Command,
    sending_dataset_id: Uuid,
    receiving_container: Arc<BtrfsContainer>,
}

impl SnapshotReceiver {
    pub fn new(mut command: Command, sending_dataset_id: Uuid, receiving_container: Arc<BtrfsContainer>) -> Self {
        command.stdin(Stdio::piped());
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());
        Self {
            command,
            sending_dataset_id,
            receiving_container,
        }
    }

    pub fn start(mut self) -> Result<StartedSnapshotReceiver> {
        self.command.spawn().map_err(|e| anyhow!(e)).map(|mut process| {
            let name_reader_stdout = Self::spawn_name_reader(process.stdout.take().unwrap());
            let name_reader_stderr = Self::spawn_name_reader(process.stderr.take().unwrap());
            StartedSnapshotReceiver {
                process,
                name_reader_stdout,
                name_reader_stderr,
                sending_dataset_id: self.sending_dataset_id,
                receiving_container: self.receiving_container,
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
    sending_dataset_id: Uuid,
    receiving_container: Arc<BtrfsContainer>,
}

impl StartedSnapshotReceiver {
    pub fn writer(&mut self) -> impl AsyncWrite {
        self.process
            .stdin
            .take()
            .expect("child did not have a handle to stdout")
    }

    pub async fn wait(self) -> Result<BtrfsContainerSnapshot> {
        let exit_code = self.process.await.unwrap();
        if !exit_code.success() {
            bail!("FIXME failed yo, {}.", exit_code);
        }
        let stdout_result = self.name_reader_stdout.await.unwrap().unwrap();
        let stderr_result = self.name_reader_stderr.await.unwrap().unwrap();
        let incoming_snapshot_name = stdout_result
            .or(stderr_result)
            .context("Failed to find incoming subvol name.")?;
        let final_snapshot_name = incoming_snapshot_name.clone() + ".bcrcv";
        let container_path = self
            .receiving_container
            .snapshot_container_path(self.sending_dataset_id)
            .as_pathbuf(&self.receiving_container.pool.filesystem.fstree_mountpoint);

        let source_path = container_path.join(incoming_snapshot_name);
        let destination_path = container_path.join(&final_snapshot_name);
        fs::rename(&source_path, &destination_path).with_context(|| {
            format!(
                "Failed to rename the snapshot from '{:?}' to '{:?}' after successfully receiving it.",
                source_path, destination_path
            )
        })?;

        self.receiving_container
            .snapshot_by_name(self.sending_dataset_id, &final_snapshot_name)
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
        let foo = self.process.await.unwrap();
        match foo.code() {
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
