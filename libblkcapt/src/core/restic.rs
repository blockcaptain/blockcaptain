use super::{parse_snapshot_label, SnapshotHandle};
use crate::{
    model::{entities::ResticContainerEntity, Entity},
    sys::fs::{bind_mount, unmount},
};
use anyhow::{anyhow, bail, Context, Error, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer};
use std::{fmt::Display, fs, path::PathBuf, process::Stdio, str::FromStr, sync::Arc};
use tokio::{
    io::AsyncBufReadExt,
    io::BufReader,
    process::{Child, ChildStdout, Command},
    task::JoinHandle,
};
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq)]
pub struct ResticContainerSnapshot {
    pub datetime: DateTime<Utc>,
    pub dataset_id: Uuid,
    pub uuid: ResticId,
    pub received_uuid: Uuid,
}

impl From<&ResticContainerSnapshot> for SnapshotHandle {
    fn from(snapshot: &ResticContainerSnapshot) -> Self {
        Self {
            datetime: snapshot.datetime,
            uuid: snapshot.uuid.low,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ResticId {
    low: Uuid,
    high: Uuid,
}

impl<'de> Deserialize<'de> for ResticId {
    fn deserialize<D>(deserializer: D) -> Result<ResticId, D::Error>
    where
        D: Deserializer<'de>,
    {
        let string = String::deserialize(deserializer)?;
        ResticId::from_str(&string).map_err(serde::de::Error::custom)
    }
}

impl FromStr for ResticId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 64 {
            bail!("restic ID should be 64 characters");
        }
        Ok(Self {
            low: Uuid::from_str(&s[..32])?,
            high: Uuid::from_str(&s[32..])?,
        })
    }
}

impl Display for ResticId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.low.to_simple(), self.high.to_simple())
    }
}

pub struct ResticRepository {
    model: ResticContainerEntity,
}

impl ResticRepository {
    pub fn validate(model: ResticContainerEntity) -> Result<Self> {
        Ok(Self { model })
    }

    pub fn backup(self: &Arc<Self>, bind_at: PathBuf, dataset_id: Uuid, snapshot: SnapshotHandle) -> ResticBackup {
        let command = self.new_command();
        ResticBackup::new(command, bind_at, dataset_id, snapshot)
    }

    pub async fn snapshots(self: &Arc<Self>) -> Result<Vec<ResticContainerSnapshot>> {
        let mut command = self.new_command();
        command.args(&["snapshots", "--json"]);
        let output = command.output().await?;
        Self::parse_snapshots(&output.stdout, self.model().id())
    }

    pub fn model(&self) -> &ResticContainerEntity {
        &self.model
    }

    fn new_command(&self) -> Command {
        let mut command = Command::new("restic");
        // let repository = match &self.model.repository {
        //     crate::model::entities::ResticRepository::Custom(r) => r,
        // };
        // ^ future
        let crate::model::entities::ResticRepository::Custom(repository) = &self.model.repository;
        command.env("RESTIC_REPOSITORY", repository);
        command.envs(&self.model.custom_environment);
        command
    }

    fn parse_snapshots(output: &[u8], expected_container_id: Uuid) -> Result<Vec<ResticContainerSnapshot>> {
        const UUID_TAG: &str = "uuid=";
        const TS_TAG: &str = "ts=";

        serde_json::from_slice::<Vec<SnapshotsOutputRecord>>(output)
            .context("unable to parse restic snapshot output")
            .map(|v| {
                v.into_iter()
                    .filter_map(|r| {
                        let path = r.paths.get(0);

                        let container_id = path
                            .and_then(|p| p.parent().and_then(|p| p.file_name()))
                            .and_then(|f| f.to_str())
                            .and_then(|s| s.parse::<Uuid>().ok());

                        if container_id.unwrap_or_default() != expected_container_id {
                            return None;
                        }

                        let dataset_id = path
                            .and_then(|p| p.file_name())
                            .and_then(|f| f.to_str())
                            .and_then(|s| s.parse().ok());

                        let uuid = r
                            .tags
                            .iter()
                            .find(|t| t.starts_with(UUID_TAG))
                            .and_then(|t| t[UUID_TAG.len()..].parse().ok());
                        let ts = r
                            .tags
                            .iter()
                            .find(|t| t.starts_with(TS_TAG))
                            .and_then(|t| parse_snapshot_label(&t[TS_TAG.len()..]).ok());

                        dataset_id
                            .zip(uuid)
                            .zip(ts)
                            .map(|((dataset_id, received_uuid), datetime)| ResticContainerSnapshot {
                                uuid: r.id,
                                datetime,
                                dataset_id,
                                received_uuid,
                            })
                    })
                    .collect()
            })
    }
}

pub struct ResticBackup {
    command: Command,
    source: SnapshotSource,
}

struct SnapshotSource {
    dataset_id: Uuid,
    snapshot: SnapshotHandle,
    bind_path: PathBuf,
}

impl ResticBackup {
    fn new(mut repo_command: Command, bind_path: PathBuf, dataset_id: Uuid, snapshot: SnapshotHandle) -> Self {
        repo_command.args(&[
            "backup",
            "--json",
            "--tag",
            format!("uuid={},ts={}", snapshot.uuid, snapshot.datetime.format("%FT%H-%M-%SZ")).as_str(),
        ]);

        ResticBackup {
            command: repo_command,
            source: SnapshotSource {
                dataset_id,
                snapshot,
                bind_path,
            },
        }
    }

    pub fn start(mut self, path: PathBuf) -> Result<StartedResticBackup> {
        fs::create_dir_all(&self.source.bind_path)?;
        bind_mount(&path, &self.source.bind_path)?;

        // spawn as restic user?
        self.command.arg(&self.source.bind_path);
        self.command.stdout(Stdio::piped());
        self.command
            .spawn()
            .map_err(|e| {
                let _ = unmount(&path);
                anyhow!(e)
            })
            .map(|mut process| {
                let message_reader = Self::spawn_message_reader(process.stdout.take().unwrap());
                StartedResticBackup {
                    process,
                    message_reader,
                    source: self.source,
                }
            })
    }

    fn spawn_message_reader(handle: ChildStdout) -> JoinHandle<Result<Option<ResticId>>> {
        tokio::spawn(async move {
            const SENTINEL: &str = "\"summary\"";
            let mut reader = BufReader::new(handle);
            let mut buffer = String::new();
            let mut result = None;
            while reader.read_line(&mut buffer).await? > 0 {
                if result.is_none() && buffer.contains(SENTINEL) {
                    result = Self::try_parse_snapshot_id(&buffer);
                }
                buffer.clear();
            }
            Ok(result)
        })
    }

    fn try_parse_snapshot_id(line: &str) -> Option<ResticId> {
        serde_json::from_str::<BackupOutputSummaryMessage>(&line)
            .ok()
            .filter(|m| m.message_type == "summary")
            .map(|m| m.snapshot_id)
    }
}

pub struct StartedResticBackup {
    process: Child,
    message_reader: JoinHandle<Result<Option<ResticId>>>,
    source: SnapshotSource,
}

impl StartedResticBackup {
    pub async fn wait(self) -> Result<ResticContainerSnapshot> {
        let exit_code = self.process.await.unwrap();
        let _ = unmount(&self.source.bind_path);
        if !exit_code.success() {
            bail!("FIXME failed yo, {}.", exit_code);
        }
        let message_result = self.message_reader.await.unwrap().unwrap();
        let new_snapshot_id = message_result.context("failed to find new snapshot id")?;

        Ok(ResticContainerSnapshot {
            datetime: self.source.snapshot.datetime,
            dataset_id: self.source.dataset_id,
            uuid: new_snapshot_id,
            received_uuid: self.source.snapshot.uuid,
        })
    }
}

#[derive(Deserialize)]
struct SnapshotsOutputRecord {
    tags: Vec<String>,
    paths: Vec<PathBuf>,
    id: ResticId,
    parent: Option<ResticId>,
}

#[derive(Deserialize)]
struct BackupOutputSummaryMessage {
    message_type: String,
    snapshot_id: ResticId,
}

#[cfg(test)]
mod tests {
    use super::*;

    //mock!(Command);

    #[test]
    fn restic_snapshots_parse() {
        const RESTIC_OUTPUT: &[u8] = br#"[{"time":"2020-11-13T21:00:49.956949773Z","parent":"6385c9977fe79a332f9c762cae1684f48efb1c8c905492b17213d70160079c76","tree":"f9048e608cdd4505abb0aa2b0a15b52167760a60d9e2794b498ad79e1910d0f5","paths":["/var/lib/blkcapt/restic/cf125c66-c94c-94f9-7097-3045d4a6c1ce/8a7ae0b5-b28c-b240-8c07-0015431d58d8"],"hostname":"blkcaptdev","username":"root","id":"cb1fa1cf88b627343a8083edb8985971ac766ccaba79d763b176b11b269cdb9b","short_id":"cb1fa1cf"},{"time":"2020-11-13T21:01:32.541412696Z","parent":"cb1fa1cf88b627343a8083edb8985971ac766ccaba79d763b176b11b269cdb9b","tree":"ccde6f9a0a5ffb971ed5ad1a56e93436342bda1be7af9b00c4228ae0a20d68d6","paths":["/var/lib/blkcapt/restic/cf125c66-c94c-94f9-7097-3045d4a6c1ce/8a7ae0b5-b28c-b240-8c07-0015431d58d8"],"hostname":"blkcaptdev","username":"root","id":"264d86e4d60777fe66596e620f8b499551117234e7478f804c33e6fbb990cd8a","short_id":"264d86e4"},{"time":"2020-11-13T21:03:10.942354588Z","parent":"264d86e4d60777fe66596e620f8b499551117234e7478f804c33e6fbb990cd8a","tree":"ccde6f9a0a5ffb971ed5ad1a56e93436342bda1be7af9b00c4228ae0a20d68d6","paths":["/var/lib/blkcapt/restic/cf125c66-c94c-94f9-7097-3045d4a6c1ce/8a7ae0b5-b28c-b240-8c07-0015431d58d8"],"hostname":"blkcaptdev","username":"root","id":"12c455f61bf583d9700014cf35f34270d39e73e2d02c7a702114de269b03aba5","short_id":"12c455f6"}]"#;
        let actual = ResticRepository::parse_snapshots(RESTIC_OUTPUT).unwrap();
        let expected_dataset_id = "8a7ae0b5-b28c-b240-8c07-0015431d58d8".parse().unwrap();
        let expected = vec![
            ResticContainerSnapshot {
                uuid: "cb1fa1cf88b627343a8083edb8985971ac766ccaba79d763b176b11b269cdb9b"
                    .parse()
                    .unwrap(),
                dataset_id: expected_dataset_id,
                datetime: "2020-11-13T21:00:49.956949773Z".parse().unwrap(),
                received_uuid: "".parse().unwrap(),
            },
            ResticContainerSnapshot {
                uuid: "264d86e4d60777fe66596e620f8b499551117234e7478f804c33e6fbb990cd8a"
                    .parse()
                    .unwrap(),
                dataset_id: expected_dataset_id,
                datetime: "2020-11-13T21:01:32.541412696Z".parse().unwrap(),
                received_uuid: "".parse().unwrap(),
            },
            ResticContainerSnapshot {
                uuid: "12c455f61bf583d9700014cf35f34270d39e73e2d02c7a702114de269b03aba5"
                    .parse()
                    .unwrap(),
                dataset_id: expected_dataset_id,
                datetime: "2020-11-13T21:03:10.942354588Z".parse().unwrap(),
                received_uuid: "".parse().unwrap(),
            },
        ];
        assert_eq!(actual, expected);
    }

    #[test]
    fn restic_backup_message_parses() {
        const RESTIC_OUTPUT: &str = r#"{"message_type":"summary","files_new":0,"files_changed":0,"files_unmodified":2,"dirs_new":0,"dirs_changed":0,"dirs_unmodified":4,"data_blobs":0,"tree_blobs":0,"data_added":0,"total_files_processed":2,"total_bytes_processed":8,"total_duration":0.227000569,"snapshot_id":"e4d43442776db0656bff8f674a94285f58ea3c4d5b1e0db9d501138d84d3817d","snapshot_short_id":"e4d43442"}"#;
        let actual = ResticBackup::try_parse_snapshot_id(RESTIC_OUTPUT).unwrap();
        let expected: ResticId = "e4d43442776db0656bff8f674a94285f58ea3c4d5b1e0db9d501138d84d3817d"
            .parse()
            .unwrap();
        assert_eq!(actual, expected);
    }
}
