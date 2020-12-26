use super::{parse_snapshot_label, Snapshot, SnapshotHandle};
use crate::{
    model::{entities::ResticContainerEntity, Entity, EntityId},
    sys::{
        fs::{bind_mount, unmount},
        process::exit_status_as_result,
    },
};
use anyhow::{anyhow, bail, Context, Error, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer};
use std::{borrow::Borrow, fmt::Display, fs, path::Path, path::PathBuf, process::Stdio, str::FromStr, sync::Arc};
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
    pub dataset_id: EntityId,
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

impl<B: Borrow<ResticContainerSnapshot> + Display> Snapshot for B {
    fn datetime(&self) -> DateTime<Utc> {
        self.borrow().datetime
    }
}

impl Display for ResticContainerSnapshot {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
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

    pub fn backup(self: &Arc<Self>, bind_at: PathBuf, dataset_id: EntityId, snapshot: SnapshotHandle) -> ResticBackup {
        let command = self.new_command();
        ResticBackup::new(command, bind_at, dataset_id, snapshot)
    }

    pub fn prune(self: &Arc<Self>) -> ResticPrune {
        let command = self.new_command();
        ResticPrune::new(command)
    }

    pub async fn snapshots(self: &Arc<Self>) -> Result<Vec<ResticContainerSnapshot>> {
        let mut command = self.new_command();
        command.args(&["snapshots", "--json"]);
        let output = command.output().await?;
        Self::parse_snapshots(&output.stdout, self.model().id())
    }

    pub async fn snapshot_by_datetime(
        self: &Arc<Self>, bind_path: &Path, datetime: DateTime<Utc>,
    ) -> Result<Option<ResticContainerSnapshot>> {
        let mut command = self.new_command();
        let datetime_tag = ResticBackup::datetime_tag(datetime);
        command.args(&["snapshots", "--json", "--tag", &datetime_tag, "--path"]);
        command.arg(&bind_path);
        let output = command.output().await?;
        Self::parse_snapshots(&output.stdout, self.model().id()).map(|mut r| r.pop())
    }

    pub fn forget(self: &Arc<Self>, snapshots: &[&ResticContainerSnapshot]) -> ResticForget {
        let command = self.new_command();
        ResticForget::new(command, snapshots)
    }

    pub fn model(&self) -> &ResticContainerEntity {
        &self.model
    }

    fn new_command(&self) -> Command {
        let mut command = Command::new("restic");
        // let repository = match &self.model.repository {
        //     crate::model::entities::ResticRepository::Custom(r) => r,
        // };
        // ^ future with more linkages
        let crate::model::entities::ResticRepository::Custom(repository) = &self.model.repository;
        command.env("RESTIC_REPOSITORY", repository);
        command.envs(&self.model.custom_environment);
        command
    }

    fn parse_snapshots(output: &[u8], expected_container_id: EntityId) -> Result<Vec<ResticContainerSnapshot>> {
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
                            .and_then(|s| s.parse::<EntityId>().ok());

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
    dataset_id: EntityId,
    snapshot: SnapshotHandle,
    bind_path: PathBuf,
}

impl ResticBackup {
    fn new(mut repo_command: Command, bind_path: PathBuf, dataset_id: EntityId, snapshot: SnapshotHandle) -> Self {
        repo_command.args(&["backup", "--json", "--tag", Self::snapshot_tags(&snapshot).as_str()]);

        ResticBackup {
            command: repo_command,
            source: SnapshotSource {
                dataset_id,
                snapshot,
                bind_path,
            },
        }
    }

    pub fn start(mut self, path: &Path) -> Result<StartedResticBackup> {
        fs::create_dir_all(&self.source.bind_path)?;
        bind_mount(path, &self.source.bind_path)?;

        // spawn as restic user?
        self.command.arg(&self.source.bind_path);
        self.command.stdout(Stdio::piped());
        self.command
            .spawn()
            .map_err(|e| {
                let _ = unmount(path);
                anyhow!(e)
            })
            .map(|mut process| {
                let message_reader = Self::spawn_message_reader(process.stdout.take().expect("only taken once"));
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

    pub fn datetime_tag(datetime: DateTime<Utc>) -> String {
        format!("ts={}", datetime.format("%FT%H-%M-%SZ"))
    }

    pub fn uuid_tag(uuid: Uuid) -> String {
        format!("uuid={}", uuid)
    }

    fn snapshot_tags(snapshot: &SnapshotHandle) -> String {
        format!(
            "{},{}",
            Self::uuid_tag(snapshot.uuid),
            Self::datetime_tag(snapshot.datetime)
        )
    }
}

pub struct StartedResticBackup {
    process: Child,
    message_reader: JoinHandle<Result<Option<ResticId>>>,
    source: SnapshotSource,
}

impl StartedResticBackup {
    pub async fn wait(self) -> Result<ResticContainerSnapshot> {
        let exit_status = self.process.await?;
        let _ = unmount(&self.source.bind_path);
        exit_status_as_result(exit_status)?;

        let message_result = self.message_reader.await.expect("task doesn't panic")?;
        let new_snapshot_id = message_result.context("failed to find new snapshot id")?;

        Ok(ResticContainerSnapshot {
            datetime: self.source.snapshot.datetime,
            dataset_id: self.source.dataset_id,
            uuid: new_snapshot_id,
            received_uuid: self.source.snapshot.uuid,
        })
    }
}

pub struct ResticPrune {
    command: Command,
}

impl ResticPrune {
    fn new(mut repo_command: Command) -> Self {
        repo_command.args(&["prune"]);

        ResticPrune { command: repo_command }
    }

    pub fn start(mut self) -> Result<StartedResticPrune> {
        let process = self.command.spawn().context("spawn restic prune process failed")?;

        Ok(StartedResticPrune { process })
    }
}

pub struct StartedResticPrune {
    process: Child,
}

impl StartedResticPrune {
    pub async fn wait(self) -> Result<()> {
        exit_status_as_result(self.process.await?)
    }
}

pub struct ResticForget {
    command: Command,
}

impl ResticForget {
    fn new(mut repo_command: Command, snapshots: &[&ResticContainerSnapshot]) -> Self {
        repo_command.args(&["forget"]);
        repo_command.args(snapshots.iter().map(|s| s.uuid.to_string()));

        Self { command: repo_command }
    }

    pub fn start(mut self) -> Result<StartedResticPrune> {
        let process = self.command.spawn().context("spawn restic forget process failed")?;

        Ok(StartedResticPrune { process })
    }
}

pub struct StartedResticForget {
    process: Child,
}

impl StartedResticForget {
    pub async fn wait(self) -> Result<()> {
        exit_status_as_result(self.process.await?)
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
        const RESTIC_OUTPUT: &[u8] = br#"[{"time":"2020-11-30T04:26:00.737443538Z","parent":"c7c4f0ed86a6a6ab812b41999a8fde92463cacb1673762541d1b5a139e5e0d19","tree":"fa98182915064b51e79bb95d20371696cbbde2d098fd0855521f79175d9e2dab","paths":["/var/lib/blkcapt/restic/e1370910-8805-4b72-b1aa-b007b6acc9cc/b99a584c-72c0-4cbe-9c6d-0c32274563f7"],"hostname":"blkcaptdev","username":"root","tags":["uuid=7f56a00a-2139-4048-96e2-c4946b731914","ts=2020-11-29T21-26-00Z"],"id":"4b0bdb80f692407f90413167a2f8673c2b948ad466e48d10a6072afc69ec7add","short_id":"4b0bdb80"},{"time":"2020-12-01T04:12:06.301970176Z","parent":"8067bdf9d334fcc550ddd9cca4afc382d97c10a583b1c37135508c2377e42ddb","tree":"b6b5f9002e282bb9ab0be82666bb1d6a038c0d71eb7dfda8dd1ee16870b5daa6","paths":["/var/lib/blkcapt/restic/e1370910-8805-4b72-b1aa-b007b6acc9cc/b99a584c-72c0-4cbe-9c6d-0c32274563f7"],"hostname":"blkcaptdev","username":"root","tags":["uuid=57c929a8-61ad-6747-957d-5daa101de0ff","ts=2020-11-30T04-58-00Z"],"id":"40e670db06225d0945b3ab4c0023f823d30f0ba15984df02266b74de29a1b657","short_id":"40e670db"}]"#;
        let actual =
            ResticRepository::parse_snapshots(RESTIC_OUTPUT, "e1370910-8805-4b72-b1aa-b007b6acc9cc".parse().unwrap())
                .unwrap();
        let expected_dataset_id = "b99a584c-72c0-4cbe-9c6d-0c32274563f7".parse().unwrap();
        let expected = vec![
            ResticContainerSnapshot {
                uuid: "4b0bdb80f692407f90413167a2f8673c2b948ad466e48d10a6072afc69ec7add"
                    .parse()
                    .unwrap(),
                dataset_id: expected_dataset_id,
                datetime: "2020-11-29T21:26:00Z".parse().unwrap(),
                received_uuid: "7f56a00a-2139-4048-96e2-c4946b731914".parse().unwrap(),
            },
            ResticContainerSnapshot {
                uuid: "40e670db06225d0945b3ab4c0023f823d30f0ba15984df02266b74de29a1b657"
                    .parse()
                    .unwrap(),
                dataset_id: expected_dataset_id,
                datetime: "2020-11-30T04:58:00Z".parse().unwrap(),
                received_uuid: "57c929a8-61ad-6747-957d-5daa101de0ff".parse().unwrap(),
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
