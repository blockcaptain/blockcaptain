use super::{
    dataset::{DatasetHolderActor, HolderReadyMessage},
    transfer::TransferComplete,
};
use crate::{
    actorbase::unhandled_error,
    actorbase::{schedule_next_message, unhandled_result},
    snapshots::{ContainerSnapshotsResponse, GetContainerSnapshotsMessage, PruneMessage},
    xactorext::{BcActor, BcActorCtrl, BcHandler},
};
use anyhow::{bail, Result};
use cron::Schedule;
use libblkcapt::{
    core::restic::ResticContainerSnapshot,
    core::restic::{ResticBackup, ResticRepository},
    core::SnapshotHandle,
    model::entities::ResticContainerEntity,
    model::Entity,
};
use slog::{o, trace, Logger};
use std::{collections::HashMap, hash::Hash, mem, path::PathBuf, sync::Arc};
use uuid::Uuid;
use xactor::{message, Addr, Context, Sender};

pub struct ResticContainerActor {
    container_id: Uuid,
    repository: RepositoryState,
    snapshots: HashMap<Uuid, Vec<ResticContainerSnapshot>>,
    prune_schedule: Option<Schedule>,
    active_transfer: Option<ActiveTransfer>,
}

enum RepositoryState {
    Started(Arc<ResticRepository>),
    Pending(ResticContainerEntity),
}

impl RepositoryState {
    fn get(&self) -> &Arc<ResticRepository> {
        match self {
            RepositoryState::Started(repository) => repository,
            RepositoryState::Pending(_) => panic!("message received before start"),
        }
    }
}

pub struct ActiveTransfer {
    actor: Addr<BcActor<ResticTransferActor>>,
    dataset_id: Uuid,
}

#[message(result = "Result<()>")]
pub struct GetBackupMessage {
    source_dataset_id: Uuid,
    source_snapshot_handle: SnapshotHandle,
    target: Addr<BcActor<ResticTransferActor>>,
}

#[message]
struct BackupReadyMessage(Result<ResticBackup>);

impl GetBackupMessage {
    pub fn new(
        requestor_addr: &Addr<BcActor<ResticTransferActor>>,
        source_dataset_id: Uuid,
        source_snapshot_handle: SnapshotHandle,
    ) -> Self {
        Self {
            source_dataset_id,
            source_snapshot_handle,
            target: requestor_addr.clone(),
        }
    }
}

impl ResticContainerActor {
    pub fn new(model: ResticContainerEntity, log: &Logger) -> BcActor<Self> {
        let id = model.id();
        BcActor::new(
            Self {
                container_id: id,
                repository: RepositoryState::Pending(model),
                snapshots: Default::default(),
                prune_schedule: None,
                active_transfer: None,
            },
            &log.new(o!("container_id" => id.to_string())),
        )
    }

    fn schedule_next_prune(&self, log: &Logger, ctx: &mut Context<BcActor<Self>>) {
        schedule_next_message(self.prune_schedule.as_ref(), "prune", PruneMessage(), log, ctx);
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for ResticContainerActor {
    async fn started(&mut self, log: &Logger, _ctx: &mut Context<BcActor<Self>>) -> Result<()> {
        if let RepositoryState::Pending(model) = &self.repository {
            let repository = ResticRepository::validate(model.clone()).map(Arc::new)?;
            self.snapshots = group_by(repository.snapshots().await?, |s| &s.dataset_id);
            trace!(
                log,
                "Starting container with {} snapshots from {} datasets.",
                self.snapshots.values().fold(0, |acc, v| acc + v.len()),
                self.snapshots.len()
            );
            self.repository = RepositoryState::Started(repository);
        } else {
            bail!("Pool already started.");
        }

        // if self.container.model().pruning_state() == FeatureState::Enabled {
        //     self.prune_schedule = self
        //         .container
        //         .model()
        //         .snapshot_retention
        //         .as_ref()
        //         .map(|r| &r.evaluation_schedule)
        //         .map_or(Ok(None), |s| s.try_into().map(Some))?;

        //     self.schedule_next_prune(log, ctx);
        // }

        Ok(())
    }

    async fn stopped(&mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>) {}
}

#[async_trait::async_trait]
impl BcHandler<GetContainerSnapshotsMessage> for ResticContainerActor {
    async fn handle(
        &mut self,
        _log: &Logger,
        _ctx: &mut Context<BcActor<Self>>,
        msg: GetContainerSnapshotsMessage,
    ) -> ContainerSnapshotsResponse {
        ContainerSnapshotsResponse {
            snapshots: self
                .snapshots
                .get(&msg.source_dataset_id)
                .map(|v| v.iter().map(|s| s.into()).collect())
                .unwrap_or_default(),
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<GetBackupMessage> for ResticContainerActor {
    async fn handle(&mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>, msg: GetBackupMessage) -> Result<()> {
        // TODO: restic method to look for 1 snapshot
        // if self
        //     .container
        //     .corresponding_snapshot(msg.source_dataset_id, &msg.source_snapshot_handle)
        //     .is_ok()
        // {
        //     anyhow::bail!(
        //         "receiver requested for existing snapshot dataset_id: {} snapshot_datetime: {}",
        //         msg.source_dataset_id,
        //         msg.source_snapshot_handle.datetime
        //     )
        // }
        let repository = &self.repository.get();

        const BASE_PATH: &str = "/var/lib/blkcapt/restic";
        let mut bind_path = PathBuf::from(BASE_PATH);
        bind_path.push(self.container_id.to_string());
        bind_path.push(msg.source_dataset_id.to_string());
        let snapshot_backup = repository.backup(bind_path, msg.source_dataset_id, msg.source_snapshot_handle);
        self.active_transfer = Some(ActiveTransfer {
            actor: msg.target.clone(),
            dataset_id: msg.source_dataset_id,
        });
        msg.target.send(BackupReadyMessage(Ok(snapshot_backup)))
    }
}

// #[async_trait::async_trait]
// impl BcHandler<ReceiverFinishedParentMessage> for ResticContainerActor {
//     async fn handle(&mut self, log: &Logger, _ctx: &mut Context<BcActor<Self>>, msg: ReceiverFinishedParentMessage) {
//         let ReceiverFinishedParentMessage(actor_id, new_snapshot) = msg;
//         let active_receiver = self.active_receivers.remove(&actor_id).expect("FIXME");

//         let new_snapshot = new_snapshot.unwrap();
//         debug!(log, "container received snapshot {}", new_snapshot.datetime(); "received_uuid" => %new_snapshot.received_uuid());

//         self.snapshots
//             .get_mut(&active_receiver.dataset_id)
//             .expect("FIXME")
//             .push(new_snapshot);
//     }
// }

#[async_trait::async_trait]
impl BcHandler<PruneMessage> for ResticContainerActor {
    async fn handle(&mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>, _msg: PruneMessage) {
        // let rules = self
        //     .container
        //     .model()
        //     .snapshot_retention
        //     .as_ref()
        //     .expect("retention exist based on message scheduling in started");

        // let result = observable_func(self.container.model().id(), ObservableEvent::ContainerPrune, || {
        //     let result = self.container.source_dataset_ids().and_then(|ids| {
        //         ids.iter()
        //             .map(|id| {
        //                 trace!(log, "prune container"; "dataset_id" => %id);
        //                 self.container
        //                     .snapshots(*id)
        //                     .and_then(|snapshots| evaluate_retention(snapshots, rules))
        //                     .and_then(|eval| prune_snapshots(eval, &log))
        //             })
        //             .collect::<Result<()>>()
        //     });
        //     ready(result)
        // })
        // .await;

        // unhandled_result(log, result);
    }
}

#[async_trait::async_trait]
impl BcHandler<ParentTransferComplete> for ResticContainerActor {
    async fn handle(&mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>, msg: ParentTransferComplete) {
        if let Some(transfer) = &self.active_transfer {
            if let Ok(snapshot) = msg.0 {
                self.snapshots.entry(transfer.dataset_id).or_default().push(snapshot);
            }
            self.active_transfer = None;
        }
    }
}

pub struct ResticTransferActor {
    state: State,
    parent: Sender<TransferComplete>,
    container: Addr<BcActor<ResticContainerActor>>,
}

enum State {
    WaitingForHoldAndBackup(Option<HolderState>, Option<ResticBackup>),
    Transferring(Addr<BcActor<DatasetHolderActor>>),
    Finished,
    Faulted,
}

struct HolderState {
    holder: Addr<BcActor<DatasetHolderActor>>,
    snapshot_path: PathBuf,
}

#[async_trait::async_trait]
impl BcActorCtrl for ResticTransferActor {
    async fn started(&mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>) -> Result<()> {
        Ok(())
    }

    async fn stopped(&mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>) {}
}

impl ResticTransferActor {
    pub fn new(
        parent: Sender<TransferComplete>,
        container: Addr<BcActor<ResticContainerActor>>,
        log: &Logger,
    ) -> BcActor<Self> {
        BcActor::new(
            Self {
                state: State::WaitingForHoldAndBackup(None, None),
                parent,
                container,
            },
            log,
        )
    }

    fn maybe_start_transfer(&mut self, ctx: &mut Context<BcActor<Self>>, log: &Logger) {
        match mem::replace(&mut self.state, State::Faulted) {
            State::WaitingForHoldAndBackup(Some(holder_state), Some(backup)) => {
                let started_backup = backup.start(holder_state.snapshot_path);
                match started_backup {
                    Ok(started) => {
                        let self_addr = ctx.address();
                        tokio::spawn(async move {
                            let result = started.wait().await;
                            let _ = self_addr.send(InternalTransferComplete(result));
                        });
                        self.state = State::Transferring(holder_state.holder)
                    }
                    Err(e) => unhandled_error(log, e),
                }
            }
            other => self.state = other,
        }
    }
}

#[message()]
struct InternalTransferComplete(Result<ResticContainerSnapshot>);

#[message()]
struct ParentTransferComplete(Result<ResticContainerSnapshot>);

#[async_trait::async_trait]
impl BcHandler<HolderReadyMessage> for ResticTransferActor {
    async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: HolderReadyMessage) {
        if let State::WaitingForHoldAndBackup(None, maybe_backup) = mem::replace(&mut self.state, State::Faulted) {
            match msg.holder {
                Ok(holder) => {
                    self.state = State::WaitingForHoldAndBackup(
                        Some(HolderState {
                            holder,
                            snapshot_path: msg.snapshot_path,
                        }),
                        maybe_backup,
                    )
                }
                Err(e) => {
                    unhandled_error(log, e);
                }
            }
        }
        self.maybe_start_transfer(ctx, log);
    }
}

#[async_trait::async_trait]
impl BcHandler<BackupReadyMessage> for ResticTransferActor {
    async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: BackupReadyMessage) {
        if let State::WaitingForHoldAndBackup(maybe_holder, None) = mem::replace(&mut self.state, State::Faulted) {
            match msg.0 {
                Ok(backup) => self.state = State::WaitingForHoldAndBackup(maybe_holder, Some(backup)),
                Err(e) => {
                    unhandled_error(log, e);
                }
            }
        }
        self.maybe_start_transfer(ctx, log);
    }
}

#[async_trait::async_trait]
impl BcHandler<InternalTransferComplete> for ResticTransferActor {
    async fn handle(&mut self, log: &Logger, _ctx: &mut Context<BcActor<Self>>, msg: InternalTransferComplete) {
        if let State::Transferring(_) = mem::replace(&mut self.state, State::Faulted) {
            let container_notify_result = self.container.call(ParentTransferComplete(msg.0)).await;
            unhandled_result(log, container_notify_result);
            let parent_notify_result = self.parent.send(TransferComplete(Ok(())));
            unhandled_result(log, parent_notify_result);
            self.state = State::Finished;
        }
    }
}

pub fn group_by<I, F, K, T>(xs: I, mut key_fn: F) -> HashMap<K, Vec<T>>
where
    I: IntoIterator<Item = T>,
    F: FnMut(&T) -> &K,
    K: Hash + Eq + Clone,
{
    let mut groups = HashMap::<K, Vec<T>>::new();
    for item in xs {
        let key = key_fn(&item);
        if let Some(group) = groups.get_mut(key) {
            group.push(item);
        } else {
            groups.insert(key.clone(), vec![item]);
        }
    }
    groups
}
