use super::{
    dataset::{DatasetHolderActor, HolderReadyMessage},
    transfer::TransferComplete,
};
use crate::actorbase::{log_result, TerminalState};
use crate::{
    actorbase::{schedule_next_message, unhandled_result},
    snapshots::{ContainerSnapshotsResponse, GetContainerSnapshotsMessage, PruneMessage},
    tasks::WorkerCompleteMessage,
    tasks::WorkerTask,
    xactorext::{BcActor, BcActorCtrl, BcHandler},
};
use anyhow::{anyhow, bail, Result};
use container::BackupReadyMessage;
pub use container::{GetBackupMessage, ResticContainerActor};
use cron::Schedule;
use derive_more::From;
use libblkcapt::model::entities::FeatureState;
use libblkcapt::{
    core::restic::ResticContainerSnapshot,
    core::restic::{ResticBackup, ResticRepository},
    core::SnapshotHandle,
    model::entities::ResticContainerEntity,
    model::Entity,
};
use slog::{o, trace, Logger};
use std::convert::TryInto;
use std::{collections::HashMap, hash::Hash, mem, panic, path::PathBuf, sync::Arc};
use transfer::ParentTransferComplete;
pub use transfer::ResticTransferActor;
use uuid::Uuid;
use xactor::{message, Addr, Context, Sender};

mod container {
    use slog::info;

    use super::*;

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
    pub struct BackupReadyMessage(pub Result<ResticBackup>);

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
        async fn started(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>) -> Result<()> {
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

            if self.repository.get().model().pruning_state() == FeatureState::Enabled {
                self.prune_schedule = self
                    .repository
                    .get()
                    .model()
                    .snapshot_retention
                    .as_ref()
                    .map(|r| &r.evaluation_schedule)
                    .map_or(Ok(None), |s| s.try_into().map(Some))?;

                self.schedule_next_prune(log, ctx);
            }

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
        async fn handle(
            &mut self,
            _log: &Logger,
            _ctx: &mut Context<BcActor<Self>>,
            msg: GetBackupMessage,
        ) -> Result<()> {
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

    // TODO
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
        async fn handle(&mut self, log: &Logger, _ctx: &mut Context<BcActor<Self>>, msg: ParentTransferComplete) {
            if let Some(transfer) = &self.active_transfer {
                if let Some(snapshot) = msg.1 {
                    info!(log, "snapshot received"; "dataset_id" => %transfer.dataset_id, "time" => %snapshot.datetime);
                    self.snapshots.entry(transfer.dataset_id).or_default().push(snapshot);
                }
                //TODO deal with failed transfer
                self.active_transfer = None;
            }
        }
    }
}

mod transfer {
    use super::*;

    pub struct ResticTransferActor {
        parent: Addr<BcActor<ResticContainerActor>>,
        requestor: Sender<TransferComplete>,
        state: State,
    }

    enum State {
        WaitingForHoldAndBackup(Option<HolderState>, Option<ResticBackup>),
        Transferring(Addr<BcActor<DatasetHolderActor>>, WorkerTask),
        Transferred(Result<ResticContainerSnapshot>),
        Faulted,
        Stopped(TerminalState),
    }

    impl State {
        fn take(&mut self) -> Self {
            mem::replace(self, State::Faulted)
        }
    }

    struct HolderState {
        holder: Addr<BcActor<DatasetHolderActor>>,
        snapshot_path: PathBuf,
    }

    impl HolderState {
        fn new(holder: Addr<BcActor<DatasetHolderActor>>, snapshot_path: PathBuf) -> Self {
            Self { holder, snapshot_path }
        }
    }

    type BackupWorkerCompleteMessage = WorkerCompleteMessage<Result<ResticContainerSnapshot>>;

    #[async_trait::async_trait]
    impl BcActorCtrl for ResticTransferActor {
        async fn started(&mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>) -> Result<()> {
            Ok(())
        }

        async fn stopped(&mut self, log: &Logger, _ctx: &mut Context<BcActor<Self>>) {
            let (state, result) = match self.state.take() {
                State::Transferring(_holder, worker_task) => {
                    worker_task.abort();
                    (TerminalState::Cancelled, Err(anyhow!("cancelled during transfer")))
                }
                State::WaitingForHoldAndBackup(..) => {
                    (TerminalState::Cancelled, Err(anyhow!("cancelled prior to transfer")))
                }
                State::Transferred(result) => (result.as_ref().into(), result),
                State::Faulted => (TerminalState::Faulted, Err(anyhow!("actor faulted"))),
                State::Stopped(_) => panic!(),
            };
            self.state = State::Stopped(state);
            log_result(log, &result);

            let container_notify_result = self.parent.call(ParentTransferComplete(state, result.ok())).await;
            unhandled_result(log, container_notify_result);
            let parent_notify_result = self.requestor.send(TransferComplete(state));
            unhandled_result(log, parent_notify_result);
        }
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
                    requestor: parent,
                    parent: container,
                },
                log,
            )
        }

        fn maybe_start_transfer(incoming: State, ctx: &mut Context<BcActor<Self>>, log: &Logger) -> State {
            if let State::WaitingForHoldAndBackup(Some(holder_state), Some(backup)) = incoming {
                let started_backup = backup.start(holder_state.snapshot_path);
                match started_backup {
                    Ok(started) => {
                        let task = WorkerTask::run(ctx.address(), log, |_| async move { started.wait().await.into() });
                        State::Transferring(holder_state.holder, task)
                    }
                    Err(e) => {
                        ctx.stop(None);
                        State::Transferred(Err(e))
                    }
                }
            } else {
                incoming
            }
        }

        fn input_ready(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, input: InputReady) {
            self.state = match (self.state.take(), input) {
                (State::WaitingForHoldAndBackup(maybe_holder, None), InputReady::Backup(Ok(backup))) => {
                    let updated_state = State::WaitingForHoldAndBackup(maybe_holder, Some(backup));
                    Self::maybe_start_transfer(updated_state, ctx, log)
                }
                (State::WaitingForHoldAndBackup(None, maybe_backup), InputReady::Holder(Ok(holder_state))) => {
                    let updated_state = State::WaitingForHoldAndBackup(Some(holder_state), maybe_backup);
                    Self::maybe_start_transfer(updated_state, ctx, log)
                }
                (State::WaitingForHoldAndBackup(_, None), InputReady::Backup(Err(e)))
                | (State::WaitingForHoldAndBackup(None, _), InputReady::Holder(Err(e))) => {
                    ctx.stop(None);
                    State::Transferred(Err(e))
                }
                _ => {
                    ctx.stop(None);
                    State::Faulted
                }
            };
        }
    }

    #[derive(From)]
    enum InputReady {
        Holder(Result<HolderState>),
        Backup(Result<ResticBackup>),
    }

    #[message()]
    pub struct ParentTransferComplete(pub TerminalState, pub Option<ResticContainerSnapshot>);

    #[async_trait::async_trait]
    impl BcHandler<HolderReadyMessage> for ResticTransferActor {
        async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: HolderReadyMessage) {
            let HolderReadyMessage {
                holder, snapshot_path, ..
            } = msg;
            self.input_ready(log, ctx, holder.map(|h| HolderState::new(h, snapshot_path)).into());
        }
    }

    #[async_trait::async_trait]
    impl BcHandler<BackupReadyMessage> for ResticTransferActor {
        async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: BackupReadyMessage) {
            self.input_ready(log, ctx, msg.0.into());
        }
    }

    #[async_trait::async_trait]
    impl BcHandler<BackupWorkerCompleteMessage> for ResticTransferActor {
        async fn handle(&mut self, _log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: BackupWorkerCompleteMessage) {
            if let State::Transferring(..) = self.state.take() {
                self.state = State::Transferred(msg.0);
            } else {
                self.state = State::Faulted;
            }
            ctx.stop(None);
        }
    }
}

mod prune {
    use super::*;

    struct ResticPruneActor {}

    impl ResticPruneActor {}

    #[async_trait::async_trait]
    impl BcActorCtrl for ResticPruneActor {}
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
