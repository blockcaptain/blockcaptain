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
    xactorext::{BcActor, BcActorCtrl, BcAddr, BcHandler},
};
use anyhow::{anyhow, bail, Context as AnyhowContext, Result};
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
use prune::{PruneCompleteMessage, ResticPruneActor};
use slog::{o, trace, Logger};
use std::convert::TryInto;
use std::{collections::HashMap, hash::Hash, mem, panic, path::PathBuf, sync::Arc};
use transfer::ParentTransferComplete;
pub use transfer::ResticTransferActor;
use uuid::Uuid;
use xactor::{message, Addr, Context, Sender};

mod container {
    use std::collections::{HashSet, VecDeque};

    use libblkcapt::core::retention::evaluate_retention;
    use slog::info;
    use xactor::{Actor, WeakAddr};

    use crate::xactorext::{BcAddr, BoxBcAddr};

    use super::*;

    pub struct ResticContainerActor {
        container_id: Uuid,
        repository: RepositoryState,
        snapshots: HashMap<Uuid, Vec<ResticContainerSnapshot>>,
        prune_schedule: Option<Schedule>,
        state: State,
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

    enum State {
        Active {
            active: Active,
            waiting: VecDeque<GetBackupMessage>,
        },
        Idle,
        Faulted,
    }

    enum Active {
        Transfer {
            actor: WeakAddr<BcActor<ResticTransferActor>>,
            dataset_id: Uuid,
            prune_pending: bool,
        },
        Prune {
            actor: Addr<BcActor<ResticPruneActor>>,
        },
    }

    impl State {
        fn take(&mut self) -> Self {
            mem::replace(self, State::Faulted)
        }
    }

    #[message(result = "Result<()>")]
    pub struct GetBackupMessage {
        source_dataset_id: Uuid,
        source_snapshot_handle: SnapshotHandle,
        target: WeakAddr<BcActor<ResticTransferActor>>,
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
                target: requestor_addr.downgrade(),
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
                    state: State::Idle,
                },
                &log.new(o!("container_id" => id.to_string())),
            )
        }

        fn schedule_next_prune(&self, log: &Logger, ctx: &mut Context<BcActor<Self>>) {
            schedule_next_message(self.prune_schedule.as_ref(), "prune", PruneMessage(), log, ctx);
        }

        async fn process_waiting(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>) {
            let mut state = self.state.take();

            if let State::Active { active, waiting } = &mut state {
                if matches!(active, Active::Transfer { prune_pending, .. } if *prune_pending) {
                    *active = self.start_prune(log, ctx).await;
                } else if let Some(waiter) = waiting.pop_front() {
                    *active = self.start_backup(waiter).await.expect("TODO");
                } else {
                    state = State::Idle;
                }
            }

            self.state = state;
        }

        async fn start_prune(&self, log: &Logger, ctx: &mut Context<BcActor<Self>>) -> Active {
            let repository = self.repository.get();
            let rules = repository
                .model()
                .snapshot_retention
                .as_ref()
                .expect("retention exist based on message scheduling in started");

            // forget
            let mut forgets = Vec::new();
            for (dataset_id, snapshots) in &self.snapshots {
                let snapshots = snapshots.iter().collect();
                let evaluation = evaluate_retention(snapshots, rules).expect("TODO");
                forgets.extend(evaluation.drop_snapshots);
            }

            repository.forget(&forgets).await.expect("TODO");

            // start prune process
            let prune = self.repository.get().prune();
            let actor = ResticPruneActor::new(ctx.address(), prune, log)
                .start()
                .await
                .expect("TODO");
            Active::Prune { actor }
        }

        async fn start_backup(&self, msg: GetBackupMessage) -> Result<Active> {
            const BASE_PATH: &str = "/var/lib/blkcapt/restic";
            let bind_path = {
                let mut p = PathBuf::from(BASE_PATH);
                p.push(self.container_id.to_string());
                p.push(msg.source_dataset_id.to_string());
                p
            };

            let repository = &self.repository.get();
            let existing_snapshot = repository
                .snapshot_by_datetime(&bind_path, msg.source_snapshot_handle.datetime)
                .await
                .context("existing snapshot check failed")?;

            if existing_snapshot.is_some() {
                anyhow::bail!(
                    "backup requested for existing snapshot dataset_id: {} snapshot_datetime: {}",
                    msg.source_dataset_id,
                    msg.source_snapshot_handle.datetime
                )
            }

            let snapshot_backup = repository.backup(bind_path, msg.source_dataset_id, msg.source_snapshot_handle);
            let addr = msg.target.upgrade().context("transfer is no longer alive")?;
            let _ = addr.send(BackupReadyMessage(Ok(snapshot_backup)));
            Ok(Active::Transfer {
                dataset_id: msg.source_dataset_id,
                prune_pending: false,
                actor: addr.downgrade(),
            })
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

        async fn stopped(&mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>) {
            self.state = match self.state.take() {
                State::Active { active, waiting } => {
                    let maybe_actor: Option<BoxBcAddr> = match active {
                        Active::Transfer { actor, .. } => actor.upgrade().map(|a| a.into()),
                        Active::Prune { actor } => Some(actor.into()),
                    };
                    if let Some(mut actor) = maybe_actor {
                        let _ = actor.stop();
                        actor.wait_for_stop().await;
                    }
                    for waiter in waiting {
                        if let Some(addr) = waiter.target.upgrade() {
                            let _ = addr.send(BackupReadyMessage(Err(anyhow!("container stopped"))));
                        }
                    }
                    State::Idle
                }
                State::Idle => State::Idle,
                State::Faulted => State::Faulted,
            }
        }
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
            match &mut self.state {
                State::Active { waiting, .. } => {
                    waiting.push_back(msg);
                    Ok(())
                }
                State::Idle => match self.start_backup(msg).await {
                    Ok(active) => {
                        self.state = State::Active {
                            active,
                            waiting: Default::default(),
                        };
                        Ok(())
                    }
                    Err(e) => Err(e),
                },
                State::Faulted => Err(anyhow!("actor faulted")),
            }
        }
    }

    #[async_trait::async_trait]
    impl BcHandler<PruneMessage> for ResticContainerActor {
        async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, _msg: PruneMessage) {
            match &mut self.state {
                State::Active {
                    active: Active::Transfer { prune_pending, .. },
                    ..
                } => {
                    *prune_pending = true;
                }
                State::Active {
                    active: Active::Prune { .. },
                    ..
                } => {
                    info!(log, "prune triggered, but already pruning");
                }
                State::Idle => {
                    self.state = State::Active {
                        active: self.start_prune(log, ctx).await,
                        waiting: Default::default(),
                    }
                }
                State::Faulted => {}
            }
        }
    }

    #[async_trait::async_trait]
    impl BcHandler<ParentTransferComplete> for ResticContainerActor {
        async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: ParentTransferComplete) {
            match &mut self.state {
                State::Active {
                    active: Active::Transfer { dataset_id, .. },
                    ..
                } => {
                    if let Some(snapshot) = msg.1 {
                        info!(log, "snapshot received"; "dataset_id" => %dataset_id, "time" => %snapshot.datetime);
                        self.snapshots.entry(*dataset_id).or_default().push(snapshot);
                    }

                    // TODO deal with failed transfer

                    self.process_waiting(log, ctx).await;
                }
                State::Active {
                    active: Active::Prune { .. },
                    ..
                }
                | State::Idle => {
                    ctx.stop(None);
                    self.state = State::Faulted;
                }
                State::Faulted => {}
            }
        }
    }

    #[async_trait::async_trait]
    impl BcHandler<PruneCompleteMessage> for ResticContainerActor {
        async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: PruneCompleteMessage) {
            match &mut self.state {
                State::Active {
                    active: Active::Prune { .. },
                    ..
                } => {
                    self.process_waiting(log, ctx).await;
                }
                State::Active {
                    active: Active::Transfer { .. },
                    ..
                }
                | State::Idle => {
                    ctx.stop(None);
                    self.state = State::Faulted;
                }
                State::Faulted => {}
            }
        }
    }
}

mod transfer {
    use slog::{debug, error, warn};

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
            let (terminal_state, result) = match self.state.take() {
                State::Transferring(_holder, worker_task) => {
                    warn!(log, "cancelled during transfer");
                    worker_task.abort();
                    debug!(log, "waiting for worker");
                    worker_task.wait().await;
                    (TerminalState::Cancelled, None)
                }
                State::WaitingForHoldAndBackup(..) => {
                    warn!(log, "cancelled prior to transfer");
                    (TerminalState::Cancelled, None)
                }
                State::Transferred(result) => (result.as_ref().into(), result.ok()),
                State::Faulted => {
                    error!(log, "actor faulted");
                    (TerminalState::Faulted, None)
                }
                State::Stopped(_) => panic!(),
            };
            self.state = State::Stopped(terminal_state);

            let container_notify_result = self.parent.send(ParentTransferComplete(terminal_state, result));
            let parent_notify_result = self.requestor.send(TransferComplete(terminal_state));
            if !matches!(terminal_state, TerminalState::Cancelled) {
                unhandled_result(log, container_notify_result);
                unhandled_result(log, parent_notify_result);
            }
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
                let started_backup = backup.start(&holder_state.snapshot_path);
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

    #[message]
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
    use libblkcapt::core::restic::{ResticPrune, StartedResticPrune};

    pub struct ResticPruneActor {
        state: State,
        parent: Addr<BcActor<ResticContainerActor>>,
    }

    enum State {
        Created(ResticPrune),
        Started(WorkerTask),
        Faulted,
        Stopped(TerminalState),
    }

    impl ResticPruneActor {
        pub fn new(container: Addr<BcActor<ResticContainerActor>>, prune: ResticPrune, log: &Logger) -> BcActor<Self> {
            BcActor::new(
                Self {
                    state: State::Created(prune),
                    parent: container,
                },
                log,
            )
        }
    }

    impl ResticPruneActor {}

    #[message]
    pub struct PruneCompleteMessage;

    type PruneWorkerCompleteMessage = WorkerCompleteMessage<Result<()>>;

    #[async_trait::async_trait]
    impl BcActorCtrl for ResticPruneActor {
        async fn started(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>) -> Result<()> {
            if let State::Created(pruner) = mem::replace(&mut self.state, State::Faulted) {
                let pruner = pruner.start()?;
                let task = WorkerTask::run(ctx.address(), log, |_| async move { pruner.wait().await.into() });
                self.state = State::Started(task);
            } else {
                ctx.stop(None);
            }
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl BcHandler<PruneWorkerCompleteMessage> for ResticPruneActor {
        async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: PruneWorkerCompleteMessage) {
            if let State::Started(..) = mem::replace(&mut self.state, State::Faulted) {
                self.state = State::Stopped(msg.0.into());
                unhandled_result(
                    log,
                    self.parent
                        .send(PruneCompleteMessage)
                        .context("failed to notify parent of completion"),
                );
            }
            ctx.stop(None);
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
