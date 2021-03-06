use super::{
    dataset::{DatasetHolderActor, HolderReadyMessage},
    transfer::TransferComplete,
};
use crate::actorbase::log_result;
use crate::xactorext::{BcContext, BoxBcAddr};
use crate::{
    actorbase::unhandled_result,
    snapshots::{ContainerSnapshotsResponse, GetContainerSnapshotsMessage, PruneMessage},
    tasks::WorkerCompleteMessage,
    tasks::WorkerTask,
    xactorext::{BcActor, BcActorCtrl, BcHandler, GetActorStatusMessage, TerminalState},
};
use anyhow::{anyhow, bail, Context as AnyhowContext, Result};
use container::BackupReadyMessage;
pub use container::{GetBackupMessage, ResticContainerActor};
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
use slog::{debug, error, warn};
use slog::{o, trace, Logger};
use std::convert::TryInto;
use std::{collections::HashMap, hash::Hash, mem, panic, path::PathBuf, sync::Arc};
use transfer::ParentTransferComplete;
pub use transfer::ResticTransferActor;
use xactor::{message, Addr, Sender};

mod container {
    use std::collections::{HashSet, VecDeque};

    use chrono::{DateTime, Utc};
    use libblkcapt::{
        core::retention::evaluate_retention,
        model::{entities::ObservableEvent, EntityId},
        runtime_dir,
    };
    use slog::info;
    use xactor::{Actor, WeakAddr};

    use crate::{actorbase::ScheduledMessage, actors::observation::start_observation, snapshots::clear_deleted};

    use super::*;

    pub struct ResticContainerActor {
        container_id: EntityId,
        repository: RepositoryState,
        snapshots: HashMap<EntityId, Vec<ResticContainerSnapshot>>,
        prune_schedule: Option<ScheduledMessage>,
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
            dataset_id: EntityId,
            prune_pending: bool,
        },
        Prune {
            actor: Addr<BcActor<ResticPruneActor>>,
            forgets: Vec<(EntityId, HashSet<DateTime<Utc>>)>,
        },
    }

    impl State {
        fn take(&mut self) -> Self {
            mem::replace(self, State::Faulted)
        }
    }

    #[message(result = "Result<()>")]
    pub struct GetBackupMessage {
        source_dataset_id: EntityId,
        source_snapshot_handle: SnapshotHandle,
        target: WeakAddr<BcActor<ResticTransferActor>>,
    }

    #[message]
    pub struct BackupReadyMessage(pub Result<ResticBackup>);

    impl GetBackupMessage {
        pub fn new(
            requestor_addr: &Addr<BcActor<ResticTransferActor>>, source_dataset_id: EntityId,
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

        async fn process_waiting(&mut self, ctx: &BcContext<'_, Self>) {
            let mut state = mem::replace(&mut self.state, State::Idle);

            if let State::Active { active, waiting } = &mut state {
                let prune_pending = matches!(active, Active::Transfer { prune_pending, .. } if *prune_pending);

                if prune_pending {
                    if let Some(active_prune) = self.start_prune(ctx).await {
                        *active = active_prune;
                        self.state = state;
                        return;
                    }
                }

                while let Some(waiter) = waiting.pop_front() {
                    if let Ok(active_transfer) = self.start_backup(waiter).await {
                        *active = active_transfer;
                        self.state = state;
                        return;
                    }
                }
            }
        }

        async fn start_prune(&self, ctx: &BcContext<'_, Self>) -> Option<Active> {
            let observation = start_observation(self.container_id, ObservableEvent::ContainerPrune).await;
            let repository = self.repository.get();
            let rules = repository
                .model()
                .snapshot_retention
                .as_ref()
                .expect("retention exist based on message scheduling in started");

            // create forget process
            let evals = self
                .snapshots
                .iter()
                .map(|(dataset_id, snapshots)| {
                    trace!(ctx.log(), "prune container"; "dataset_id" => %dataset_id);
                    (*dataset_id, evaluate_retention(snapshots, rules))
                })
                .collect::<Vec<_>>();

            let forgets = evals
                .iter()
                .flat_map(|(_, eval)| eval.drop_snapshots.iter().cloned())
                .collect::<Vec<_>>();

            if forgets.is_empty() {
                observation.succeeded();
                return None;
            }

            let forget = repository.forget(&forgets);

            // create prune process
            let prune = repository.prune();

            // start forget+prune actor
            let actor_result = ResticPruneActor::new(ctx.address(), forget, prune, observation, ctx.log())
                .start()
                .await
                .context("failed to start prune actor");
            log_result(ctx.log(), &actor_result);
            actor_result
                .map(|actor| Active::Prune {
                    actor,
                    forgets: evals
                        .into_iter()
                        .map(|(id, eval)| (id, eval.drop_snapshots.into_iter().map(|s| s.datetime).collect()))
                        .collect(),
                })
                .ok()
        }

        async fn start_backup(&self, msg: GetBackupMessage) -> Result<Active> {
            let bind_path = {
                let mut p = runtime_dir();
                p.push("restic_bind");
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
        async fn started(&mut self, ctx: BcContext<'_, Self>) -> Result<()> {
            if let RepositoryState::Pending(model) = &self.repository {
                let repository = ResticRepository::validate(model.clone()).map(Arc::new)?;
                self.snapshots = group_by(repository.snapshots().await?, |s| &s.dataset_id);
                trace!(
                    ctx.log(),
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
                    .map_or(Ok(None), |s| {
                        s.try_into()
                            .map(|schedule| Some(ScheduledMessage::new(schedule, "prune", PruneMessage, &ctx)))
                    })?;
            }

            Ok(())
        }

        async fn stopped(&mut self, _ctx: BcContext<'_, Self>) -> TerminalState {
            match self.state.take() {
                State::Active { active, waiting } => {
                    let maybe_actor: Option<BoxBcAddr> = match active {
                        Active::Transfer { actor, .. } => actor.upgrade().map(|a| a.into()),
                        Active::Prune { actor, .. } => Some(actor.into()),
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
                    TerminalState::Cancelled
                }
                State::Idle => TerminalState::Succeeded,
                State::Faulted => TerminalState::Faulted,
            }
        }
    }

    #[async_trait::async_trait]
    impl BcHandler<GetContainerSnapshotsMessage> for ResticContainerActor {
        async fn handle(
            &mut self, _ctx: BcContext<'_, Self>, msg: GetContainerSnapshotsMessage,
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
        async fn handle(&mut self, _ctx: BcContext<'_, Self>, msg: GetBackupMessage) -> Result<()> {
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
        async fn handle(&mut self, ctx: BcContext<'_, Self>, _msg: PruneMessage) {
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
                    info!(ctx.log(), "prune triggered, but already pruning");
                }
                State::Idle => {
                    self.state = self
                        .start_prune(&ctx)
                        .await
                        .map(|active| State::Active {
                            active,
                            waiting: Default::default(),
                        })
                        .unwrap_or(State::Idle);
                }
                State::Faulted => {}
            }
        }
    }

    #[async_trait::async_trait]
    impl BcHandler<ParentTransferComplete> for ResticContainerActor {
        async fn handle(&mut self, ctx: BcContext<'_, Self>, msg: ParentTransferComplete) {
            match &mut self.state {
                State::Active {
                    active: Active::Transfer { dataset_id, .. },
                    ..
                } => {
                    if let Some(snapshot) = msg.0 {
                        info!(ctx.log(), "snapshot received"; "dataset_id" => %dataset_id, "time" => %snapshot.datetime);
                        self.snapshots.entry(*dataset_id).or_default().push(snapshot);
                    }

                    self.process_waiting(&ctx).await;
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
        async fn handle(&mut self, ctx: BcContext<'_, Self>, msg: PruneCompleteMessage) {
            match &mut self.state {
                State::Active {
                    active: Active::Prune { forgets, .. },
                    ..
                } => {
                    let PruneCompleteMessage(forgot) = msg;
                    if forgot {
                        for (dataset_id, snapshots) in forgets {
                            if let Some(cache) = self.snapshots.get_mut(dataset_id) {
                                clear_deleted(cache, mem::take(snapshots));
                            }
                        }
                    }

                    self.process_waiting(&ctx).await;
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

    #[async_trait::async_trait]
    impl BcHandler<GetActorStatusMessage> for ResticContainerActor {
        async fn handle(&mut self, _ctx: BcContext<'_, Self>, _msg: GetActorStatusMessage) -> String {
            match &self.state {
                State::Active { active, .. } => match active {
                    Active::Transfer { .. } => "transferring",
                    Active::Prune { .. } => "pruning",
                },
                State::Idle => "idle",
                State::Faulted => "faulted",
            }
            .into()
        }
    }
}

mod transfer {
    use crate::{
        actorbase::{state_result, state_result_from_result},
        actors::observation::StartedObservation,
    };

    use super::*;

    pub struct ResticTransferActor {
        parent: Addr<BcActor<ResticContainerActor>>,
        requestor: Sender<TransferComplete>,
        state: State,
    }

    enum State {
        WaitingForHoldAndBackup(Option<HolderState>, Option<ResticBackup>, StartedObservation),
        Transferring(Addr<BcActor<DatasetHolderActor>>, WorkerTask, StartedObservation),
        Transferred(Result<ResticContainerSnapshot>),
        Faulted,
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
        async fn started(&mut self, _ctx: BcContext<'_, Self>) -> Result<()> {
            Ok(())
        }

        async fn stopped(&mut self, ctx: BcContext<'_, Self>) -> TerminalState {
            let (terminal_state, result) = match self.state.take() {
                State::Transferring(_holder, worker_task, observation) => {
                    warn!(ctx.log(), "cancelled during transfer");
                    worker_task.abort();
                    debug!(ctx.log(), "waiting for worker");
                    worker_task.wait().await;
                    observation.cancelled();
                    state_result(TerminalState::Cancelled)
                }
                State::WaitingForHoldAndBackup(.., observation) => {
                    warn!(ctx.log(), "cancelled prior to transfer");
                    observation.cancelled();
                    state_result(TerminalState::Cancelled)
                }
                State::Transferred(result) => state_result_from_result(result),
                State::Faulted => {
                    error!(ctx.log(), "actor faulted");
                    state_result(TerminalState::Faulted)
                }
            };

            let container_notify_result = self.parent.send(ParentTransferComplete(result.ok()));
            let requestor_notify_result = self.requestor.send(TransferComplete(terminal_state));
            if !matches!(terminal_state, TerminalState::Cancelled) {
                unhandled_result(ctx.log(), container_notify_result);
                unhandled_result(ctx.log(), requestor_notify_result);
            }
            terminal_state
        }
    }

    impl ResticTransferActor {
        pub fn new(
            parent: Sender<TransferComplete>, container: Addr<BcActor<ResticContainerActor>>,
            observation: StartedObservation, log: &Logger,
        ) -> BcActor<Self> {
            BcActor::new(
                Self {
                    state: State::WaitingForHoldAndBackup(None, None, observation),
                    requestor: parent,
                    parent: container,
                },
                log,
            )
        }

        fn maybe_start_transfer(incoming: State, ctx: &BcContext<'_, Self>) -> State {
            if let State::WaitingForHoldAndBackup(Some(holder_state), Some(backup), observation) = incoming {
                let started_backup = backup.start(&holder_state.snapshot_path);
                match started_backup {
                    Ok(started) => {
                        let task =
                            WorkerTask::run(ctx.address(), ctx.log(), |_| async move { started.wait().await.into() });
                        State::Transferring(holder_state.holder, task, observation)
                    }
                    Err(e) => {
                        ctx.stop(None);
                        observation.error::<anyhow::Error, _>(&e);
                        State::Transferred(Err(e))
                    }
                }
            } else {
                incoming
            }
        }

        fn input_ready(&mut self, ctx: &BcContext<'_, Self>, input: InputReady) {
            self.state = match (self.state.take(), input) {
                (State::WaitingForHoldAndBackup(maybe_holder, None, observation), InputReady::Backup(Ok(backup))) => {
                    let updated_state = State::WaitingForHoldAndBackup(maybe_holder, Some(backup), observation);
                    Self::maybe_start_transfer(updated_state, ctx)
                }
                (
                    State::WaitingForHoldAndBackup(None, maybe_backup, observation),
                    InputReady::Holder(Ok(holder_state)),
                ) => {
                    let updated_state = State::WaitingForHoldAndBackup(Some(holder_state), maybe_backup, observation);
                    Self::maybe_start_transfer(updated_state, ctx)
                }
                (State::WaitingForHoldAndBackup(_, None, observation), InputReady::Backup(Err(e)))
                | (State::WaitingForHoldAndBackup(None, _, observation), InputReady::Holder(Err(e))) => {
                    ctx.stop(None);
                    observation.error::<anyhow::Error, _>(&e);
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
    pub struct ParentTransferComplete(pub Option<ResticContainerSnapshot>);

    #[async_trait::async_trait]
    impl BcHandler<HolderReadyMessage> for ResticTransferActor {
        async fn handle(&mut self, ctx: BcContext<'_, Self>, msg: HolderReadyMessage) {
            let HolderReadyMessage {
                holder, snapshot_path, ..
            } = msg;
            self.input_ready(&ctx, holder.map(|h| HolderState::new(h, snapshot_path)).into());
        }
    }

    #[async_trait::async_trait]
    impl BcHandler<BackupReadyMessage> for ResticTransferActor {
        async fn handle(&mut self, ctx: BcContext<'_, Self>, msg: BackupReadyMessage) {
            self.input_ready(&ctx, msg.0.into());
        }
    }

    #[async_trait::async_trait]
    impl BcHandler<BackupWorkerCompleteMessage> for ResticTransferActor {
        async fn handle(&mut self, ctx: BcContext<'_, Self>, msg: BackupWorkerCompleteMessage) {
            if let State::Transferring(.., observation) = self.state.take() {
                observation.result(&msg.0);
                self.state = State::Transferred(msg.0);
            }
            ctx.stop(None);
        }
    }

    #[async_trait::async_trait]
    impl BcHandler<GetActorStatusMessage> for ResticTransferActor {
        async fn handle(&mut self, _ctx: BcContext<'_, Self>, _msg: GetActorStatusMessage) -> String {
            String::from("ok")
        }
    }
}

mod prune {
    use crate::actors::observation::StartedObservation;

    use super::*;
    use libblkcapt::core::restic::{ResticForget, ResticPrune};

    pub struct ResticPruneActor {
        state: State,
        parent: Addr<BcActor<ResticContainerActor>>,
        forgot: bool,
    }

    enum State {
        Created(ResticForget, ResticPrune, StartedObservation),
        StartedForget(WorkerTask, ResticPrune, StartedObservation),
        StartedPrune(WorkerTask, StartedObservation),
        Pruned(Result<()>, StartedObservation),
        Faulted,
    }

    impl State {
        fn take(&mut self) -> Self {
            mem::replace(self, State::Faulted)
        }
    }

    impl ResticPruneActor {
        pub fn new(
            container: Addr<BcActor<ResticContainerActor>>, forget: ResticForget, prune: ResticPrune,
            observation: StartedObservation, log: &Logger,
        ) -> BcActor<Self> {
            BcActor::new(
                Self {
                    state: State::Created(forget, prune, observation),
                    parent: container,
                    forgot: false,
                },
                log,
            )
        }
    }

    impl ResticPruneActor {}

    #[message]
    pub struct PruneCompleteMessage(pub bool);

    type PruneWorkerCompleteMessage = WorkerCompleteMessage<Result<()>>;

    #[async_trait::async_trait]
    impl BcActorCtrl for ResticPruneActor {
        async fn started(&mut self, ctx: BcContext<'_, Self>) -> Result<()> {
            if let State::Created(forget, prune, observation) = self.state.take() {
                let forgetter = forget.start()?;
                let task = WorkerTask::run(
                    ctx.address(),
                    ctx.log(),
                    |_| async move { forgetter.wait().await.into() },
                );
                self.state = State::StartedForget(task, prune, observation);
            }
            Ok(())
        }

        async fn stopped(&mut self, ctx: BcContext<'_, Self>) -> TerminalState {
            let terminal_state = match self.state.take() {
                State::Created(.., observation) => {
                    warn!(ctx.log(), "cancelled prior to transfer");
                    observation.cancelled();
                    TerminalState::Cancelled
                }
                State::StartedForget(worker, _, observation) => {
                    warn!(ctx.log(), "cancelled during forget");
                    observation.cancelled();
                    worker.abort();
                    TerminalState::Cancelled
                }
                State::StartedPrune(worker, observation) => {
                    warn!(ctx.log(), "cancelled during prune");
                    observation.cancelled();
                    worker.abort();
                    TerminalState::Cancelled
                }
                State::Pruned(result, observation) => {
                    log_result(ctx.log(), &result);
                    observation.result(&result);
                    result.into()
                }
                State::Faulted => TerminalState::Faulted,
            };

            let container_notify_result = self.parent.send(PruneCompleteMessage(self.forgot));
            if !matches!(terminal_state, TerminalState::Cancelled) {
                unhandled_result(ctx.log(), container_notify_result);
            }
            terminal_state
        }
    }

    #[async_trait::async_trait]
    impl BcHandler<PruneWorkerCompleteMessage> for ResticPruneActor {
        async fn handle(&mut self, ctx: BcContext<'_, Self>, msg: PruneWorkerCompleteMessage) {
            self.state =
                match self.state.take() {
                    State::StartedForget(_, prune, observation) => match msg.0 {
                        Ok(_) => {
                            self.forgot = true;
                            match prune.start() {
                                Ok(pruner) => {
                                    let task = WorkerTask::run(ctx.address(), ctx.log(), |_| async move {
                                        pruner.wait().await.into()
                                    });
                                    State::StartedPrune(task, observation)
                                }
                                Err(e) => {
                                    ctx.stop(None);
                                    State::Pruned(Err(e), observation)
                                }
                            }
                        }
                        e => {
                            ctx.stop(None);
                            State::Pruned(e, observation)
                        }
                    },
                    State::StartedPrune(_, observation) => {
                        ctx.stop(None);
                        State::Pruned(msg.0, observation)
                    }
                    State::Faulted | State::Pruned(..) | State::Created(..) => {
                        ctx.stop(None);
                        State::Faulted
                    }
                }
        }
    }

    #[async_trait::async_trait]
    impl BcHandler<GetActorStatusMessage> for ResticPruneActor {
        async fn handle(&mut self, _ctx: BcContext<'_, Self>, _msg: GetActorStatusMessage) -> String {
            String::from("ok")
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
