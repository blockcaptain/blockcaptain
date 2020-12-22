use super::{
    localsender::{LocalSenderActor, LocalSenderFinishedMessage, LocalSenderParentFinishedMessage},
    observation::observable_func,
    pool::PoolActor,
};
use crate::{
    actorbase::unhandled_result,
    xactorext::{BcActor, BcActorCtrl, BcContext, BcHandler},
};
use crate::{
    actorbase::{unhandled_error, ScheduledMessage},
    snapshots::PruneMessage,
    snapshots::{failed_snapshot_deletes_as_result, prune_btrfs_snapshots},
    xactorext::{join_all_actors, stop_all_actors, BoxBcWeakAddr, GetActorStatusMessage, TerminalState},
};
use anyhow::{Context as AnyhowContext, Result};
use futures_util::future::ready;
use libblkcapt::{
    core::{BtrfsDataset, BtrfsDatasetSnapshot, BtrfsPool, BtrfsSnapshot},
    core::{Snapshot, SnapshotHandle},
    model::entities::BtrfsDatasetEntity,
    model::entities::FeatureState,
    model::entities::ObservableEvent,
    model::Entity,
};
use slog::{info, o, Logger};
use std::{convert::TryInto, iter::once, path::PathBuf, sync::Arc};
use uuid::Uuid;
use xactor::{message, Actor, Addr, Handler, Sender};

pub struct DatasetActor {
    pool: Addr<BcActor<PoolActor>>,
    dataset: Arc<BtrfsDataset>,
    snapshots: Vec<BtrfsDatasetSnapshot>,
    snapshot_schedule: Option<ScheduledMessage>,
    prune_schedule: Option<ScheduledMessage>,
    active_sends_holds: Vec<(BoxBcWeakAddr, Uuid, Option<Uuid>)>,
}

#[message()]
#[derive(Clone)]
struct SnapshotMessage;

#[message(result = "DatasetSnapshotsResponse")]
pub struct GetDatasetSnapshotsMessage;

pub struct DatasetSnapshotsResponse {
    pub snapshots: Vec<SnapshotHandle>,
}

#[message(result = "Result<()>")]
pub struct GetSnapshotSenderMessage {
    pub send_snapshot_handle: SnapshotHandle,
    pub parent_snapshot_handle: Option<SnapshotHandle>,
    pub target_ready: Sender<SenderReadyMessage>,
    pub target_finished: Sender<LocalSenderFinishedMessage>,
}

impl GetSnapshotSenderMessage {
    pub fn new<A>(
        requestor_addr: &Addr<A>, send_snapshot_handle: SnapshotHandle, parent_snapshot_handle: Option<SnapshotHandle>,
    ) -> Self
    where
        A: Handler<SenderReadyMessage> + Handler<LocalSenderFinishedMessage>,
    {
        Self {
            send_snapshot_handle,
            parent_snapshot_handle,
            target_ready: requestor_addr.sender(),
            target_finished: requestor_addr.sender(),
        }
    }
}

#[message()]
pub struct SenderReadyMessage(pub Result<Addr<BcActor<LocalSenderActor>>>);

#[message(result = "Result<()>")]
pub struct GetSnapshotHolderMessage {
    pub send_snapshot_handle: SnapshotHandle,
    pub parent_snapshot_handle: Option<SnapshotHandle>,
    pub target_ready: Sender<HolderReadyMessage>,
}

impl GetSnapshotHolderMessage {
    pub fn new<A>(
        requestor_addr: &Addr<A>, send_snapshot_handle: SnapshotHandle, parent_snapshot_handle: Option<SnapshotHandle>,
    ) -> Self
    where
        A: Handler<HolderReadyMessage>,
    {
        Self {
            send_snapshot_handle,
            parent_snapshot_handle,
            target_ready: requestor_addr.sender(),
        }
    }
}

#[message()]
pub struct HolderReadyMessage {
    pub holder: Result<Addr<BcActor<DatasetHolderActor>>>,
    pub snapshot_path: PathBuf,
    pub parent_snapshot_path: Option<PathBuf>,
}

impl DatasetActor {
    pub fn new(
        pool_actor: Addr<BcActor<PoolActor>>, pool: &Arc<BtrfsPool>, model: BtrfsDatasetEntity, log: &Logger,
    ) -> Result<BcActor<DatasetActor>> {
        let id = model.id();
        BtrfsDataset::validate(pool, model).map(Arc::new).and_then(|dataset| {
            Ok(BcActor::new(
                DatasetActor {
                    pool: pool_actor,
                    snapshots: dataset.snapshots()?,
                    dataset,
                    snapshot_schedule: None,
                    prune_schedule: None,
                    active_sends_holds: Default::default(),
                },
                &log.new(o!("dataset_id" => id.to_string())),
            ))
        })
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for DatasetActor {
    async fn started(&mut self, ctx: BcContext<'_, Self>) -> Result<()> {
        if self.dataset.model().snapshotting_state() == FeatureState::Enabled {
            self.snapshot_schedule = self.dataset.model().snapshot_schedule.as_ref().map_or(Ok(None), |s| {
                s.try_into()
                    .map(|schedule| Some(ScheduledMessage::new(schedule, "snapshot", SnapshotMessage, &ctx)))
            })?;
        }

        if self.dataset.model().pruning_state() == FeatureState::Enabled {
            self.prune_schedule = self
                .dataset
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
        let mut active_actors = self
            .active_sends_holds
            .drain(..)
            .filter_map(|(actor, ..)| actor.upgrade())
            .collect::<Vec<_>>();
        if !active_actors.is_empty() {
            stop_all_actors(&mut active_actors);
            join_all_actors(active_actors).await;
            TerminalState::Cancelled
        } else {
            TerminalState::Succeeded
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<SnapshotMessage> for DatasetActor {
    async fn handle(&mut self, ctx: BcContext<'_, Self>, _msg: SnapshotMessage) {
        let result = observable_func(self.dataset.model().id(), ObservableEvent::DatasetSnapshot, || {
            ready(self.dataset.create_local_snapshot())
        })
        .await;
        match result {
            Ok(snapshot) => {
                info!(ctx.log(), "snapshot created"; "time" => %snapshot.datetime());
                self.snapshots.push(snapshot);
            }
            Err(e) => {
                unhandled_error(ctx.log(), e);
            }
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<PruneMessage> for DatasetActor {
    async fn handle(&mut self, ctx: BcContext<'_, Self>, _msg: PruneMessage) {
        let result = observable_func(self.dataset.model().id(), ObservableEvent::DatasetPrune, || {
            let rules = self
                .dataset
                .model()
                .snapshot_retention
                .as_ref()
                .expect("retention exist based on message scheduling in started");

            let holds: Vec<_> = self
                .active_sends_holds
                .iter()
                .flat_map(|a| once(a.1).chain(a.2.into_iter()))
                .collect();
            let failed_deletes = prune_btrfs_snapshots(&mut self.snapshots, &holds, rules, ctx.log());
            ready(failed_snapshot_deletes_as_result(failed_deletes))
        })
        .await;

        unhandled_result(ctx.log(), result);
    }
}

#[async_trait::async_trait]
impl BcHandler<GetDatasetSnapshotsMessage> for DatasetActor {
    async fn handle(
        &mut self, _ctx: BcContext<'_, Self>, _msg: GetDatasetSnapshotsMessage,
    ) -> DatasetSnapshotsResponse {
        DatasetSnapshotsResponse {
            snapshots: self.snapshots.iter().map(|s| s.into()).collect(),
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<GetSnapshotSenderMessage> for DatasetActor {
    async fn handle(&mut self, ctx: BcContext<'_, Self>, msg: GetSnapshotSenderMessage) -> Result<()> {
        let send_snapshot = self
            .snapshots
            .iter()
            .find(|s| s.uuid() == msg.send_snapshot_handle.uuid)
            .context("Snapshot not found.")?;
        let parent_snapshot = match msg.parent_snapshot_handle {
            Some(handle) => Some(
                self.snapshots
                    .iter()
                    .find(|s| s.uuid() == handle.uuid)
                    .context("Parent not found")?,
            ),
            None => None,
        };

        let snapshot_sender = send_snapshot.send(parent_snapshot);
        let started_sender_actor = LocalSenderActor::new(
            ctx.address().sender(),
            msg.target_finished,
            snapshot_sender,
            &ctx.log().new(o!("message" => ())),
        )
        .start()
        .await;

        if let Ok(addr) = &started_sender_actor {
            self.active_sends_holds
                .push((addr.into(), send_snapshot.uuid(), parent_snapshot.map(|s| s.uuid())));
        }
        msg.target_ready.send(SenderReadyMessage(started_sender_actor))?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl BcHandler<GetSnapshotHolderMessage> for DatasetActor {
    async fn handle(&mut self, ctx: BcContext<'_, Self>, msg: GetSnapshotHolderMessage) -> Result<()> {
        let send_snapshot = self
            .snapshots
            .iter()
            .find(|s| s.uuid() == msg.send_snapshot_handle.uuid)
            .context("Snapshot not found.")?;
        let parent_snapshot = match &msg.parent_snapshot_handle {
            Some(handle) => Some(
                self.snapshots
                    .iter()
                    .find(|s| s.uuid() == handle.uuid)
                    .context("Parent not found")?,
            ),
            None => None,
        };

        let started_holder_actor = DatasetHolderActor::new(
            ctx.log(),
            ctx.address().sender(),
            msg.send_snapshot_handle,
            msg.parent_snapshot_handle,
        )
        .start()
        .await;
        if let Ok(addr) = &started_holder_actor {
            self.active_sends_holds
                .push((addr.into(), send_snapshot.uuid(), parent_snapshot.map(|s| s.uuid())));
        }
        msg.target_ready.send(HolderReadyMessage {
            holder: started_holder_actor,
            snapshot_path: send_snapshot.canonical_path(),
            parent_snapshot_path: parent_snapshot.map(|s| s.canonical_path()),
        })?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl BcHandler<LocalSenderParentFinishedMessage> for DatasetActor {
    async fn handle(&mut self, _ctx: BcContext<'_, Self>, msg: LocalSenderParentFinishedMessage) {
        self.active_sends_holds.retain(|(x, ..)| x.actor_id() != msg.0);
    }
}

#[async_trait::async_trait]
impl BcHandler<GetActorStatusMessage> for DatasetActor {
    async fn handle(&mut self, _ctx: BcContext<'_, Self>, _msg: GetActorStatusMessage) -> String {
        String::from("ok")
    }
}

pub struct DatasetHolderActor {
    parent: Sender<LocalSenderParentFinishedMessage>,
}

impl DatasetHolderActor {
    fn new(
        log: &Logger, parent: Sender<LocalSenderParentFinishedMessage>, send_handle: SnapshotHandle,
        parent_handle: Option<SnapshotHandle>,
    ) -> BcActor<DatasetHolderActor> {
        let snapshot_id = send_handle.uuid.to_string();
        let log = match parent_handle {
            Some(parent) => {
                log.new(o!("snapshot_pinned" => snapshot_id, "snapshot_parent_pinned" => parent.uuid.to_string()))
            }
            None => log.new(o!("snapshot_pinned" => snapshot_id)),
        };
        BcActor::new(DatasetHolderActor { parent }, &log)
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for DatasetHolderActor {
    async fn stopped(&mut self, ctx: BcContext<'_, Self>) -> TerminalState {
        let _ = self.parent.send(LocalSenderParentFinishedMessage(ctx.actor_id()));
        TerminalState::Succeeded
    }
}

#[async_trait::async_trait]
impl BcHandler<GetActorStatusMessage> for DatasetHolderActor {
    async fn handle(&mut self, _ctx: BcContext<'_, Self>, _msg: GetActorStatusMessage) -> String {
        String::from("ok")
    }
}
