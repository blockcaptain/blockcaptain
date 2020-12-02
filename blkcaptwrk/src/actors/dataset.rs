use super::{
    localsender::{LocalSenderActor, LocalSenderFinishedMessage},
    observation::observable_func,
    pool::PoolActor,
};
use crate::{
    actorbase::schedule_next_message, actorbase::unhandled_error, snapshots::prune_snapshots, snapshots::PruneMessage,
    xactorext::GetActorStatusMessage,
};
use crate::{
    actorbase::unhandled_result,
    xactorext::{BcActor, BcActorCtrl, BcHandler},
};
use anyhow::{Context as AnyhowContext, Result};
use cron::Schedule;
use futures_util::future::ready;
use libblkcapt::{
    core::{retention::evaluate_retention, BtrfsDataset, BtrfsDatasetSnapshot, BtrfsPool, BtrfsSnapshot},
    core::{Snapshot, SnapshotHandle},
    model::entities::BtrfsDatasetEntity,
    model::entities::FeatureState,
    model::entities::ObservableEvent,
    model::Entity,
};
use slog::{info, o, Logger};
use std::{convert::TryInto, path::PathBuf, sync::Arc};
use xactor::{message, Actor, Addr, Context, Handler, Sender};

pub struct DatasetActor {
    pool: Addr<BcActor<PoolActor>>,
    dataset: Arc<BtrfsDataset>,
    snapshots: Vec<BtrfsDatasetSnapshot>,
    snapshot_schedule: Option<Schedule>,
    prune_schedule: Option<Schedule>,
}

#[message()]
#[derive(Clone)]
struct SnapshotMessage();

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
        requestor_addr: &Addr<A>,
        send_snapshot_handle: SnapshotHandle,
        parent_snapshot_handle: Option<SnapshotHandle>,
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
        requestor_addr: &Addr<A>,
        send_snapshot_handle: SnapshotHandle,
        parent_snapshot_handle: Option<SnapshotHandle>,
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
// {
//     pub send_snapshot_handle: SnapshotHandle,
//     pub parent_snapshot_handle: Option<SnapshotHandle>,
//     pub send_snapshot_path: PathBuf,
//     pub parent_snapshot_path: Option<PathBuf>,
// }

impl DatasetActor {
    pub fn new(
        pool_actor: Addr<BcActor<PoolActor>>,
        pool: &Arc<BtrfsPool>,
        model: BtrfsDatasetEntity,
        log: &Logger,
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
                },
                &log.new(o!("dataset_id" => id.to_string())),
            ))
        })
    }

    fn schedule_next_snapshot(&self, log: &Logger, ctx: &mut Context<BcActor<Self>>) {
        schedule_next_message(self.snapshot_schedule.as_ref(), "snapshot", SnapshotMessage(), log, ctx);
    }

    fn schedule_next_prune(&self, log: &Logger, ctx: &mut Context<BcActor<Self>>) {
        schedule_next_message(self.prune_schedule.as_ref(), "prune", PruneMessage(), log, ctx);
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for DatasetActor {
    async fn started(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>) -> Result<()> {
        if self.dataset.model().snapshotting_state() == FeatureState::Enabled {
            self.snapshot_schedule = self
                .dataset
                .model()
                .snapshot_schedule
                .as_ref()
                .map_or(Ok(None), |s| s.try_into().map(Some))?;

            self.schedule_next_snapshot(log, ctx);
        }

        if self.dataset.model().pruning_state() == FeatureState::Enabled {
            self.prune_schedule = self
                .dataset
                .model()
                .snapshot_retention
                .as_ref()
                .map(|r| &r.evaluation_schedule)
                .map_or(Ok(None), |s| s.try_into().map(Some))?;

            self.schedule_next_prune(log, ctx);
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl BcHandler<SnapshotMessage> for DatasetActor {
    async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, _msg: SnapshotMessage) {
        let result = observable_func(self.dataset.model().id(), ObservableEvent::DatasetSnapshot, || {
            ready(self.dataset.create_local_snapshot())
        })
        .await;
        match result {
            Ok(snapshot) => {
                info!(log, "snapshot created"; "time" => %snapshot.datetime());
                self.snapshots.push(snapshot);
            }
            Err(e) => {
                unhandled_error(log, e);
            }
        }

        self.schedule_next_snapshot(log, ctx);
    }
}

#[async_trait::async_trait]
impl BcHandler<PruneMessage> for DatasetActor {
    async fn handle(&mut self, log: &Logger, _ctx: &mut Context<BcActor<Self>>, _msg: PruneMessage) {
        let rules = self
            .dataset
            .model()
            .snapshot_retention
            .as_ref()
            .expect("retention exist based on message scheduling in started");

        let result = observable_func(self.dataset.model().id(), ObservableEvent::DatasetPrune, || {
            let result = self
                .dataset
                .snapshots()
                .and_then(|snapshots| evaluate_retention(snapshots, rules))
                .and_then(|eval| prune_snapshots(eval, &log));
            ready(result)
        })
        .await;

        unhandled_result(log, result);
    }
}

#[async_trait::async_trait]
impl BcHandler<GetDatasetSnapshotsMessage> for DatasetActor {
    async fn handle(
        &mut self,
        _log: &Logger,
        _ctx: &mut Context<BcActor<Self>>,
        _msg: GetDatasetSnapshotsMessage,
    ) -> DatasetSnapshotsResponse {
        DatasetSnapshotsResponse {
            snapshots: self.snapshots.iter().map(|s| s.into()).collect(),
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<GetSnapshotSenderMessage> for DatasetActor {
    async fn handle(
        &mut self,
        log: &Logger,
        ctx: &mut Context<BcActor<Self>>,
        msg: GetSnapshotSenderMessage,
    ) -> Result<()> {
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
            &log.new(o!("message" => ())),
        )
        .start()
        .await;
        msg.target_ready.send(SenderReadyMessage(started_sender_actor))?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl BcHandler<GetSnapshotHolderMessage> for DatasetActor {
    async fn handle(
        &mut self,
        log: &Logger,
        _ctx: &mut Context<BcActor<Self>>,
        msg: GetSnapshotHolderMessage,
    ) -> Result<()> {
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

        let started_holder_actor = DatasetHolderActor::new(log, msg.send_snapshot_handle, msg.parent_snapshot_handle)
            .start()
            .await;
        msg.target_ready.send(HolderReadyMessage {
            holder: started_holder_actor,
            snapshot_path: send_snapshot.canonical_path(),
            parent_snapshot_path: parent_snapshot.map(|s| s.canonical_path()),
        })?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl BcHandler<LocalSenderFinishedMessage> for DatasetActor {
    async fn handle(&mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>, _msg: LocalSenderFinishedMessage) {}
}

// #[async_trait::async_trait]
// impl BcHandler<ConfigureSendSnapshotMessage> for PoolActor {
//     async fn handle(&mut self, _log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: ConfigureSendSnapshotMessage) {
//         trace!("Pool actor configure send message.");
//         let delay_next = Self::next_scheduled_prune(); // TODO: change to a proper schedule
//         debug!(
//             "First snapshot send for {} in {}.",
//             msg.config.dataset_id(),
//             humantime::Duration::from(delay_next)
//         );
//         ctx.send_interval_later(
//             SendSnapshotMessage(Arc::from(msg)),
//             Duration::from_secs(24 * 3600), // TODO: proper frequncy or immediate
//             delay_next,
//         );
//     }
// }

// #[async_trait::async_trait]
// impl BcHandler<SendSnapshotMessage> for PoolActor {
//     async fn handle(&mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>, msg: SendSnapshotMessage) {
//         trace!("Pool actor send message.");
//         let dataset = &*self.owned_dataset(msg.0.config.dataset_id()).expect("FIXME");
//         let latest_in_container = msg
//             .0
//             .container_pool
//             .call(GetLatestSnapshot {
//                 dataset_id: msg.0.config.dataset_id(),
//                 container_id: msg.0.config.container_id(),
//             })
//             .await
//             .expect("FIXME");
//         let only_after = self
//             .queued_syncs
//             .back()
//             .or_else(|| self.active_sync.as_ref().map(|s| &s.0))
//             .map(|s| s.datetime())
//             .or(latest_in_container);
//         let ready_snapsots = ready_snapshots(snapshots, only_after);
//         self.queued_syncs.extend(ready_snapsots);

//         self.process_send_queue();
//     }
// }

#[async_trait::async_trait]
impl BcHandler<GetActorStatusMessage> for DatasetActor {
    async fn handle(
        &mut self,
        _log: &Logger,
        _ctx: &mut Context<BcActor<Self>>,
        _msg: GetActorStatusMessage,
    ) -> String {
        String::from("ok")
    }
}

pub struct DatasetHolderActor;

impl DatasetHolderActor {
    fn new(
        log: &Logger,
        send_handle: SnapshotHandle,
        parent_handle: Option<SnapshotHandle>,
    ) -> BcActor<DatasetHolderActor> {
        let snapshot_id = send_handle.uuid.to_string();
        let log = match parent_handle {
            Some(parent) => {
                log.new(o!("snapshot_pinned" => snapshot_id, "snapshot_parent_pinned" => parent.uuid.to_string()))
            }
            None => log.new(o!("snapshot_pinned" => snapshot_id)),
        };
        BcActor::new(DatasetHolderActor, &log)
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for DatasetHolderActor {}

#[async_trait::async_trait]
impl BcHandler<GetActorStatusMessage> for DatasetHolderActor {
    async fn handle(
        &mut self,
        _log: &Logger,
        _ctx: &mut Context<BcActor<Self>>,
        _msg: GetActorStatusMessage,
    ) -> String {
        String::from("ok")
    }
}
