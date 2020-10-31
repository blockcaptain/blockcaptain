use super::{observation::observable_func, pool::PoolActor};
use crate::{
    actorbase::unhandled_error, snapshots::prune_snapshots, snapshots::PruneMessage, xactorext::ActorContextExt,
};
use crate::{
    actorbase::unhandled_result,
    xactorext::{BcActor, BcActorCtrl, BcHandler},
};
use anyhow::{Context as AnyhowContext, Result};
use chrono::{DateTime, Local, Timelike, Utc};
use futures_util::future::ready;
use libblkcapt::{
    core::localsndrcv::SnapshotSender,
    core::{
        retention::{evaluate_retention, RetentionEvaluation},
        BtrfsDataset, BtrfsDatasetHandle, BtrfsDatasetSnapshot, BtrfsDatasetSnapshotHandle, BtrfsPool, BtrfsSnapshot,
    },
    model::entities::BtrfsDatasetEntity,
    model::entities::FeatureState,
    model::entities::ObservableEvent,
    model::Entity,
};
use slog::{debug, error, info, o, trace, Logger};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;
use xactor::{message, Actor, Addr, Context, Handler};

pub struct DatasetActor {
    pool: Addr<BcActor<PoolActor>>,
    dataset: Arc<BtrfsDataset>,
    snapshots: Vec<BtrfsDatasetSnapshot>,
}

#[message()]
#[derive(Clone)]
struct SnapshotMessage();

#[message(result = "DatasetSnapshotsResponse")]
pub struct GetDatasetSnapshotsMessage();

pub struct DatasetSnapshotsResponse {
    pub dataset: BtrfsDatasetHandle,
    pub snapshots: Vec<BtrfsDatasetSnapshotHandle>,
}

#[message(result = "Result<SnapshotSender>")]
pub struct GetSnapshotSenderMessage {
    pub send_snapshot_uuid: Uuid,
    pub parent_snapshot_uuid: Option<Uuid>,
}

// #[message()]
// pub struct ConfigureSendSnapshotMessage {
//     pub config: SnapshotSyncEntity,
//     pub container_pool: Addr<PoolActor>,
// }

// #[message()]
// #[derive(Clone)]
// struct SendSnapshotMessage(Arc<ConfigureSendSnapshotMessage>);

// #[message()]
// struct SendSnapshotCompleteMessage();

// #[message(result = "Option<DateTime<Utc>>")]
// struct GetLatestSnapshot {
//     dataset_id: Uuid,
//     container_id: Uuid,
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
                },
                &log.new(o!("dataset_id" => id.to_string())),
            ))
        })
    }

    fn next_scheduled_snapshot(&self, frequency: Duration, log: &Logger) -> Duration {
        let latest = self.snapshots.last();
        if let Some(latest_snapshot) = latest {
            trace!(
                log,
                "Existing snapshot for {} at {}.",
                self.dataset,
                latest_snapshot.datetime()
            );
            let now = chrono::Utc::now();
            let next_datetime = latest_snapshot.datetime() + chrono::Duration::from_std(frequency).unwrap();
            if now < next_datetime {
                return (next_datetime - now).to_std().unwrap();
            }
        } else {
            trace!(log, "No existing snapshot for {}.", self.dataset);
        }
        Duration::from_secs(0)
    }

    fn next_scheduled_prune() -> Duration {
        const WORK_HOUR: u32 = 2;
        let now = Local::now();
        let next = match now.hour() {
            hour if hour < WORK_HOUR => now.date(),
            _ => now.date() + chrono::Duration::days(1),
        };
        let next = next.and_hms(WORK_HOUR, 0, 0);

        (next - now).to_std().unwrap()
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for DatasetActor {
    async fn started(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>) -> Result<()> {
        if self.dataset.model().snapshotting_state() == FeatureState::Enabled {
            let frequency = self
                .dataset
                .model()
                .snapshot_frequency
                .expect("INVARIANT: Frequency must exist for snapshotting to be enabled.");
            let delay_next = self.next_scheduled_snapshot(frequency, log);
            debug!(
                log,
                "First snapshot for {} in {}.",
                self.dataset,
                humantime::Duration::from(delay_next)
            );
            ctx.send_interval_later(SnapshotMessage(), frequency, delay_next);
        }

        if self.dataset.model().pruning_state() == FeatureState::Enabled {
            let delay_next = Self::next_scheduled_prune();
            debug!(
                log,
                "First prune for {} in {}.",
                self.dataset,
                humantime::Duration::from(delay_next)
            );
            ctx.send_interval_later(
                PruneMessage(),
                self.dataset
                    .model()
                    .snapshot_retention
                    .as_ref()
                    .expect("INVARIANT: Retention must exist for pruning to be enabled.")
                    .evaluation_frequency,
                delay_next,
            );
        }

        Ok(())
    }

    async fn stopped(&mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>) {}
}

#[async_trait::async_trait]
impl BcHandler<SnapshotMessage> for DatasetActor {
    async fn handle(&mut self, log: &Logger, _ctx: &mut Context<BcActor<Self>>, _msg: SnapshotMessage) {
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
            dataset: self.dataset.as_ref().into(),
            snapshots: self.snapshots.iter().map(|s| s.into()).collect(),
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<GetSnapshotSenderMessage> for DatasetActor {
    async fn handle(
        &mut self,
        _log: &Logger,
        _ctx: &mut Context<BcActor<Self>>,
        msg: GetSnapshotSenderMessage,
    ) -> Result<SnapshotSender> {
        let send_snapshot = self
            .snapshots
            .iter()
            .find(|s| s.uuid() == msg.send_snapshot_uuid)
            .context("Snapshot not found.")?;
        let parent_snapshot = match msg.parent_snapshot_uuid {
            Some(uuid) => Some(
                self.snapshots
                    .iter()
                    .find(|s| s.uuid() == uuid)
                    .context("Parent not found")?,
            ),
            None => None,
        };
        Ok(send_snapshot.send(parent_snapshot))
    }
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

// #[async_trait::async_trait]
// impl BcHandler<SendSnapshotCompleteMessage> for PoolActor {
//     async fn handle(&mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>, msg: SendSnapshotCompleteMessage) {
//         trace!("Pool actor send complete message.");
//         self.active_sync = None;
//         self.process_send_queue();
//     }
// }

// #[async_trait::async_trait]
// impl BcHandler<GetLatestSnapshot> for PoolActor {
//     async fn handle(&mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>, msg: GetLatestSnapshot) -> Option<DateTime<Utc>> {
//         trace!("Pool actor send message.");
//         let container = self.container(msg.container_id).expect("FIXME");

//         let container_snapshots = container.snapshots(msg.dataset_id).expect("FIXME");
//         container_snapshots.last().map(|s| s.datetime())
//     }
// }
