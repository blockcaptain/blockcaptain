use super::{observation::observable_func, pool::PoolActor};
use crate::xactorext::ActorContextExt;
use anyhow::Result;
use chrono::{DateTime, Local, Timelike, Utc};
use futures_util::future::ready;
use libblkcapt::{
    core::{
        retention::{evaluate_retention, RetentionEvaluation},
        BtrfsDataset, BtrfsDatasetSnapshot, BtrfsPool, BtrfsSnapshot,
    },
    model::entities::BtrfsDatasetEntity,
    model::entities::FeatureState,
    model::entities::ObservableEvent,
    model::Entity,
};
use log::*;
use std::sync::Arc;
use std::{collections::HashMap, collections::VecDeque, fmt::Debug, fmt::Display, time::Duration};
use uuid::Uuid;
use xactor::{message, Actor, Addr, Context, Handler};

pub struct DatasetActor {
    pool: Addr<PoolActor>,
    dataset: Arc<BtrfsDataset>,
    snapshots: Vec<BtrfsDatasetSnapshot>,
}

#[message()]
#[derive(Clone)]
struct SnapshotMessage();

#[message()]
#[derive(Clone)]
struct PruneMessage();

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
    pub fn new(pool_actor: Addr<PoolActor>, pool: &Arc<BtrfsPool>, model: BtrfsDatasetEntity) -> Result<DatasetActor> {
        BtrfsDataset::validate(pool, model).map(Arc::new).and_then(|dataset| {
            Ok(DatasetActor {
                pool: pool_actor,
                snapshots: dataset.snapshots()?,
                dataset,
            })
        })
    }

    pub fn id(&self) -> Uuid {
        self.dataset.model().id()
    }

    fn next_scheduled_snapshot(&self, frequency: Duration) -> Duration {
        let latest = self.snapshots.last();
        if let Some(latest_snapshot) = latest {
            trace!(
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
            trace!("No existing snapshot for {}.", self.dataset);
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
impl Actor for DatasetActor {
    async fn started(&mut self, ctx: &mut xactor::Context<Self>) -> Result<()> {
        if self.dataset.model().snapshotting_state() == FeatureState::Enabled {
            let frequency = self
                .dataset
                .model()
                .snapshot_frequency
                .expect("INVARIANT: Frequency must exist for snapshotting to be enabled.");
            let delay_next = self.next_scheduled_snapshot(frequency);
            debug!(
                "First snapshot for {} in {}.",
                self.dataset,
                humantime::Duration::from(delay_next)
            );
            ctx.send_interval_later(SnapshotMessage(), frequency, delay_next);
        }

        if self.dataset.model().pruning_state() == FeatureState::Enabled {
            let delay_next = Self::next_scheduled_prune();
            debug!(
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

    async fn stopped(&mut self, ctx: &mut xactor::Context<Self>) {}
}

#[async_trait::async_trait]
impl Handler<SnapshotMessage> for DatasetActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: SnapshotMessage) {
        trace!("Dataset actor snapshot message.");
        let result = observable_func(self.id(), ObservableEvent::DatasetSnapshot, move || {
            ready(self.dataset.create_local_snapshot())
        })
        .await;
        if let Err(e) = result {
            error!("Failed to create snapshot: {}", e);
        }
    }
}

#[async_trait::async_trait]
impl Handler<PruneMessage> for DatasetActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: PruneMessage) {
        trace!("Dataset prune snapshot message.");

        let rules = self
            .dataset
            .model()
            .snapshot_retention
            .as_ref()
            .expect("INVARIANT: Retention exist based on message scheduling in started.");

        let result = observable_func(self.id(), ObservableEvent::DatasetPrune, || {
            let result = self
                .dataset
                .snapshots()
                .and_then(|snapshots| evaluate_retention(snapshots, rules))
                .and_then(prune_snapshots);
            ready(result)
        })
        .await;

        if let Err(e) = result {
            error!("Failed to prune dataset or container: {}", e);
        }
    }
}

fn prune_snapshots<T: BtrfsSnapshot>(evaluation: RetentionEvaluation<T>) -> Result<()> {
    if log_enabled!(Level::Trace) {
        for snapshot in evaluation.keep_interval_buckets.iter().flat_map(|b| b.snapshots.iter()) {
            trace!("Keeping snapshot {} reason: in retention interval.", snapshot);
        }

        for snapshot in evaluation.keep_minimum_snapshots.iter() {
            trace!("Keeping snapshot {} reason: keep minimum newest.", snapshot);
        }
    }

    for snapshot in evaluation.drop_snapshots {
        info!(
            "Snapshot {} is being pruned because it did not meet any retention criteria.",
            snapshot
        );
        snapshot.delete()?;
    }

    Ok(())
}

// #[async_trait::async_trait]
// impl Handler<ConfigureSendSnapshotMessage> for PoolActor {
//     async fn handle(&mut self, ctx: &mut Context<Self>, msg: ConfigureSendSnapshotMessage) {
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
// impl Handler<SendSnapshotMessage> for PoolActor {
//     async fn handle(&mut self, _ctx: &mut Context<Self>, msg: SendSnapshotMessage) {
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
// impl Handler<SendSnapshotCompleteMessage> for PoolActor {
//     async fn handle(&mut self, _ctx: &mut Context<Self>, msg: SendSnapshotCompleteMessage) {
//         trace!("Pool actor send complete message.");
//         self.active_sync = None;
//         self.process_send_queue();
//     }
// }

// #[async_trait::async_trait]
// impl Handler<GetLatestSnapshot> for PoolActor {
//     async fn handle(&mut self, _ctx: &mut Context<Self>, msg: GetLatestSnapshot) -> Option<DateTime<Utc>> {
//         trace!("Pool actor send message.");
//         let container = self.container(msg.container_id).expect("FIXME");

//         let container_snapshots = container.snapshots(msg.dataset_id).expect("FIXME");
//         container_snapshots.last().map(|s| s.datetime())
//     }
// }
