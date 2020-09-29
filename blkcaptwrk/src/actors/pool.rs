use anyhow::{anyhow, Result};
use chrono::{Local, Timelike};
use futures_util::future::ready;
use libblkcapt::{
    core::retention::evaluate_retention, core::retention::RetentionEvaluation, core::BtrfsContainer,
    core::BtrfsDataset, core::BtrfsPool, core::BtrfsSnapshot, model::entities::BtrfsPoolEntity,
    model::entities::FeatureState, model::entities::ObservableEvent, model::Entity,
};
use log::*;
use std::{collections::HashMap, fmt::Debug, fmt::Display, sync::Arc, time::Duration};
use uuid::Uuid;
use xactor::{message, Actor, Context, Handler};

use super::observation::observable_func;
use crate::xactorext::ActorContextExt;

#[message()]
#[derive(Clone, Debug)]
struct SnapshotMessage {
    dataset_id: Uuid,
}

#[message()]
#[derive(Clone, Debug)]
struct PruneMessage {
    dataset_or_container_id: Uuid,
}

#[message()]
#[derive(Clone, Debug)]
struct ScrubMessage {}

pub struct PoolActor {
    pool: Arc<BtrfsPool>,
    datasets: HashMap<Uuid, Arc<BtrfsDataset>>,
    containers: HashMap<Uuid, Arc<BtrfsContainer>>,
}

impl PoolActor {
    pub fn new(model: BtrfsPoolEntity) -> Result<Self> {
        let pool = BtrfsPool::validate(model).map(Arc::new)?;
        let datasets = pool
            .model()
            .datasets
            .iter()
            .map(|m| {
                BtrfsDataset::validate(&pool, m.clone())
                    .map(Arc::new)
                    .map(|d| (m.id(), d))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        let containers = pool
            .model()
            .containers
            .iter()
            .map(|m| {
                BtrfsContainer::validate(&pool, m.clone())
                    .map(Arc::new)
                    .map(|d| (m.id(), d))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        Ok(Self {
            pool,
            datasets,
            containers,
        })
    }

    fn next_scheduled_snapshot(dataset: &Arc<BtrfsDataset>, frequency: Duration) -> Result<Duration> {
        let latest = dataset.latest_snapshot()?;
        if let Some(latest_snapshot) = latest {
            trace!("Existing snapshot for {} at {}.", dataset, latest_snapshot.datetime());
            let now = chrono::Utc::now();
            let next_datetime = latest_snapshot.datetime() + chrono::Duration::from_std(frequency).unwrap();
            if now < next_datetime {
                return Ok((next_datetime - now).to_std().unwrap());
            }
        } else {
            trace!("No existing snapshot for {}.", dataset);
        }
        Ok(Duration::from_secs(0))
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
impl Actor for PoolActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        for (id, dataset) in self
            .datasets
            .iter()
            .filter(|(_, ds)| ds.model().snapshotting_state() == FeatureState::Enabled)
        {
            let frequency = dataset.model().snapshot_frequency.unwrap();
            let delay_next = Self::next_scheduled_snapshot(dataset, frequency)?;
            debug!(
                "First snapshot for {} in {}.",
                dataset,
                humantime::Duration::from(delay_next)
            );
            ctx.send_interval_later(SnapshotMessage { dataset_id: *id }, frequency, delay_next);
        }

        let prunable_datasets = self
            .datasets
            .iter()
            .filter_map(|(id, ds)| match ds.model().pruning_state() {
                FeatureState::Enabled => Some((
                    id,
                    ds.model().snapshot_retention.as_ref().unwrap(),
                    ds.as_ref() as &dyn Display,
                )),
                _ => None,
            });
        let prunable_containers = self
            .containers
            .iter()
            .filter_map(|(id, ds)| match ds.model().pruning_state() {
                FeatureState::Enabled => Some((
                    id,
                    ds.model().snapshot_retention.as_ref().unwrap(),
                    ds.as_ref() as &dyn Display,
                )),
                _ => None,
            });

        for (id, retention, displayable) in prunable_datasets.chain(prunable_containers) {
            let delay_next = Self::next_scheduled_prune();
            debug!(
                "First prune for {} in {}.",
                displayable,
                humantime::Duration::from(delay_next)
            );
            ctx.send_interval_later(
                PruneMessage {
                    dataset_or_container_id: *id,
                },
                retention.evaluation_frequency,
                delay_next,
            );
        }

        // init scrubbing here
        trace!("pool scrub {}", self.pool);

        info!("Pool actor started successfully.");
        Ok(())
    }

    async fn stopped(&mut self, _ctx: &mut Context<Self>) {
        info!("Pool actor stopped successfully.");
    }
}

#[async_trait::async_trait]
impl Handler<SnapshotMessage> for PoolActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: SnapshotMessage) {
        trace!("PoolActor actor snapshot message.");
        let dataset = self
            .datasets
            .get(&msg.dataset_id)
            .expect("Bad message. Dataset not found in pool.");
        let result = observable_func(dataset.model().id(), ObservableEvent::DatasetSnapshot, move || {
            ready(dataset.create_local_snapshot())
        })
        .await;
        if let Err(e) = result {
            error!("Failed to create snapshot: {}", e);
        }
    }
}

#[async_trait::async_trait]
impl Handler<PruneMessage> for PoolActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: PruneMessage) {
        trace!("PoolActor prune snapshot message.");

        let result = if let Some(dataset) = self.datasets.get(&msg.dataset_or_container_id) {
            let rules = dataset
                .model()
                .snapshot_retention
                .as_ref()
                .expect("Exists based on actor start.");
            observable_func(dataset.model().id(), ObservableEvent::DatasetPrune, move || {
                let result = dataset
                    .snapshots()
                    .and_then(|snapshots| evaluate_retention(snapshots, rules))
                    .and_then(prune_snapshots);
                ready(result)
            })
            .await
        } else if let Some(container) = self.containers.get(&msg.dataset_or_container_id) {
            let rules = container
                .model()
                .snapshot_retention
                .as_ref()
                .expect("Exists based on actor start.");
            observable_func(container.model().id(), ObservableEvent::ContainerPrune, move || {
                let result = container.source_dataset_ids().and_then(|ids| {
                    ids.iter()
                        .map(|id| {
                            trace!("Running prune for dataset id {} in container {}.", id, container);
                            container
                                .snapshots(*id)
                                .and_then(|snapshots| evaluate_retention(snapshots, rules))
                                .and_then(prune_snapshots)
                        })
                        .collect::<Result<()>>()
                });
                ready(result)
            })
            .await
        } else {
            Err(anyhow!("Bad message. Dataset or container not found in pool."))
        };

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
