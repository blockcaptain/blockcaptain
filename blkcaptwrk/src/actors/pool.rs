use anyhow::{anyhow, bail, Context as AnyhowContext, Result};
use chrono::{DateTime, Local, Timelike, Utc};
use futures_util::{future::ready, stream::FuturesUnordered};
use libblkcapt::{
    core::retention::evaluate_retention, core::retention::RetentionEvaluation, core::sync::ready_snapshots,
    core::BtrfsContainer, core::BtrfsContainerSnapshot, core::BtrfsDataset, core::BtrfsDatasetSnapshot,
    core::BtrfsPool, core::BtrfsSnapshot, model::entities::BtrfsPoolEntity, model::entities::FeatureState,
    model::entities::ObservableEvent, model::entities::SnapshotSyncEntity, model::Entity,
};
use log::*;
use std::{collections::HashMap, collections::VecDeque, fmt::Debug, fmt::Display, sync::Arc, time::Duration};
use uuid::Uuid;
use xactor::{message, Actor, Addr, Context, Handler};

use super::{container::ContainerActor, dataset::DatasetActor, observation::observable_func, sync::SyncActor};
use crate::xactorext::ActorContextExt;
use futures_util::stream::StreamExt;

pub struct PoolActor {
    pool: PoolState,
    datasets: HashMap<Uuid, Addr<DatasetActor>>,
    containers: HashMap<Uuid, Addr<ContainerActor>>,
}

enum PoolState {
    Started(Arc<BtrfsPool>),
    Pending(BtrfsPoolEntity),
}

#[message()]
#[derive(Clone)]
struct ScrubMessage();

impl PoolActor {
    pub fn new(model: BtrfsPoolEntity) -> Self {
        Self {
            pool: PoolState::Pending(model),
            datasets: HashMap::<_, _>::default(),
            containers: HashMap::<_, _>::default(),
        }
    }

    pub fn id(&self) -> Uuid {
        match &self.pool {
            PoolState::Started(pool) => pool.model().id(),
            PoolState::Pending(model) => model.id(),
        }
    }

    fn pool(&self) -> &Arc<BtrfsPool> {
        match &self.pool {
            PoolState::Started(pool) => pool,
            PoolState::Pending(_) => panic!("Message received on unstarted pool."),
        }
    }

    // fn process_send_queue(&mut self) {
    //     if !self.queued_syncs.is_empty() && self.active_sync.is_none() {
    //         // func get source parent. (from worker code)
    //         // msg other pool to figure out parent are return a writer and if it can accept the parent.
    //         // func to get parent. (from worker code)
    //         // create a reader with/without parent.

    //         // start a sync actor now given the reader and writers.
    //     }
    // }

    // fn dataset(&self, id: Uuid) -> Result<&Arc<BtrfsDataset>> {
    //     self.datasets
    //         .get(&id)
    //         .with_context(|| format!("Bad message. Dataset with id {} not found in this pool.", id))
    //         .map(|d| &d.dataset)
    // }

    // fn owned_dataset(&mut self, id: Uuid) -> Result<&mut OwnedDataset> {
    //     self.datasets
    //         .get_mut(&id)
    //         .with_context(|| format!("Bad message. Dataset with id {} not found in this pool.", id))
    // }

    // fn container(&self, id: Uuid) -> Result<&Arc<BtrfsContainer>> {
    //     self.containers
    //         .get(&id)
    //         .with_context(|| format!("Bad message. Dataset with id {} not found in this pool.", id))
    //         .map(|c| &c.container)
    // }
}

#[async_trait::async_trait]
impl Actor for PoolActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        if let PoolState::Pending(model) = &self.pool {
            self.pool = PoolState::Started(BtrfsPool::validate(model.clone()).map(Arc::new)?);
        } else {
            bail!("Pool already started.");
        }

        let dataset_actors = self
            .pool()
            .model()
            .datasets
            .iter()
            .map(|m| DatasetActor::new(ctx.address(), self.pool(), m.clone()))
            .filter_map(|d| match d {
                Ok(dataset_actor) => Some(dataset_actor),
                Err(error) => {
                    error!("Failed to create dataset actor: {}", error);
                    None
                }
            })
            .collect::<Vec<_>>();

        self.datasets = dataset_actors
            .into_iter()
            .map(|actor| async {
                let id = actor.id();
                let addr = actor.start().await?;
                Result::<_>::Ok((id, addr))
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .filter_map(|sa| match sa {
                Ok(started_actor) => Some(started_actor),
                Err(error) => {
                    error!("Failed to start dataset actor: {}", error);
                    None
                }
            })
            .collect();

        // init containers here

        // init scrubbing here
        // trace!("pool scrub {}", self.pool());

        info!("Pool actor started successfully.");
        Ok(())
    }

    async fn stopped(&mut self, _ctx: &mut Context<Self>) {
        info!("Pool actor stopped successfully.");
    }
}
