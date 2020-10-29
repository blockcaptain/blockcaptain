use anyhow::{anyhow, bail, Context as AnyhowContext, Result};

use futures_util::stream::FuturesUnordered;
use libblkcapt::{
    core::retention::evaluate_retention, core::retention::RetentionEvaluation, core::BtrfsContainer,
    core::BtrfsContainerSnapshot, core::BtrfsDataset, core::BtrfsDatasetSnapshot, core::BtrfsPool, core::BtrfsSnapshot,
    model::entities::BtrfsPoolEntity, model::entities::FeatureState, model::entities::ObservableEvent,
    model::entities::SnapshotSyncEntity, model::Entity,
};
use slog::{debug, error, o, Logger};
use std::{collections::HashMap, collections::VecDeque, fmt::Debug, fmt::Display, sync::Arc, time::Duration};
use uuid::Uuid;
use xactor::{message, Actor, Addr, Context, Handler};

use super::{container::ContainerActor, dataset::DatasetActor};
use crate::xactorext::GetChildActorMessage;
use futures_util::stream::StreamExt;

pub struct PoolActor {
    pool: PoolState,
    datasets: HashMap<Uuid, Addr<DatasetActor>>,
    containers: HashMap<Uuid, Addr<ContainerActor>>,
    log: Logger,
}

enum PoolState {
    Started(Arc<BtrfsPool>),
    Pending(BtrfsPoolEntity),
}

#[message()]
#[derive(Clone)]
struct ScrubMessage();

impl PoolActor {
    pub fn new(model: BtrfsPoolEntity, log: &Logger) -> Self {
        Self {
            log: log.new(o!("actor" => "pool", "pool_id" => model.id().to_string())),
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
        debug!(self.log, "starting");

        if let PoolState::Pending(model) = &self.pool {
            self.pool = PoolState::Started(BtrfsPool::validate(model.clone()).map(Arc::new)?);
        } else {
            bail!("Pool already started.");
        }

        self.datasets = self
            .pool()
            .model()
            .datasets
            .iter()
            .map(|m| DatasetActor::new(ctx.address(), self.pool(), m.clone(), &self.log))
            .filter_map(|d| match d {
                Ok(dataset_actor) => Some(dataset_actor),
                Err(error) => {
                    error!(self.log, "Failed to create dataset actor: {}", error);
                    None
                }
            })
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
                    error!(self.log, "Failed to start dataset actor: {}", error);
                    None
                }
            })
            .collect();

        // How to do more code sharing with above???
        self.containers = self
            .pool()
            .model()
            .containers
            .iter()
            .map(|m| ContainerActor::new(ctx.address(), self.pool(), m.clone()))
            .filter_map(|d| match d {
                Ok(container_actor) => Some(container_actor),
                Err(error) => {
                    error!(self.log, "Failed to create container actor: {}", error);
                    None
                }
            })
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
                    error!(self.log, "Failed to start container actor: {}", error);
                    None
                }
            })
            .collect();

        // init scrubbing here
        // trace!("pool scrub {}", self.pool());

        debug!(self.log, "started");
        Ok(())
    }

    async fn stopped(&mut self, _ctx: &mut Context<Self>) {
        debug!(self.log, "stopped");
    }
}

#[async_trait::async_trait]
impl Handler<GetChildActorMessage<DatasetActor>> for PoolActor {
    async fn handle(
        &mut self,
        _ctx: &mut Context<Self>,
        msg: GetChildActorMessage<DatasetActor>,
    ) -> Option<Addr<DatasetActor>> {
        self.datasets.get(&msg.0).map(|d| d.clone())
    }
}

#[async_trait::async_trait]
impl Handler<GetChildActorMessage<ContainerActor>> for PoolActor {
    async fn handle(
        &mut self,
        _ctx: &mut Context<Self>,
        msg: GetChildActorMessage<ContainerActor>,
    ) -> Option<Addr<ContainerActor>> {
        self.containers.get(&msg.0).map(|d| d.clone())
    }
}
