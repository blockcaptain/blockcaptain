use super::pool::PoolActor;
use super::{observation::HealthchecksActor, sync::SyncActor};
use crate::xactorext::{join_all_actors, stop_all_actors, GetChildActorMessage};
use crate::xactorext::{BcActor, BcActorCtrl};
use anyhow::{Context as AnyhowContext, Result};
use futures_util::{
    future::ready,
    stream::{self, FuturesUnordered, StreamExt},
};
use libblkcapt::model::{entities::SnapshotSyncEntity, storage, Entities, Entity};
use slog::{error, trace, Logger};
use std::{collections::HashMap, mem};
use uuid::Uuid;
use xactor::{Actor, Addr, Context};

pub struct CaptainActor {
    healthcheck_actors: Vec<Addr<BcActor<HealthchecksActor>>>,
    sync_actors: Vec<Addr<BcActor<SyncActor>>>,
    pool_actors: HashMap<Uuid, Addr<BcActor<PoolActor>>>,
}

impl CaptainActor {
    pub fn new(log: &Logger) -> BcActor<Self> {
        BcActor::new(
            Self {
                healthcheck_actors: Default::default(),
                sync_actors: Default::default(),
                pool_actors: Default::default(),
            },
            log,
        )
    }

    async fn new_sync_actor(
        &self,
        entities: &Entities,
        model: SnapshotSyncEntity,
        log: &Logger,
    ) -> Result<BcActor<SyncActor>> {
        let dataset_pool_id = entities
            .dataset(model.dataset_id())
            .map(|p| p.parent.id())
            .context("Invalid sync configuration. Source dataset does not exist.")?;

        let container_pool_id = entities
            .container(model.container_id())
            .map(|p| p.parent.id())
            .context("Invalid sync configuration. Destination container does not exist.")?;

        let dataset_pool = self
            .pool_actors
            .get(&dataset_pool_id)
            .context("Source dataset's pool didn't start.")?;
        let container_pool = self
            .pool_actors
            .get(&container_pool_id)
            .context("Destination container's pool didn't start.")?;

        let dataset_actor = dataset_pool
            .call(GetChildActorMessage::new(model.dataset_id()))
            .await?
            .context("Source dataset didn't start.")?;
        let container_actor = container_pool
            .call(GetChildActorMessage::new(model.container_id()))
            .await?
            .context("Destination container didn't start.")?;

        Ok(SyncActor::new(dataset_actor, container_actor, model, log))
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for CaptainActor {
    async fn started(&mut self, log: &Logger, _ctx: &mut Context<BcActor<Self>>) -> Result<()> {
        let mut entities = storage::load_entity_state();
        if !entities.observers.is_empty() {
            trace!(log, "building observer actors");
            self.healthcheck_actors = entities
                .observers
                .drain(..)
                .map(|m| HealthchecksActor::new(m, &log).start())
                .collect::<FuturesUnordered<_>>()
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .filter_map(|sa| match sa {
                    Ok(started_actor) => Some(started_actor),
                    Err(error) => {
                        error!(log, "Failed to start observer actor: {}", error);
                        None
                    }
                })
                .collect();
        };

        if !entities.btrfs_pools.is_empty() {
            trace!(log, "building pool actors");
            self.pool_actors = entities
                .btrfs_pools
                .iter()
                .map(|m| (m.id(), PoolActor::new(m.clone(), &log)))
                .map(|(id, actor)| async move {
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
                        error!(log, "Failed to start pool actor: {}", error);
                        None
                    }
                })
                .collect();
        }

        if !entities.snapshot_syncs.is_empty() {
            trace!(log, "building sync actors");
            self.sync_actors = stream::iter(mem::take(&mut entities.snapshot_syncs).into_iter())
                .then(|m| self.new_sync_actor(&entities, m, log))
                .filter_map(|s| {
                    let actor = match s {
                        Ok(sync_actor) => Some(sync_actor),
                        Err(error) => {
                            error!(log, "Failed to create sync actor: {}", error);
                            None
                        }
                    };
                    ready(actor)
                })
                .then(|s| s.start())
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .filter_map(|sa| match sa {
                    Ok(started_actor) => Some(started_actor),
                    Err(error) => {
                        error!(log, "Failed to start sync actor: {}", error);
                        None
                    }
                })
                .collect();
        }

        Ok(())
    }

    async fn stopped(&mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>) {
        stop_all_actors(&mut self.healthcheck_actors);
        stop_all_actors(&mut self.sync_actors);
        stop_all_actors(self.pool_actors.values_mut());

        join_all_actors(self.healthcheck_actors.drain(..)).await;
        join_all_actors(self.sync_actors.drain(..)).await;
        join_all_actors(self.pool_actors.drain().map(|(_k, v)| v)).await;
    }
}