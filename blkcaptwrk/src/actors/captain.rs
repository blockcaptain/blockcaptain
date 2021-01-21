use super::{observation::HealthchecksActor, server::ServerActor, sync::SyncActor};
use super::{pool::PoolActor, restic::ResticContainerActor, sync::SyncToContainer};
use crate::xactorext::{BcActor, BcActorCtrl, BcContext};
use crate::{
    actorbase::logged_result,
    xactorext::{
        join_all_actors, stop_all_actors, BcHandler, GetActorStatusMessage, GetChildActorMessage, TerminalState,
    },
};
use anyhow::{Context as AnyhowContext, Result};
use futures_util::{
    future::ready,
    stream::{self, FuturesUnordered, StreamExt},
};
use libblkcapt::{
    create_data_dir,
    model::{entities::SnapshotSyncEntity, storage, Entities, Entity, EntityId},
};
use slog::{error, trace, Logger};
use std::{collections::HashMap, mem};
use xactor::{Actor, Addr};

pub struct CaptainActor {
    healthcheck_actors: Vec<Addr<BcActor<HealthchecksActor>>>,
    sync_actors: Vec<Addr<BcActor<SyncActor>>>,
    pool_actors: HashMap<EntityId, Addr<BcActor<PoolActor>>>,
    restic_actors: HashMap<EntityId, Addr<BcActor<ResticContainerActor>>>,
    server_actor: Option<Addr<BcActor<ServerActor>>>,
}

impl CaptainActor {
    pub fn new(log: &Logger) -> BcActor<Self> {
        BcActor::new(
            Self {
                healthcheck_actors: Default::default(),
                sync_actors: Default::default(),
                pool_actors: Default::default(),
                restic_actors: Default::default(),
                server_actor: None,
            },
            log,
        )
    }

    async fn new_sync_actor(
        &self, entities: &Entities, model: SnapshotSyncEntity, log: &Logger,
    ) -> Result<BcActor<SyncActor>> {
        let dataset_pool_id = entities
            .dataset(model.dataset_id)
            .map(|p| p.parent.id())
            .context("Invalid sync configuration. Source dataset does not exist.")?;

        let dataset_pool = self
            .pool_actors
            .get(&dataset_pool_id)
            .context("Source dataset's pool didn't start.")?;

        let dataset_actor = dataset_pool
            .call(GetChildActorMessage::new(model.dataset_id))
            .await?
            .context("Source dataset didn't start.")?;

        let maybe_container_pool_id = entities.container(model.container_id).map(|p| p.parent.id());

        let to_container_actor = if let Some(container_pool_id) = maybe_container_pool_id {
            let container_pool = self
                .pool_actors
                .get(&container_pool_id)
                .context("Destination container's pool didn't start.")?;
            let container_actor = container_pool
                .call(GetChildActorMessage::new(model.container_id))
                .await?
                .context("Destination container didn't start.")?;

            SyncToContainer::Btrfs(container_actor)
        } else {
            let _ = entities
                .restic_container(model.container_id)
                .context("Invalid sync configuration. Destination container does not exist.")?;
            let container_actor = self
                .restic_actors
                .get(&model.container_id)
                .context("Destination restic container didn't start.")?;

            SyncToContainer::Restic(container_actor.clone())
        };

        Ok(SyncActor::new(dataset_actor, to_container_actor, model, log))
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for CaptainActor {
    async fn started(&mut self, ctx: BcContext<'_, Self>) -> Result<()> {
        create_data_dir()?;

        let mut entities = storage::load_entity_state();
        if !entities.observers.is_empty() {
            trace!(ctx.log(), "building observer actors");
            self.healthcheck_actors = entities
                .observers
                .drain(..)
                .map(|m| HealthchecksActor::new(m, ctx.log()).start())
                .collect::<FuturesUnordered<_>>()
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .filter_map(|sa| logged_result(ctx.log(), sa.context("failed to start observer actor")).ok())
                .collect();
        };

        if !entities.btrfs_pools.is_empty() {
            trace!(ctx.log(), "building pool actors");
            self.pool_actors = entities
                .btrfs_pools
                .iter()
                .map(|m| (m.id(), PoolActor::new(m.clone(), ctx.log())))
                .map(|(id, actor)| async move {
                    let addr = actor.start().await?;
                    Result::<_>::Ok((id, addr))
                })
                .collect::<FuturesUnordered<_>>()
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .filter_map(|sa| logged_result(ctx.log(), sa.context("failed to start pool actor")).ok())
                .collect();
        }

        if !entities.restic_containers.is_empty() {
            trace!(ctx.log(), "building restic actors");
            self.restic_actors = entities
                .restic_containers
                .iter()
                .map(|m| {
                    let log = ctx.log();
                    async move {
                        let addr = ResticContainerActor::new(m.clone(), log).start().await?;
                        Result::<_>::Ok((m.id(), addr))
                    }
                })
                .collect::<FuturesUnordered<_>>()
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .filter_map(|sa| logged_result(ctx.log(), sa.context("failed to start restic actor")).ok())
                .collect();
        };

        if !entities.snapshot_syncs.is_empty() {
            trace!(ctx.log(), "building sync actors");
            self.sync_actors = stream::iter(mem::take(&mut entities.snapshot_syncs).into_iter())
                .then(|m| self.new_sync_actor(&entities, m, ctx.log()))
                .filter_map(|s| {
                    let actor = match s {
                        Ok(sync_actor) => Some(sync_actor),
                        Err(error) => {
                            error!(ctx.log(), "Failed to create sync actor: {}", error);
                            None
                        }
                    };
                    ready(actor)
                })
                .then(|s| s.start())
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .filter_map(|sa| logged_result(ctx.log(), sa.context("failed to start sync actor")).ok())
                .collect();
        }

        self.server_actor = logged_result(
            ctx.log(),
            ServerActor::new(ctx.log())
                .start()
                .await
                .context("failed to start server actor"),
        )
        .ok();

        Ok(())
    }

    async fn stopped(&mut self, _ctx: BcContext<'_, Self>) -> TerminalState {
        stop_all_actors(&mut self.healthcheck_actors);
        stop_all_actors(&mut self.sync_actors);
        stop_all_actors(self.pool_actors.values_mut());
        stop_all_actors(self.restic_actors.values_mut());

        join_all_actors(self.healthcheck_actors.drain(..)).await;
        join_all_actors(self.sync_actors.drain(..)).await;
        join_all_actors(self.pool_actors.drain().map(|(_k, v)| v)).await;
        join_all_actors(self.restic_actors.drain().map(|(_k, v)| v)).await;

        if let Some(mut actor) = self.server_actor.take() {
            let _ = actor.stop(None);
            let _ = actor.wait_for_stop();
        }

        TerminalState::Succeeded
    }
}

#[async_trait::async_trait]
impl BcHandler<GetActorStatusMessage> for CaptainActor {
    async fn handle(&mut self, _ctx: BcContext<'_, Self>, _msg: GetActorStatusMessage) -> String {
        String::from("ok")
    }
}
