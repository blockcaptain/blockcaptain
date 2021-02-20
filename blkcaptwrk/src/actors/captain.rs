use super::{observation::HealthchecksActor, server::ServerActor, sync::SyncActor};
use super::{pool::PoolActor, restic::ResticContainerActor, sync::SyncToContainer};
use crate::{
    actorbase::logged_error,
    xactorext::{BcActor, BcActorCtrl, BcContext},
};
use crate::{
    actorbase::logged_result,
    xactorext::{
        join_all_actors, stop_all_actors, BcHandler, GetActorStatusMessage, GetChildActorMessage, TerminalState,
    },
};
use anyhow::{Context as AnyhowContext, Result};
use futures_util::{
    future,
    stream::{FuturesUnordered, StreamExt},
};
use libblkcapt::{
    create_data_dir,
    model::{entities::SnapshotSyncEntity, storage, AnyContainer, Entities, Entity, EntityId, EntityStatic},
};
use slog::{trace, Logger};
use std::collections::HashMap;
use std::future::Future;
use xactor::{Actor, Addr};

pub struct CaptainActor {
    healthcheck_actors: HashMap<EntityId, Addr<BcActor<HealthchecksActor>>>,
    sync_actors: HashMap<EntityId, Addr<BcActor<SyncActor>>>,
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

    async fn build_actors<'a, A, M, IM, B, BR>(
        ctx: &BcContext<'_, Self>, models: IM, builder: B,
    ) -> HashMap<EntityId, Addr<A>>
    where
        BR: Future<Output = Result<A>>,
        B: Fn(&M) -> BR,
        IM: Iterator<Item = &'a M>,
        M: 'a + Entity + EntityStatic,
        A: Actor,
    {
        models
            .map(|m| {
                let m = m;
                let builder = &builder;
                async move {
                    let maybe_actor = builder(m).await;
                    match maybe_actor {
                        Ok(actor) => match actor.start().await {
                            Ok(started_actor) => Some((m.id(), started_actor)),
                            Err(error) => {
                                logged_error(
                                    ctx.log(),
                                    error.context(format!(
                                        "failed to start {} actor '{}'",
                                        M::entity_type_static(),
                                        m.name()
                                    )),
                                );
                                None
                            }
                        },
                        Err(error) => {
                            logged_error(
                                ctx.log(),
                                error.context(format!(
                                    "failed to create {} actor '{}",
                                    M::entity_type_static(),
                                    m.name()
                                )),
                            );
                            None
                        }
                    }
                }
            })
            .collect::<FuturesUnordered<_>>()
            .filter_map(future::ready)
            .collect::<HashMap<_, _>>()
            .await
    }

    async fn new_sync_actor(
        &self, entities: &Entities, model: SnapshotSyncEntity, log: &Logger,
    ) -> Result<BcActor<SyncActor>> {
        let dataset_pool_id = entities
            .dataset(model.dataset_id)
            .map(|p| p.parent.id())
            .context("source dataset does not exist")?;

        let dataset_pool = self
            .pool_actors
            .get(&dataset_pool_id)
            .context("source dataset's pool did not start")?;

        let dataset_actor = dataset_pool
            .call(GetChildActorMessage::new(model.dataset_id))
            .await?
            .context("source dataset did not start")?;

        let container_model = entities
            .any_container(model.container_id)
            .context("destination container does not exist")?;

        let to_container_actor = match container_model {
            AnyContainer::Btrfs(container_model) => {
                let container_pool = self
                    .pool_actors
                    .get(&container_model.parent())
                    .context("Destination container's pool didn't start.")?;
                let container_actor = container_pool
                    .call(GetChildActorMessage::new(model.container_id))
                    .await?
                    .context("destination btrfs container did not start")?;

                SyncToContainer::Btrfs(container_actor)
            }
            AnyContainer::Restic(container_model) => {
                let container_actor = self
                    .restic_actors
                    .get(&container_model.id())
                    .context("destination restic container did not start")?;

                SyncToContainer::Restic(container_actor.clone())
            }
        };

        Ok(SyncActor::new(dataset_actor, to_container_actor, model, log))
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for CaptainActor {
    async fn started(&mut self, ctx: BcContext<'_, Self>) -> Result<()> {
        create_data_dir()?;

        let entities = storage::load_entity_config();

        if !entities.observers.is_empty() {
            trace!(ctx.log(), "building observer actors");
            self.healthcheck_actors = CaptainActor::build_actors(&ctx, entities.observers.iter(), |m| {
                future::ok(HealthchecksActor::new(m.clone(), ctx.log()))
            })
            .await;
        };

        if !entities.btrfs_pools.is_empty() {
            trace!(ctx.log(), "building pool actors");
            self.pool_actors = CaptainActor::build_actors(&ctx, entities.btrfs_pools.iter(), |m| {
                future::ok(PoolActor::new(m.clone(), ctx.log()))
            })
            .await;
        }

        if !entities.restic_containers.is_empty() {
            trace!(ctx.log(), "building restic actors");
            self.restic_actors = CaptainActor::build_actors(&ctx, entities.restic_containers.iter(), |m| {
                future::ok(ResticContainerActor::new(m.clone(), ctx.log()))
            })
            .await;
        };

        if !entities.snapshot_syncs.is_empty() {
            trace!(ctx.log(), "building sync actors");
            self.sync_actors = CaptainActor::build_actors(&ctx, entities.snapshot_syncs.iter(), |m| {
                self.new_sync_actor(&entities, m.clone(), ctx.log())
            })
            .await;
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
        stop_all_actors(self.healthcheck_actors.values_mut());
        stop_all_actors(self.sync_actors.values_mut());
        stop_all_actors(self.pool_actors.values_mut());
        stop_all_actors(self.restic_actors.values_mut());

        join_all_actors(self.healthcheck_actors.drain().map(|(_k, v)| v)).await;
        join_all_actors(self.sync_actors.drain().map(|(_k, v)| v)).await;
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
        String::from("idle")
    }
}
