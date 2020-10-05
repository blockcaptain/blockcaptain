use super::pool::PoolActor;
use super::{observation::HealthchecksActor, sync::SyncActor};
use crate::xactorext::{join_all_actors, stop_all_actors};
use anyhow::{Context as AnyhowContext, Result};
use futures_util::{
    future::join_all,
    stream::{FuturesOrdered, FuturesUnordered, StreamExt, TryStreamExt},
};
use libblkcapt::model::{storage, Entity};
use log::*;
use std::{collections::HashMap, future::Future, iter::FromIterator, mem};
use uuid::Uuid;
use xactor::{Actor, Addr, Context};

#[derive(Default)]
pub struct CaptainActor {
    healthcheck_actors: Vec<Addr<HealthchecksActor>>,
    sync_actors: Vec<Addr<SyncActor>>,
    pool_actors: HashMap<Uuid, Addr<PoolActor>>,
}

#[async_trait::async_trait]
impl Actor for CaptainActor {
    async fn started(&mut self, _ctx: &mut Context<Self>) -> Result<()> {
        let mut entities = storage::load_entity_state();
        if !entities.observers.is_empty() {
            trace!("Building observer actors.");
            self.healthcheck_actors = entities
                .observers
                .drain(..)
                .map(|m| HealthchecksActor::new(m).start())
                .collect::<FuturesUnordered<_>>()
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .filter_map(|sa| match sa {
                    Ok(started_actor) => Some(started_actor),
                    Err(error) => {
                        error!("Failed to start observer actor: {}", error);
                        None
                    }
                })
                .collect();
        };

        if !entities.btrfs_pools.is_empty() {
            trace!("Building pool actors.");
            self.pool_actors = entities
                .btrfs_pools
                .iter()
                .map(|m| PoolActor::new(m.clone()))
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
                        error!("Failed to start pool actor: {}", error);
                        None
                    }
                })
                .collect();
        }

        if !entities.snapshot_syncs.is_empty() {
            let x = mem::take(&mut entities.snapshot_syncs)
                .into_iter()
                .filter_map(|m| {
                    let dataset_pool_id = match entities.dataset(m.dataset_id()) {
                        Some(entity_path) => entity_path.parent.id(),
                        None => {
                            error!("Invalid sync configuration. Source dataset does not exist.");
                            return None;
                        }
                    };
                    let container_pool_id = match entities.container(m.container_id()) {
                        Some(entity_path) => entity_path.parent.id(),
                        None => {
                            error!("Invalid sync configuration. Destination container does not exist.");
                            return None;
                        }
                    };
                    // look up the pool actor
                    // ask the pool actor for the dataset actor
                    // create a return the new sync actor

                    Some(1)
                });
        }
        
        
            //     entities
            //         .dataset(sync.dataset_id())
            //         .context("")?;
            //     let sync_container = entities
            //         .container(sync.container_id())
            //         .context("Invalid sync configuration. does not exist.")?;

            //     let pool_addr = self
            //         .pool_actors
            //         .get(&sync_container.parent.id())
            //         .expect("INVARIANT: Actors for all pools are created above. If container is found, pool must exist.");

      
        

        info!("Captain actor started successfully.");
        Ok(())
    }

    async fn stopped(&mut self, _ctx: &mut Context<Self>) {
        stop_all_actors(&mut self.healthcheck_actors);
        stop_all_actors(&mut self.sync_actors);
        stop_all_actors(self.pool_actors.values_mut());

        join_all_actors(self.healthcheck_actors.drain(..)).await;
        join_all_actors(self.sync_actors.drain(..)).await;
        join_all_actors(self.pool_actors.drain().map(|(_k, v)| v)).await;

        info!("Captain stopped successfully.");
    }
}
