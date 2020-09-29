use super::observation::HealthchecksActor;
use super::pool::PoolActor;
use anyhow::Result;
use futures_util::{
    future::join_all,
    stream::{FuturesUnordered, TryStreamExt},
};
use libblkcapt::model::storage;
use log::*;
use std::iter::FromIterator;
use xactor::{Actor, Addr, Context};

#[derive(Default)]
pub struct CaptainActor {
    healthcheck_actors: Vec<Addr<HealthchecksActor>>,
    pool_actors: Vec<Addr<PoolActor>>,
}

#[async_trait::async_trait]
impl Actor for CaptainActor {
    async fn started(&mut self, _ctx: &mut Context<Self>) -> Result<()> {
        let mut entities = storage::load_entity_state();
        if !entities.observers.is_empty() {
            trace!("Building observer actors.");
            self.healthcheck_actors =
                FuturesUnordered::from_iter(entities.observers.drain(0..).map(|e| HealthchecksActor::new(e).start()))
                    .try_collect()
                    .await?;
        };

        if !entities.btrfs_pools.is_empty() {
            trace!("Building pool actors.");
            self.pool_actors = FuturesUnordered::from_iter(
                entities
                    .btrfs_pools
                    .drain(0..)
                    .map(|e| PoolActor::new(e).expect("FIXME").start()),
            )
            .try_collect()
            .await?;
        }

        // setup syncs somehow. maybe message the pools with addr of where they need to send to.
        // for sync in model.snapshot_syncs() {
        //     let sync_dataset = datasets
        //         .iter()
        //         .find(|d| d.model().id() == sync.dataset_id())
        //         .expect("FIXME");
        //     let sync_container = containers
        //         .iter()
        //         .find(|d| d.model().id() == sync.container_id())
        //         .expect("FIXME");
        //     jobs.push(Box::new(LocalSyncJob::new(sync_dataset, sync_container)));
        // }

        info!("Captain actor started successfully.");
        Ok(())
    }

    async fn stopped(&mut self, _ctx: &mut Context<Self>) {
        for actor in self.healthcheck_actors.iter_mut() {
            actor
                .stop(None)
                .unwrap_or_else(|e| error!("Stopping Healthchecks actor failed: {}.", e));
        }

        join_all(self.healthcheck_actors.drain(0..).map(|a| a.wait_for_stop())).await;

        info!("Captain stopped successfully.");
    }
}
