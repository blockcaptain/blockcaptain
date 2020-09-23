use super::jobdispatch::JobDispatchActor;
use super::observation::HealthchecksActor;
use anyhow::Result;
use futures_util::{
    future::join_all,
    stream::{FuturesUnordered, TryStreamExt},
};
use libblkcapt::model::storage;
use log::*;
use std::iter::FromIterator;
use std::mem;
use xactor::{Actor, Addr, Context};

#[derive(Default)]
pub struct CaptainActor {
    healthcheck_actors: Vec<Addr<HealthchecksActor>>,
    jobdispatch_actor: Option<Addr<JobDispatchActor>>,
}

#[async_trait::async_trait]
impl Actor for CaptainActor {
    async fn started(&mut self, _ctx: &mut Context<Self>) -> Result<()> {
        let mut entities = storage::load_entity_state();
        if !entities.observers.is_empty() {
            let observer_entities: Vec<_> = mem::take(entities.observers.as_mut());
            self.healthcheck_actors =
                FuturesUnordered::from_iter(observer_entities.into_iter().map(|e| HealthchecksActor::new(e).start()))
                    .try_collect()
                    .await?;
        };
        let entities = entities;

        self.jobdispatch_actor = Some(JobDispatchActor::new(entities)?.start().await?);

        info!("Captain actor started successfully.");
        Ok(())
    }

    async fn stopped(&mut self, _ctx: &mut Context<Self>) {
        for actor in self.healthcheck_actors.iter_mut() {
            actor
                .stop(None)
                .unwrap_or_else(|e| error!("Stopping Healthchecks actor failed: {}.", e));
        }

        self.jobdispatch_actor
            .as_mut()
            .unwrap()
            .stop(None)
            .unwrap_or_else(|e| error!("Stopping Job Dispatch actor failed: {}.", e));

        join_all(self.healthcheck_actors.drain(0..).map(|a| a.wait_for_stop())).await;
        self.jobdispatch_actor.take().unwrap().wait_for_stop().await;

        info!("Captain stopped successfully.");
    }
}
