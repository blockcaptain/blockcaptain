use super::{observation::observable_func, pool::PoolActor};
use crate::{actorbase::{schedule_next_message, unhandled_result}, snapshots::{prune_snapshots, PruneMessage}, xactorext::{BcActor, BcActorCtrl, BcHandler}};
use anyhow::Result;
use cron::Schedule;
use futures_util::future::ready;
use libblkcapt::{model::entities::FeatureState, core::localsndrcv::SnapshotReceiver, core::retention::evaluate_retention, core::{BtrfsContainer, BtrfsContainerSnapshot, BtrfsContainerSnapshotHandle, BtrfsPool}, model::Entity, model::entities::{BtrfsContainerEntity, ObservableEvent}};
use slog::{o, trace, Logger};
use std::{collections::HashMap, sync::Arc, convert::TryInto};
use uuid::Uuid;
use xactor::{message, Actor, Addr, Context, Handler};

pub struct ContainerActor {
    pool: Addr<BcActor<PoolActor>>,
    container: Arc<BtrfsContainer>,
    snapshots: HashMap<Uuid, Vec<BtrfsContainerSnapshot>>,
    prune_schedule: Option<Schedule>,
}

#[message(result = "ContainerSnapshotsResponse")]
pub struct GetContainerSnapshotsMessage {
    pub source_dataset_id: Uuid,
}

pub struct ContainerSnapshotsResponse {
    pub snapshots: Vec<BtrfsContainerSnapshotHandle>,
}

#[message(result = "Result<SnapshotReceiver>")]
pub struct GetSnapshotReceiverMessage {
    pub source_dataset_id: Uuid,
}

impl ContainerActor {
    pub fn new(
        pool_actor: Addr<BcActor<PoolActor>>,
        pool: &Arc<BtrfsPool>,
        model: BtrfsContainerEntity,
        log: &Logger,
    ) -> Result<BcActor<Self>> {
        let id = model.id();
        BtrfsContainer::validate(pool, model)
            .map(Arc::new)
            .and_then(|container| {
                let source_ids = container.source_dataset_ids()?;
                Ok(BcActor::new(
                    Self {
                        pool: pool_actor,
                        snapshots: source_ids
                            .iter()
                            .map(|&source_id| container.snapshots(source_id).map(|snapshots| (source_id, snapshots)))
                            .collect::<Result<_>>()?,
                        container,
                        prune_schedule: None,
                    },
                    &log.new(o!("container_id" => id.to_string())),
                ))
            })
    }

    fn schedule_next_prune(&self, log: &Logger, ctx: &mut Context<BcActor<Self>>) {
        schedule_next_message(self.prune_schedule.as_ref(), "prune", PruneMessage(), log, ctx);
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for ContainerActor {
    async fn started(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>) -> Result<()> {
        trace!(
            log,
            "Starting container with {} snapshots from {} datasets.",
            self.snapshots.values().fold(0, |acc, v| acc + v.len()),
            self.snapshots.len()
        );

        if self.container.model().pruning_state() == FeatureState::Enabled {
            self.prune_schedule = self
                .container
                .model()
                .snapshot_retention
                .as_ref()
                .map(|r| &r.evaluation_schedule)
                .map_or(Ok(None), |s| s.try_into().map(Some))?;

            self.schedule_next_prune(log, ctx);
        }

        Ok(())
    }

    async fn stopped(&mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>) {}
}

#[async_trait::async_trait]
impl BcHandler<GetContainerSnapshotsMessage> for ContainerActor {
    async fn handle(
        &mut self,
        _log: &Logger,
        _ctx: &mut Context<BcActor<Self>>,
        msg: GetContainerSnapshotsMessage,
    ) -> ContainerSnapshotsResponse {
        ContainerSnapshotsResponse {
            snapshots: self.snapshots[&msg.source_dataset_id]
                .iter()
                .map(|s| s.into())
                .collect(),
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<GetSnapshotReceiverMessage> for ContainerActor {
    async fn handle(
        &mut self,
        _log: &Logger,
        _ctx: &mut Context<BcActor<Self>>,
        msg: GetSnapshotReceiverMessage,
    ) -> Result<SnapshotReceiver> {
        Ok(self.container.receive(msg.source_dataset_id))
    }
}

#[async_trait::async_trait]
impl BcHandler<PruneMessage> for ContainerActor {
    async fn handle(&mut self, log: &Logger, _ctx: &mut Context<BcActor<Self>>, _msg: PruneMessage) {
        let rules = self
            .container
            .model()
            .snapshot_retention
            .as_ref()
            .expect("retention exist based on message scheduling in started");

        let result = observable_func(self.container.model().id(), ObservableEvent::ContainerPrune, || {
            let result = self.container.source_dataset_ids().and_then(|ids| {
                ids.iter()
                    .map(|id| {
                        trace!(log, "prune container"; "dataset_id" => %id);
                        self.container
                            .snapshots(*id)
                            .and_then(|snapshots| evaluate_retention(snapshots, rules))
                            .and_then(|eval| prune_snapshots(eval, &log))
                    })
                    .collect::<Result<()>>()
            });
            ready(result)
        })
        .await;

        unhandled_result(log, result);
    }
}
