use super::pool::PoolActor;
use crate::{
    actorbase::unhandled_result,
    xactorext::{BcActor, BcActorCtrl, BcHandler},
};
use anyhow::Result;
use libblkcapt::{
    core::localsndrcv::SnapshotReceiver,
    core::{BtrfsContainer, BtrfsContainerSnapshot, BtrfsContainerSnapshotHandle, BtrfsPool},
    model::entities::BtrfsContainerEntity,
    model::Entity,
};
use slog::{o, trace, Logger};
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;
use xactor::{message, Actor, Addr, Context, Handler};

pub struct ContainerActor {
    pool: Addr<BcActor<PoolActor>>,
    container: Arc<BtrfsContainer>,
    snapshots: HashMap<Uuid, Vec<BtrfsContainerSnapshot>>,
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
                    },
                    &log.new(o!("container_id" => id.to_string())),
                ))
            })
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for ContainerActor {
    async fn started(&mut self, log: &Logger, _ctx: &mut Context<BcActor<Self>>) -> Result<()> {
        trace!(
            log,
            "Starting container with {} snapshots from {} datasets.",
            self.snapshots.values().fold(0, |acc, v| acc + v.len()),
            self.snapshots.len()
        );
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

// else if let Ok(container) = self.container(msg.dataset_or_container_id) {
//     let rules = container
//         .model()
//         .snapshot_retention
//         .as_ref()
//         .expect("Exists based on actor start.");
//     observable_func(container.model().id(), ObservableEvent::ContainerPrune, move || {
//         let result = container.source_dataset_ids().and_then(|ids| {
//             ids.iter()
//                 .map(|id| {
//                     trace!("Running prune for dataset id {} in container {}.", id, container);
//                     container
//                         .snapshots(*id)
//                         .and_then(|snapshots| evaluate_retention(snapshots, rules))
//                         .and_then(prune_snapshots)
//                 })
//                 .collect::<Result<()>>()
//         });
//         ready(result)
//     })
//     .await
