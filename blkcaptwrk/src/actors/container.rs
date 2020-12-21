use super::{
    localreceiver::{LocalReceiverActor, ReceiverFinishedMessage, ReceiverFinishedParentMessage},
    observation::observable_func,
    pool::PoolActor,
};
use crate::{
    actorbase::{schedule_next_message, unhandled_result},
    snapshots::{
        clear_deleted, delete_snapshots, failed_snapshot_deletes_as_result, log_evaluation, prune_btrfs_snapshots,
        ContainerSnapshotsResponse, GetContainerSnapshotsMessage, PruneMessage,
    },
    xactorext::{
        join_all_actors, stop_all_actors, BcActor, BcActorCtrl, BcHandler, GetActorStatusMessage, TerminalState,
    },
};
use anyhow::{anyhow, Result};
use cron::Schedule;
use futures_util::future::ready;
use libblkcapt::{
    core::retention::evaluate_retention,
    core::{BtrfsContainer, BtrfsContainerSnapshot, BtrfsPool, BtrfsSnapshot},
    core::{Snapshot, SnapshotHandle},
    model::entities::FeatureState,
    model::entities::{BtrfsContainerEntity, ObservableEvent},
    model::Entity,
};
use slog::{debug, o, trace, Logger};
use std::{collections::HashMap, convert::TryInto, sync::Arc};
use uuid::Uuid;
use xactor::{message, Actor, Addr, Context, Handler, Sender, WeakAddr};

pub struct ContainerActor {
    pool: Addr<BcActor<PoolActor>>,
    container: Arc<BtrfsContainer>,
    snapshots: HashMap<Uuid, Vec<BtrfsContainerSnapshot>>,
    prune_schedule: Option<Schedule>,
    active_receivers: HashMap<u64, ActiveReceiver>,
}

pub struct ActiveReceiver {
    actor: WeakAddr<BcActor<LocalReceiverActor>>,
    dataset_id: Uuid,
}

#[message(result = "Result<()>")]
pub struct GetSnapshotReceiverMessage {
    source_dataset_id: Uuid,
    source_snapshot_handle: SnapshotHandle,
    target_ready: Sender<ReceiverReadyMessage>,
    target_finished: Sender<ReceiverFinishedMessage>,
}

impl GetSnapshotReceiverMessage {
    pub fn new<A>(
        requestor_addr: &Addr<A>, source_dataset_id: Uuid, source_snapshot_handle: SnapshotHandle,
    ) -> GetSnapshotReceiverMessage
    where
        A: Handler<ReceiverReadyMessage> + Handler<ReceiverFinishedMessage>,
    {
        Self {
            source_dataset_id,
            source_snapshot_handle,
            target_ready: requestor_addr.sender(),
            target_finished: requestor_addr.sender(),
        }
    }
}

#[message()]
pub struct ReceiverReadyMessage(pub Result<Addr<BcActor<LocalReceiverActor>>>);

impl ContainerActor {
    pub fn new(
        pool_actor: Addr<BcActor<PoolActor>>, pool: &Arc<BtrfsPool>, model: BtrfsContainerEntity, log: &Logger,
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
                        active_receivers: Default::default(),
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

    async fn stopped(&mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>) -> TerminalState {
        let mut active_actors = self
            .active_receivers
            .drain()
            .filter_map(|(_, a)| a.actor.upgrade())
            .collect::<Vec<_>>();
        if !active_actors.is_empty() {
            stop_all_actors(&mut active_actors);
            join_all_actors(active_actors).await;
            TerminalState::Cancelled
        } else {
            TerminalState::Succeeded
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<GetContainerSnapshotsMessage> for ContainerActor {
    async fn handle(
        &mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>, msg: GetContainerSnapshotsMessage,
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
        &mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: GetSnapshotReceiverMessage,
    ) -> Result<()> {
        if self
            .container
            .snapshot_by_datetime(msg.source_dataset_id, msg.source_snapshot_handle.datetime)
            .is_ok()
        {
            anyhow::bail!(
                "receiver requested for existing snapshot dataset_id: {} snapshot_datetime: {}",
                msg.source_dataset_id,
                msg.source_snapshot_handle.datetime
            )
        }

        let snapshot_receiver = self.container.receive(msg.source_dataset_id);
        let started_receiver_actor = LocalReceiverActor::new(
            ctx.address().caller(),
            msg.target_finished,
            snapshot_receiver,
            &log.new(o!("message" => ())),
        )
        .start()
        .await;

        if let Ok(addr) = &started_receiver_actor {
            self.active_receivers.insert(
                addr.actor_id(),
                ActiveReceiver {
                    actor: addr.downgrade(),
                    dataset_id: msg.source_dataset_id,
                },
            );
        } else {
            return started_receiver_actor.map(|_| ());
        }

        msg.target_ready.send(ReceiverReadyMessage(started_receiver_actor))
    }
}

#[async_trait::async_trait]
impl BcHandler<ReceiverFinishedParentMessage> for ContainerActor {
    async fn handle(&mut self, log: &Logger, _ctx: &mut Context<BcActor<Self>>, msg: ReceiverFinishedParentMessage) {
        let ReceiverFinishedParentMessage(actor_id, new_snapshot) = msg;
        let active_receiver = self.active_receivers.remove(&actor_id).expect("FIXME");

        let new_snapshot = new_snapshot.unwrap();
        debug!(log, "container received snapshot {}", new_snapshot.datetime(); "received_uuid" => %new_snapshot.received_uuid());

        self.snapshots
            .get_mut(&active_receiver.dataset_id)
            .expect("FIXME")
            .push(new_snapshot);
    }
}

#[async_trait::async_trait]
impl BcHandler<PruneMessage> for ContainerActor {
    async fn handle(&mut self, log: &Logger, _ctx: &mut Context<BcActor<Self>>, _msg: PruneMessage) {
        let result = observable_func(self.container.model().id(), ObservableEvent::ContainerPrune, || {
            let rules = self
                .container
                .model()
                .snapshot_retention
                .as_ref()
                .expect("retention exist based on message scheduling in started");

            let failed_deletes = self.snapshots.iter_mut().fold(0, |acc, (dataset_id, snapshots)| {
                trace!(log, "prune container"; "dataset_id" => %dataset_id);
                acc + prune_btrfs_snapshots(snapshots, &vec![], rules, log)
            });
            ready(failed_snapshot_deletes_as_result(failed_deletes))
        })
        .await;

        unhandled_result(log, result);
    }
}

#[async_trait::async_trait]
impl BcHandler<GetActorStatusMessage> for ContainerActor {
    async fn handle(
        &mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>, _msg: GetActorStatusMessage,
    ) -> String {
        String::from("ok")
    }
}
