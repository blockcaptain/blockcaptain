use super::{
    localreceiver::{LocalReceiverActor, LocalReceiverStoppedMessage, LocalReceiverStoppedParentMessage},
    observation::observable_func,
    pool::PoolActor,
};
use crate::{
    actorbase::{log_result, unhandled_result, ScheduledMessage},
    snapshots::{
        failed_snapshot_deletes_as_result, prune_btrfs_snapshots, ContainerSnapshotsResponse,
        GetContainerSnapshotsMessage, PruneMessage,
    },
    xactorext::{
        join_all_actors, stop_all_actors, BcActor, BcActorCtrl, BcContext, BcHandler, GetActorStatusMessage,
        TerminalState,
    },
};
use anyhow::{Context as _, Result};
use futures_util::future::ready;
use libblkcapt::{
    core::{BtrfsContainer, BtrfsContainerSnapshot, BtrfsPool},
    core::{Snapshot, SnapshotHandle},
    model::entities::FeatureState,
    model::Entity,
    model::{
        entities::{BtrfsContainerEntity, ObservableEvent},
        EntityId,
    },
};
use slog::{debug, o, trace, Logger};
use std::{collections::HashMap, convert::TryInto, sync::Arc};
use xactor::{message, Actor, Addr, Handler, Sender, WeakAddr};

pub struct ContainerActor {
    pool: Addr<BcActor<PoolActor>>,
    container: Arc<BtrfsContainer>,
    snapshots: HashMap<EntityId, Vec<BtrfsContainerSnapshot>>,
    prune_schedule: Option<ScheduledMessage>,
    active_receivers: HashMap<u64, ActiveReceiver>,
    faulted: bool,
}

pub struct ActiveReceiver {
    actor: WeakAddr<BcActor<LocalReceiverActor>>,
    dataset_id: EntityId,
}

#[message(result = "Result<()>")]
pub struct GetSnapshotReceiverMessage {
    source_dataset_id: EntityId,
    source_snapshot_handle: SnapshotHandle,
    target_ready: Sender<ReceiverReadyMessage>,
    target_finished: Sender<LocalReceiverStoppedMessage>,
}

impl GetSnapshotReceiverMessage {
    pub fn new<A>(
        requestor_addr: &Addr<A>, source_dataset_id: EntityId, source_snapshot_handle: SnapshotHandle,
    ) -> GetSnapshotReceiverMessage
    where
        A: Handler<ReceiverReadyMessage> + Handler<LocalReceiverStoppedMessage>,
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
                        faulted: false,
                    },
                    &log.new(o!("container_id" => id.to_string())),
                ))
            })
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for ContainerActor {
    async fn started(&mut self, ctx: BcContext<'_, Self>) -> Result<()> {
        trace!(
            ctx.log(),
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
                .map_or(Ok(None), |s| {
                    s.try_into()
                        .map(|schedule| Some(ScheduledMessage::new(schedule, "prune", PruneMessage, &ctx)))
                })?;
        }

        Ok(())
    }

    async fn stopped(&mut self, _ctx: BcContext<'_, Self>) -> TerminalState {
        if self.faulted {
            return TerminalState::Faulted;
        }

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
        &mut self, _ctx: BcContext<'_, Self>, msg: GetContainerSnapshotsMessage,
    ) -> ContainerSnapshotsResponse {
        ContainerSnapshotsResponse {
            snapshots: self
                .snapshots
                .entry(msg.source_dataset_id)
                .or_default()
                .iter()
                .map(|s| s.into())
                .collect(),
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<GetSnapshotReceiverMessage> for ContainerActor {
    async fn handle(&mut self, ctx: BcContext<'_, Self>, msg: GetSnapshotReceiverMessage) -> Result<()> {
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

        let snapshot_receiver = self.container.receive(msg.source_dataset_id)?;
        let started_receiver_actor = LocalReceiverActor::new(
            ctx.address().sender(),
            msg.target_finished,
            snapshot_receiver,
            &ctx.log().new(o!("message" => ())),
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
impl BcHandler<LocalReceiverStoppedParentMessage> for ContainerActor {
    async fn handle(&mut self, ctx: BcContext<'_, Self>, msg: LocalReceiverStoppedParentMessage) {
        let LocalReceiverStoppedParentMessage(actor_id, maybe_snapshot_name) = msg;
        let active_receiver = match self.active_receivers.remove(&actor_id) {
            Some(active) => active,
            None => {
                self.faulted = true;
                ctx.stop(None);
                return;
            }
        };

        if let Some(new_snapshot_name) = maybe_snapshot_name {
            let sealed_snapshot = self
                .container
                .seal_snapshot(active_receiver.dataset_id, &new_snapshot_name)
                .with_context(|| format!("received snapshot {} but failed to seal it", new_snapshot_name));
            log_result(ctx.log(), &sealed_snapshot);
            if let Ok(new_snapshot) = sealed_snapshot {
                debug!(ctx.log(), "container received snapshot {}", new_snapshot.datetime(); "received_uuid" => %new_snapshot.received_uuid());

                self.snapshots
                    .entry(active_receiver.dataset_id)
                    .or_default()
                    .push(new_snapshot);
            }
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<PruneMessage> for ContainerActor {
    async fn handle(&mut self, ctx: BcContext<'_, Self>, _msg: PruneMessage) {
        let result = observable_func(self.container.model().id(), ObservableEvent::ContainerPrune, || {
            let rules = self
                .container
                .model()
                .snapshot_retention
                .as_ref()
                .expect("retention exist based on message scheduling in started");

            let failed_deletes = self.snapshots.iter_mut().fold(0, |acc, (dataset_id, snapshots)| {
                trace!(ctx.log(), "prune container"; "dataset_id" => %dataset_id);
                acc + prune_btrfs_snapshots(snapshots, &[], rules, ctx.log())
            });
            ready(failed_snapshot_deletes_as_result(failed_deletes))
        })
        .await;

        unhandled_result(ctx.log(), result);
    }
}

#[async_trait::async_trait]
impl BcHandler<GetActorStatusMessage> for ContainerActor {
    async fn handle(&mut self, _ctx: BcContext<'_, Self>, _msg: GetActorStatusMessage) -> String {
        if self.active_receivers.is_empty() {
            String::from("idle")
        } else {
            String::from("active")
        }
    }
}
