use std::{collections::VecDeque, time::Duration};

use super::{
    container::ContainerActor, container::GetContainerSnapshotsMessage, container::GetSnapshotReceiverMessage,
    dataset::DatasetActor, dataset::GetDatasetSnapshotsMessage, dataset::GetSnapshotSenderMessage,
    observation::ObservableEventMessage, transfer::TransferActor, transfer::TransferComplete,
};
use anyhow::Result;

use chrono::{DateTime, Utc};
use libblkcapt::{
    core::localsndrcv::{SnapshotReceiver, SnapshotSender},
    core::sync::find_parent,
    core::sync::find_ready,
    core::sync::FindMode,
    core::ObservableEventStage,
    model::entities::{ObservableEvent, SnapshotSyncEntity, SnapshotSyncMode},
};
use log::*;

use xactor::{message, Actor, Addr, Context, Handler};

pub struct SyncActor {
    dataset: Addr<DatasetActor>,
    container: Addr<ContainerActor>,
    model: SnapshotSyncEntity,

    state_mode: ModeState,
    state_active_send: Option<Addr<TransferActor>>,
}

enum ModeState {
    SendLatest(VecDeque<DateTime<Utc>>),
    SendAll(Option<DateTime<Utc>>),
}

#[message()]
#[derive(Clone)]
struct StartSnapshotSyncCycleMessage();

impl SyncActor {
    pub fn new(dataset: Addr<DatasetActor>, container: Addr<ContainerActor>, model: SnapshotSyncEntity) -> Self {
        trace!("Creating SyncActor for {:?}", model);
        Self {
            dataset,
            container,
            state_mode: match model.sync_mode {
                SnapshotSyncMode::SyncLatest => ModeState::SendLatest(VecDeque::<_>::default()),
                SnapshotSyncMode::SyncAll | SnapshotSyncMode::SyncImmediate => ModeState::SendAll(None),
            },
            state_active_send: None,
            model,
        }
    }

    async fn run_cycle(&mut self, ctx: &Context<Self>) -> Result<()> {
        let dataset = self.dataset.call(GetDatasetSnapshotsMessage()).await.unwrap();
        let container = self
            .container
            .call(GetContainerSnapshotsMessage {
                source_dataset_id: self.model.dataset_id(),
            })
            .await
            .unwrap();

        let to_send = match &mut self.state_mode {
            ModeState::SendLatest(queue) => queue
                .pop_front()
                .and_then(|limit| find_ready(&dataset.snapshots, &container.snapshots, FindMode::LatestBefore(limit))),
            ModeState::SendAll(limit) => limit.and_then(|limit| {
                find_ready(
                    &dataset.snapshots,
                    &container.snapshots,
                    FindMode::EarliestBefore(limit),
                )
            }),
        };

        let to_send = if let Some(handle) = to_send {
            handle
        } else {
            debug!("No snapshots to send this cycle.");
            return Ok(());
        };

        let parent = find_parent(to_send, &dataset.snapshots, &container.snapshots);

        let sender: SnapshotSender = self
            .dataset
            .call(GetSnapshotSenderMessage {
                send_snapshot_uuid: to_send.uuid,
                parent_snapshot_uuid: parent.map(|s| s.uuid),
            })
            .await
            .unwrap()
            .unwrap();

        let receiver: SnapshotReceiver = self
            .container
            .call(GetSnapshotReceiverMessage {
                source_dataset_id: self.model.dataset_id(),
            })
            .await
            .expect("can call")
            .expect("valid receiver");

        let transfer_actor = TransferActor::new(ctx.address().sender::<TransferComplete>(), sender, receiver);
        let transfer_actor = transfer_actor.start().await.unwrap();
        self.state_active_send = Some(transfer_actor);

        Ok(())
    }
}

#[async_trait::async_trait]
impl Actor for SyncActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        trace!("SYNC ACTOR STARTED");
        if self.model.sync_mode == SnapshotSyncMode::SyncImmediate {
            ctx.subscribe::<ObservableEventMessage>().await?;
        }
        ctx.send_later(StartSnapshotSyncCycleMessage(), Duration::from_secs(3)); // TEMPORARY
        Ok(())
    }

    async fn stopped(&mut self, ctx: &mut Context<Self>) {
        trace!("SYNC ACTOR STOPPED");
        if self.model.sync_mode == SnapshotSyncMode::SyncImmediate {
            ctx.unsubscribe::<ObservableEventMessage>().await.expect("FIXME");
        }
    }
}

#[async_trait::async_trait]
impl Handler<ObservableEventMessage> for SyncActor {
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: ObservableEventMessage) {
        if msg.source == self.model.dataset_id()
            && msg.event == ObservableEvent::DatasetSnapshot
            && msg.stage == ObservableEventStage::Succeeded
        {
            ctx.address().send(StartSnapshotSyncCycleMessage()).expect("FIXME");
        }
    }
}

#[async_trait::async_trait]
impl Handler<StartSnapshotSyncCycleMessage> for SyncActor {
    async fn handle(&mut self, ctx: &mut Context<Self>, _msg: StartSnapshotSyncCycleMessage) {
        trace!("Sync actor snapshot cycle message.");

        let new_limit_time = Utc::now();
        match &mut self.state_mode {
            ModeState::SendLatest(queue) => {
                trace!("Adding sync time {} to queue.", new_limit_time);
                queue.push_back(new_limit_time);
            }
            ModeState::SendAll(limit) => {
                trace!("Moving limit sync forward to {}", new_limit_time);
                limit.replace(new_limit_time);
            }
        }

        if self.state_active_send.is_some() {
            debug!("Received snapshot cycle message while in active send state.");
            return;
        }

        let result = self.run_cycle(ctx).await;

        if let Err(e) = result {
            error!("Failed to start sync cycle: {}", e);
        }
    }
}

#[async_trait::async_trait]
impl Handler<TransferComplete> for SyncActor {
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: TransferComplete) {
        trace!("Sync actor transfer complete message.");

        self.state_active_send = None;
        let TransferComplete(finished_receiver) = msg;
        info!("received: {:?}", finished_receiver.expect("FIXME").received_snapshot);

        let result = self.run_cycle(ctx).await;

        if let Err(e) = result {
            error!("Failed to start sync cycle: {}", e);
        }
    }
}
