use super::{
    container::ContainerActor, container::GetContainerSnapshotsMessage, container::GetSnapshotReceiverMessage,
    dataset::DatasetActor, dataset::GetDatasetSnapshotsMessage, dataset::GetSnapshotSenderMessage,
    observation::ObservableEventMessage, transfer::TransferActor, transfer::TransferComplete,
};
use crate::{
    actorbase::unhandled_result,
    xactorext::{BcActor, BcActorCtrl, BcHandler},
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
use slog::{debug, error, info, o, trace, Logger};
use std::{collections::VecDeque, time::Duration};
use xactor::{message, Actor, Addr, Context, Handler};

pub struct SyncActor {
    dataset: Addr<BcActor<DatasetActor>>,
    container: Addr<BcActor<ContainerActor>>,
    model: SnapshotSyncEntity,

    state_mode: ModeState,
    state_active_send: Option<Addr<BcActor<TransferActor>>>,
}

enum ModeState {
    SendLatest(VecDeque<DateTime<Utc>>),
    SendAll(Option<DateTime<Utc>>),
}

#[message()]
#[derive(Clone)]
struct StartSnapshotSyncCycleMessage();

impl SyncActor {
    pub fn new(
        dataset: Addr<BcActor<DatasetActor>>,
        container: Addr<BcActor<ContainerActor>>,
        model: SnapshotSyncEntity,
        log: &Logger,
    ) -> BcActor<Self> {
        let dataset_id = model.dataset_id();
        let container_id = model.container_id();
        BcActor::new(
            Self {
                dataset,
                container,
                state_mode: match model.sync_mode {
                    SnapshotSyncMode::SyncLatest => ModeState::SendLatest(VecDeque::<_>::default()),
                    SnapshotSyncMode::SyncAll | SnapshotSyncMode::SyncImmediate => ModeState::SendAll(None),
                },
                state_active_send: None,
                model,
            },
            &log.new(o!("dataset_id" => dataset_id.to_string(), "container_id" => container_id.to_string())),
        )
    }

    async fn run_cycle(&mut self, ctx: &Context<BcActor<Self>>, log: &Logger) -> Result<()> {
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
            debug!(log, "no snapshots ready to send");
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

        let transfer_actor = TransferActor::new(
            ctx.address().sender::<TransferComplete>(),
            sender,
            receiver,
            &log.new(o!("message" => ())),
        );
        let transfer_actor = transfer_actor.start().await.unwrap();
        self.state_active_send = Some(transfer_actor);

        Ok(())
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for SyncActor {
    async fn started(&mut self, _log: &Logger, ctx: &mut Context<BcActor<Self>>) -> Result<()> {
        if self.model.sync_mode == SnapshotSyncMode::SyncImmediate {
            ctx.subscribe::<ObservableEventMessage>().await?;
        }
        ctx.send_later(StartSnapshotSyncCycleMessage(), Duration::from_secs(3)); // TEMPORARY
        Ok(())
    }

    async fn stopped(&mut self, _log: &Logger, ctx: &mut Context<BcActor<Self>>) {
        if self.model.sync_mode == SnapshotSyncMode::SyncImmediate {
            ctx.unsubscribe::<ObservableEventMessage>().await.expect("FIXME");
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<ObservableEventMessage> for SyncActor {
    async fn handle(&mut self, _log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: ObservableEventMessage) {
        if msg.source == self.model.dataset_id()
            && msg.event == ObservableEvent::DatasetSnapshot
            && msg.stage == ObservableEventStage::Succeeded
        {
            ctx.address().send(StartSnapshotSyncCycleMessage()).expect("FIXME");
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<StartSnapshotSyncCycleMessage> for SyncActor {
    async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, _msg: StartSnapshotSyncCycleMessage) {
        let new_limit_time = Utc::now();
        match &mut self.state_mode {
            ModeState::SendLatest(queue) => {
                trace!(log, "adding sync time {} to queue", new_limit_time);
                queue.push_back(new_limit_time);
            }
            ModeState::SendAll(limit) => {
                trace!(log, "moving limit sync forward to {}", new_limit_time);
                limit.replace(new_limit_time);
            }
        }

        if self.state_active_send.is_some() {
            debug!(log, "received snapshot cycle message while in active send state");
            return;
        }

        let result = self.run_cycle(ctx, log).await;
        unhandled_result(log, result);
    }
}

#[async_trait::async_trait]
impl BcHandler<TransferComplete> for SyncActor {
    async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: TransferComplete) {
        self.state_active_send = None;
        let TransferComplete(finished_receiver) = msg;
        info!(
            log,
            "received: {:?}",
            finished_receiver.expect("FIXME").received_snapshot
        );

        let result = self.run_cycle(ctx, log).await;
        unhandled_result(log, result);
    }
}
