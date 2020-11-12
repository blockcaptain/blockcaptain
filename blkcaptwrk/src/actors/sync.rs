use super::{
    container::ContainerActor, container::GetContainerSnapshotsMessage, container::GetSnapshotReceiverMessage,
    dataset::DatasetActor, dataset::GetDatasetSnapshotsMessage, dataset::GetSnapshotSenderMessage,
    observation::ObservableEventMessage, transfer::TransferActor, transfer::TransferComplete,
};
use crate::{
    actorbase::{schedule_next_message, unhandled_result},
    xactorext::{BcActor, BcActorCtrl, BcHandler},
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use cron::Schedule;
use libblkcapt::{
    core::sync::find_parent,
    core::sync::find_ready,
    core::sync::FindMode,
    core::ObservableEventStage,
    model::entities::{ObservableEvent, SnapshotSyncEntity, SnapshotSyncMode},
};
use slog::{debug, o, trace, Logger};
use std::{collections::VecDeque, convert::TryInto, time::Duration};
use xactor::{message, Actor, Addr, Context};

pub struct SyncActor {
    dataset: Addr<BcActor<DatasetActor>>,
    container: Addr<BcActor<ContainerActor>>,
    model: SnapshotSyncEntity,

    state_mode: SyncModeState,
    state_active_send: Option<(Addr<BcActor<TransferActor>>, DateTime<Utc>)>,
    last_sent: Option<DateTime<Utc>>,
    sync_cycle_schedule: Option<Schedule>,
}

enum SyncModeState {
    LatestScheduled(VecDeque<DateTime<Utc>>),
    AllScheduled(Option<DateTime<Utc>>),
    AllImmediate,
    LatestImmediate(VecDeque<DateTime<Utc>>, Duration),
}

fn is_immediate(mode: &SnapshotSyncMode) -> bool {
    matches!(
        mode,
        SnapshotSyncMode::AllImmediate | SnapshotSyncMode::IntervalImmediate(..)
    )
}

fn get_schedule(mode: &SnapshotSyncMode) -> Option<Result<Schedule>> {
    match mode {
        SnapshotSyncMode::AllScheduled(model) | SnapshotSyncMode::LatestScheduled(model) => Some(model.try_into()),
        _ => None,
    }
}

#[message()]
#[derive(Clone)]
struct StartSnapshotSyncCycleMessage;

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
                    SnapshotSyncMode::AllScheduled(..) => SyncModeState::AllScheduled(None),
                    SnapshotSyncMode::LatestScheduled(..) => SyncModeState::LatestScheduled(Default::default()),
                    SnapshotSyncMode::AllImmediate => SyncModeState::AllImmediate,
                    SnapshotSyncMode::IntervalImmediate(interval) => {
                        SyncModeState::LatestImmediate(Default::default(), interval)
                    }
                },
                state_active_send: None,
                sync_cycle_schedule: None,
                last_sent: None,
                model,
            },
            &log.new(o!("dataset_id" => dataset_id.to_string(), "container_id" => container_id.to_string())),
        )
    }

    async fn run_cycle<'a>(&'a mut self, ctx: &Context<BcActor<Self>>, log: &Logger) -> Result<()> {
        let dataset = self.dataset.call(GetDatasetSnapshotsMessage()).await.unwrap();
        let container = self
            .container
            .call(GetContainerSnapshotsMessage {
                source_dataset_id: self.model.dataset_id(),
            })
            .await
            .unwrap();

        // trace!(log, "CONTAINER CONTENT");
        // for snapshot in &container.snapshots {
        //     trace!(log, "SNAPSHOT IN CONTAINER: {:?}", snapshot);
        // }

        let to_send = match &mut self.state_mode {
            SyncModeState::LatestScheduled(queue) | SyncModeState::LatestImmediate(queue, _) => queue
                .pop_front()
                .and_then(|limit| find_ready(&dataset.snapshots, &container.snapshots, FindMode::LatestBefore(limit))),
            SyncModeState::AllScheduled(limit) => limit.and_then(|limit| {
                find_ready(
                    &dataset.snapshots,
                    &container.snapshots,
                    FindMode::EarliestBefore(limit),
                )
            }),
            SyncModeState::AllImmediate => find_ready(&dataset.snapshots, &container.snapshots, FindMode::Earliest),
        };

        let to_send = if let Some(handle) = to_send {
            handle
        } else {
            debug!(log, "no snapshots ready to send");
            return Ok(());
        };

        let parent = find_parent(to_send, &dataset.snapshots, &container.snapshots);

        let transfer_actor = TransferActor::new(
            ctx.address().sender::<TransferComplete>(),
            &log.new(o!("message" => ())),
        );

        let transfer_actor = transfer_actor.start().await.unwrap();
        let sender_ready_sender = transfer_actor.sender();
        let sender_finished_sender = transfer_actor.sender();

        self.dataset
            .call(GetSnapshotSenderMessage {
                send_snapshot_handle: to_send.clone(),
                parent_snapshot_handle: parent.cloned(),
                target_ready: sender_ready_sender,
                target_finished: sender_finished_sender,
            })
            .await
            .unwrap()?;

        self.container
            .call(GetSnapshotReceiverMessage::new(
                &transfer_actor,
                self.model.dataset_id(),
                to_send.clone(),
            ))
            .await
            .unwrap()?;

        self.state_active_send = Some((transfer_actor, to_send.datetime));

        Ok(())
    }

    fn schedule_next_cycle(&self, log: &Logger, ctx: &mut Context<BcActor<Self>>) {
        if self.sync_cycle_schedule.is_some() {
            schedule_next_message(
                self.sync_cycle_schedule.as_ref(),
                "sync_cycle",
                StartSnapshotSyncCycleMessage,
                log,
                ctx,
            );
        }
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for SyncActor {
    async fn started(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>) -> Result<()> {
        if is_immediate(&self.model.sync_mode) {
            ctx.subscribe::<ObservableEventMessage>().await?;
        }

        self.sync_cycle_schedule = get_schedule(&self.model.sync_mode).map_or(Ok(None), |result| result.map(Some))?;
        self.schedule_next_cycle(log, ctx);

        if matches!(self.model.sync_mode, SnapshotSyncMode::IntervalImmediate(..)) {
            self.last_sent = self
                .container
                .call(GetContainerSnapshotsMessage {
                    source_dataset_id: self.model.dataset_id(),
                })
                .await?
                .snapshots
                .last()
                .map(|s| s.datetime);
        }

        Ok(())
    }

    async fn stopped(&mut self, _log: &Logger, ctx: &mut Context<BcActor<Self>>) {
        if is_immediate(&self.model.sync_mode) {
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
            ctx.address().send(StartSnapshotSyncCycleMessage).expect("FIXME");
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<StartSnapshotSyncCycleMessage> for SyncActor {
    async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, _msg: StartSnapshotSyncCycleMessage) {
        let new_limit_time = Utc::now();
        match &mut self.state_mode {
            SyncModeState::LatestScheduled(queue) => {
                trace!(log, "adding sync time {} to queue", new_limit_time);
                queue.push_back(new_limit_time);
            }
            SyncModeState::AllScheduled(limit) => {
                trace!(log, "moving limit sync forward to {}", new_limit_time);
                limit.replace(new_limit_time);
            }
            SyncModeState::AllImmediate => {
                trace!(log, "syncing all immediately");
            }
            SyncModeState::LatestImmediate(queue, interval) => {
                if self.last_sent.is_none()
                    || new_limit_time - self.last_sent.unwrap() > chrono::Duration::from_std(*interval).unwrap()
                {
                    trace!(log, "adding sync time {} to queue", new_limit_time);
                    queue.push_back(new_limit_time);
                } else {
                    trace!(log, "sync interval not yet elapsed");
                }
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
        if let Some((_, sent_snapshot_datetime)) = self.state_active_send {
            self.last_sent = Some(sent_snapshot_datetime);
            self.state_active_send = None;
        }
        unhandled_result(log, msg.0);

        //let TransferComplete(finished_receiver) = msg;
        // info!(
        //     log,
        //     "received: {:?}",
        //     finished_receiver.expect("FIXME").received_snapshot
        // );

        let result = self.run_cycle(ctx, log).await;
        unhandled_result(log, result);
    }
}
