use super::{
    container::ContainerActor,
    container::GetSnapshotReceiverMessage,
    dataset::DatasetActor,
    dataset::GetDatasetSnapshotsMessage,
    dataset::{GetSnapshotHolderMessage, GetSnapshotSenderMessage},
    observation::{start_observation, ObservableEventMessage, StartedObservation},
    restic::GetBackupMessage,
    restic::{ResticContainerActor, ResticTransferActor},
    transfer::TransferActor,
    transfer::TransferComplete,
};
use crate::{
    actorbase::{unhandled_result, ScheduledMessage},
    snapshots::{find_parent, find_ready, FindMode, GetContainerSnapshotsMessage},
    xactorext::BoxBcAddr,
    xactorext::{BcActor, BcActorCtrl, BcContext, BcHandler, GetActorStatusMessage, TerminalState},
};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use cron::Schedule;
use libblkcapt::{
    core::{ObservableEventStage, SnapshotHandle},
    model::{
        entities::{ObservableEvent, SnapshotSyncEntity, SnapshotSyncMode},
        Entity,
    },
};
use slog::{debug, o, trace, Logger};
use std::{collections::VecDeque, convert::TryInto, time::Duration};
use xactor::{message, Actor, Addr, Handler};

pub struct SyncActor {
    dataset: Addr<BcActor<DatasetActor>>,
    container: SyncToContainer,
    model: SnapshotSyncEntity,

    state_mode: SyncModeState,
    state_active_send: Option<ActiveSend>,
    last_sent: Option<DateTime<Utc>>,
    sync_cycle_schedule: Option<ScheduledMessage>,
}

struct ActiveSend {
    actor: BoxBcAddr,
    sending_snapshot: DateTime<Utc>,
    active_limit: Option<DateTime<Utc>>,
}

pub enum SyncToContainer {
    Btrfs(Addr<BcActor<ContainerActor>>),
    Restic(Addr<BcActor<ResticContainerActor>>),
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

#[message()]
struct RetrySnapshotSyncCycleMessage;

impl SyncActor {
    pub fn new(
        dataset: Addr<BcActor<DatasetActor>>, container: SyncToContainer, model: SnapshotSyncEntity, log: &Logger,
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

    async fn run_cycle(&mut self, ctx: &BcContext<'_, Self>) -> Result<()> {
        let dataset_snapshots = self.get_dataset_snapshots().await?;
        let container_snapshots = self.get_container_snapshots().await?;

        let observation = start_observation(self.model.id(), ObservableEvent::SnapshotSync).await;
        let mut active_limit = None;
        let to_send = match &mut self.state_mode {
            SyncModeState::LatestScheduled(queue) | SyncModeState::LatestImmediate(queue, _) => {
                active_limit = queue.pop_front();
                active_limit.and_then(|limit| {
                    find_ready(&dataset_snapshots, &container_snapshots, FindMode::LatestBefore(limit))
                })
            }
            SyncModeState::AllScheduled(ref limit) => limit.and_then(|limit| {
                find_ready(
                    &dataset_snapshots,
                    &container_snapshots,
                    FindMode::EarliestBefore(limit),
                )
            }),
            SyncModeState::AllImmediate => find_ready(&dataset_snapshots, &container_snapshots, FindMode::Earliest),
        };

        let to_send = if let Some(handle) = to_send {
            if let Some(last_datetime) = self.last_sent {
                if handle.datetime == last_datetime {
                    let result = Err(anyhow!(
                        "sanity check failed: snapshot {} was successfully sent, but was selected to be sent again",
                        last_datetime
                    ));
                    observation.result(&result);
                    return result;
                }
            }
            handle
        } else {
            debug!(ctx.log(), "no snapshots ready to send");
            observation.succeeded();
            return Ok(());
        };

        let parent = find_parent(to_send, &dataset_snapshots, &container_snapshots);

        let actor = self.start_transfer_actor(to_send, parent, observation, &ctx).await?;
        self.state_active_send = Some(ActiveSend {
            actor,
            sending_snapshot: to_send.datetime,
            active_limit,
        });
        Ok(())
    }

    async fn get_container_snapshots(&self) -> Result<Vec<SnapshotHandle>> {
        match &self.container {
            SyncToContainer::Btrfs(c) => self._get_container_snapshots(c).await,
            SyncToContainer::Restic(c) => self._get_container_snapshots(c).await,
        }
    }

    async fn _get_container_snapshots<T: Handler<GetContainerSnapshotsMessage>>(
        &self, addr: &Addr<T>,
    ) -> Result<Vec<SnapshotHandle>> {
        addr.call(GetContainerSnapshotsMessage {
            source_dataset_id: self.model.dataset_id(),
        })
        .await
        .map(|r| r.snapshots)
    }

    async fn get_dataset_snapshots(&self) -> Result<Vec<SnapshotHandle>> {
        self.dataset.call(GetDatasetSnapshotsMessage).await.map(|r| r.snapshots)
    }

    async fn start_transfer_actor(
        &self, snapshot: &SnapshotHandle, parent: Option<&SnapshotHandle>, observation: StartedObservation,
        ctx: &BcContext<'_, Self>,
    ) -> Result<BoxBcAddr> {
        match &self.container {
            SyncToContainer::Btrfs(container) => {
                let transfer_actor = TransferActor::new(
                    ctx.address().sender::<TransferComplete>(),
                    observation,
                    &ctx.log().new(o!("message" => ())),
                );

                let transfer_actor = transfer_actor.start().await?;

                self.dataset
                    .call(GetSnapshotSenderMessage::new(
                        &transfer_actor,
                        snapshot.clone(),
                        parent.cloned(),
                    ))
                    .await??;

                container
                    .call(GetSnapshotReceiverMessage::new(
                        &transfer_actor,
                        self.model.dataset_id(),
                        snapshot.clone(),
                    ))
                    .await??;

                Ok(transfer_actor.into())
            }
            SyncToContainer::Restic(container) => {
                let transfer_actor = ResticTransferActor::new(
                    ctx.address().sender::<TransferComplete>(),
                    container.clone(),
                    observation,
                    &ctx.log().new(o!("message" => ())),
                );

                let transfer_actor = transfer_actor.start().await?;

                self.dataset
                    .call(GetSnapshotHolderMessage::new(
                        &transfer_actor,
                        snapshot.clone(),
                        parent.cloned(),
                    ))
                    .await??;

                container
                    .call(GetBackupMessage::new(
                        &transfer_actor,
                        self.model.dataset_id(),
                        snapshot.clone(),
                    ))
                    .await??;

                Ok(transfer_actor.into())
            }
        }
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for SyncActor {
    async fn started(&mut self, ctx: BcContext<'_, Self>) -> Result<()> {
        if is_immediate(&self.model.sync_mode) {
            ctx.subscribe::<ObservableEventMessage>().await?;
        }

        self.sync_cycle_schedule = get_schedule(&self.model.sync_mode).map_or(Ok(None), |s| {
            s.map(|schedule| {
                Some(ScheduledMessage::new(
                    schedule,
                    "sync_cycle",
                    StartSnapshotSyncCycleMessage,
                    &ctx,
                ))
            })
        })?;

        if matches!(self.model.sync_mode, SnapshotSyncMode::IntervalImmediate(..)) {
            self.last_sent = self.get_container_snapshots().await?.last().map(|s| s.datetime);
        }

        Ok(())
    }

    async fn stopped(&mut self, ctx: BcContext<'_, Self>) -> TerminalState {
        if is_immediate(&self.model.sync_mode) {
            let _ = ctx.unsubscribe::<ObservableEventMessage>().await;
        }

        if let Some(ActiveSend { mut actor, .. }) = self.state_active_send.take() {
            let _ = actor.stop();
            actor.wait_for_stop().await;
            TerminalState::Cancelled
        } else {
            TerminalState::Succeeded
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<ObservableEventMessage> for SyncActor {
    async fn handle(&mut self, ctx: BcContext<'_, Self>, msg: ObservableEventMessage) {
        if msg.source == self.model.dataset_id()
            && msg.event == ObservableEvent::DatasetSnapshot
            && msg.stage == ObservableEventStage::Succeeded
        {
            ctx.address()
                .send(StartSnapshotSyncCycleMessage)
                .expect("send to self is infalliable");
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<StartSnapshotSyncCycleMessage> for SyncActor {
    async fn handle(&mut self, ctx: BcContext<'_, Self>, _msg: StartSnapshotSyncCycleMessage) {
        let new_limit_time = Utc::now();
        match &mut self.state_mode {
            SyncModeState::LatestScheduled(queue) => {
                trace!(ctx.log(), "adding sync time {} to queue", new_limit_time);
                queue.push_back(new_limit_time);
            }
            SyncModeState::AllScheduled(limit) => {
                trace!(ctx.log(), "moving limit sync forward to {}", new_limit_time);
                limit.replace(new_limit_time);
            }
            SyncModeState::AllImmediate => {
                trace!(ctx.log(), "syncing all immediately");
            }
            SyncModeState::LatestImmediate(queue, interval) => {
                if self.last_sent.is_none()
                    || new_limit_time - self.last_sent.expect("always exists, validated earlier in expr")
                        > chrono::Duration::from_std(*interval).expect("interval always fits in chrono duration")
                {
                    trace!(ctx.log(), "adding sync time {} to queue", new_limit_time);
                    queue.push_back(new_limit_time);
                } else {
                    trace!(ctx.log(), "sync interval not yet elapsed");
                }
            }
        }

        if self.state_active_send.is_some() {
            debug!(ctx.log(), "received snapshot cycle message while in active send state");
            return;
        }

        let result = self.run_cycle(&ctx).await;
        unhandled_result(ctx.log(), result);
    }
}

#[async_trait::async_trait]
impl BcHandler<TransferComplete> for SyncActor {
    async fn handle(&mut self, ctx: BcContext<'_, Self>, msg: TransferComplete) {
        let transfer = msg.0;
        if let Some(ActiveSend {
            sending_snapshot,
            active_limit,
            ..
        }) = self.state_active_send.take()
        {
            if transfer.succeeded() {
                self.last_sent = Some(sending_snapshot);
            } else if let Some(active_limit) = active_limit {
                match &mut self.state_mode {
                    SyncModeState::LatestScheduled(queue) | SyncModeState::LatestImmediate(queue, _) => {
                        queue.push_front(active_limit);
                    }
                    SyncModeState::AllScheduled(_) | SyncModeState::AllImmediate => {}
                };
            }
        }

        if transfer.succeeded() {
            let result = self.run_cycle(&ctx).await;
            unhandled_result(ctx.log(), result);
        } else {
            ctx.send_later(RetrySnapshotSyncCycleMessage, Duration::from_secs(300));
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<RetrySnapshotSyncCycleMessage> for SyncActor {
    async fn handle(&mut self, ctx: BcContext<'_, Self>, _msg: RetrySnapshotSyncCycleMessage) {
        if self.state_active_send.is_some() {
            debug!(
                ctx.log(),
                "received retry snapshot cycle message while in active send state"
            );
            return;
        }

        let result = self.run_cycle(&ctx).await;
        unhandled_result(ctx.log(), result);
    }
}

#[async_trait::async_trait]
impl BcHandler<GetActorStatusMessage> for SyncActor {
    async fn handle(&mut self, _ctx: BcContext<'_, Self>, _msg: GetActorStatusMessage) -> String {
        String::from("ok")
    }
}
