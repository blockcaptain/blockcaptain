use crate::{
    actorbase::{unhandled_result, ScheduledMessage},
    xactorext::{BcActor, BcActorCtrl, BcContext, BcHandler, GetActorStatusMessage, TerminalState},
};
use anyhow::Result;
use libblkcapt::{
    core::ObservableEventStage,
    core::ObservationEmitter,
    core::ObservationRouter,
    model::entities::HealthchecksHeartbeat,
    model::Entity,
    model::{
        entities::{HealthchecksObserverEntity, ObservableEvent, ScheduleModel},
        EntityId,
    },
};
use slog::{o, Logger};
use std::{borrow::Borrow, convert::TryFrom, convert::TryInto, fmt::Debug, future::Future};
use xactor::{message, Addr, Broker, Service};

#[message()]
#[derive(Clone, Debug)]
pub struct ObservableEventMessage {
    pub source: EntityId,
    pub event: ObservableEvent,
    pub stage: ObservableEventStage,
}

#[message()]
#[derive(Clone)]
struct HeartbeatMessage;

pub async fn observable_func<F, T, E, R>(source: EntityId, event: ObservableEvent, func: F) -> std::result::Result<T, E>
where
    F: FnOnce() -> R,
    R: Future<Output = std::result::Result<T, E>>,
    E: Debug,
{
    let observation = start_observation(source, event).await;
    let result = func().await;
    observation.result(&result);
    result
}

pub async fn start_observation(source: EntityId, event: ObservableEvent) -> StartedObservation {
    let mut broker = Broker::from_registry().await.expect("broker is always available");
    broker
        .publish(ObservableEventMessage {
            source,
            event,
            stage: ObservableEventStage::Starting,
        })
        .expect("can always publish");

    StartedObservation {
        source,
        event,
        stopped: false,
        broker,
    }
}

pub struct StartedObservation {
    source: EntityId,
    event: ObservableEvent,
    stopped: bool,
    broker: Addr<Broker<ObservableEventMessage>>,
}

impl StartedObservation {
    pub fn succeeded(self) {
        slog_scope::trace!("observation succeeded"; "entity_id" => %self.source, "observable_event" => %self.event);
        self.stop(ObservableEventStage::Succeeded);
    }

    pub fn failed<S: AsRef<str>>(self, message: S) {
        slog_scope::trace!("observable failed"; "entity_id" => %self.source, "observable_event" => %self.event, "error" => message.as_ref());
        self.stop(ObservableEventStage::Failed(message.as_ref().to_owned()));
    }

    pub fn cancelled(self) {
        slog_scope::trace!("observation cancelled"; "entity_id" => %self.source, "observable_event" => %self.event);
        self.failed("cancelled");
    }

    pub fn result<T, E: Debug, R: Borrow<Result<T, E>>>(self, result: R) {
        match result.borrow() {
            Ok(_) => self.succeeded(),
            Err(e) => {
                self.error::<E, &E>(e);
            }
        };
    }

    pub fn error<E: Debug, BE: Borrow<E>>(self, error: BE) {
        self.failed(format!("{:?}", error.borrow()));
    }

    fn stop(mut self, stage: ObservableEventStage) {
        self.broker
            .publish(ObservableEventMessage {
                source: self.source,
                event: self.event,
                stage,
            })
            .expect("can always publish");
        self.stopped = true;
    }
}

impl Drop for StartedObservation {
    fn drop(&mut self) {
        if !self.stopped {
            let _ = self.broker.publish(ObservableEventMessage {
                source: self.source,
                event: self.event,
                stage: ObservableEventStage::Failed(String::from("observation was not stopped explicitly")),
            });
        }
    }
}

pub struct HealthchecksActor {
    router: ObservationRouter,
    emitter: ObservationEmitter,
    heartbeat_config: Option<HealthchecksHeartbeat>,
    heartbeat_schedule: Option<ScheduledMessage>,
}

impl HealthchecksActor {
    pub fn new(model: HealthchecksObserverEntity, log: &Logger) -> BcActor<Self> {
        let observer_id = model.id().to_string();
        BcActor::new(
            Self {
                router: ObservationRouter::new(model.observations),
                emitter: model
                    .custom_url
                    .map_or_else(ObservationEmitter::default, ObservationEmitter::new),
                heartbeat_config: model.heartbeat,
                heartbeat_schedule: None,
            },
            &log.new(o!("observer_id" => observer_id)),
        )
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for HealthchecksActor {
    async fn started(&mut self, ctx: BcContext<'_, Self>) -> Result<()> {
        ctx.subscribe::<ObservableEventMessage>().await?;

        if let Some(config) = &self.heartbeat_config {
            self.heartbeat_schedule = Some(
                ScheduleModel::try_from(config.frequency)?
                    .try_into()
                    .map(|schedule| ScheduledMessage::new(schedule, "heartbeat", HeartbeatMessage, &ctx))?,
            );
        }

        Ok(())
    }

    async fn stopped(&mut self, ctx: BcContext<'_, Self>) -> TerminalState {
        ctx.unsubscribe::<ObservableEventMessage>()
            .await
            .expect("can always unsubscribe");

        TerminalState::Succeeded
    }
}

#[async_trait::async_trait]
impl BcHandler<ObservableEventMessage> for HealthchecksActor {
    async fn handle(&mut self, ctx: BcContext<'_, Self>, msg: ObservableEventMessage) {
        let observers = self.router.route(msg.source, msg.event);
        for observer in observers {
            let result = self.emitter.emit(observer.healthcheck_id, msg.stage.clone()).await;
            unhandled_result(ctx.log(), result);
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<HeartbeatMessage> for HealthchecksActor {
    async fn handle(&mut self, ctx: BcContext<'_, Self>, _msg: HeartbeatMessage) {
        let result = self
            .emitter
            .emit(
                self.heartbeat_config
                    .as_ref()
                    .expect("heartbeat config exists if heartbeat messages are scheduled")
                    .healthcheck_id,
                ObservableEventStage::Succeeded,
            )
            .await;

        unhandled_result(ctx.log(), result);
    }
}

#[async_trait::async_trait]
impl BcHandler<GetActorStatusMessage> for HealthchecksActor {
    async fn handle(&mut self, _ctx: BcContext<'_, Self>, _msg: GetActorStatusMessage) -> String {
        String::from("ok")
    }
}
