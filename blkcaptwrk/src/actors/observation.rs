use crate::{
    actorbase::{schedule_next_message, unhandled_result},
    xactorext::{BcActor, BcActorCtrl, BcHandler, GetActorStatusMessage, TerminalState},
};
use anyhow::Result;
use cron::Schedule;
use libblkcapt::{
    core::ObservableEventStage,
    core::ObservationEmitter,
    core::ObservationRouter,
    model::entities::HealthchecksHeartbeat,
    model::entities::{HealthchecksObserverEntity, ObservableEvent, ScheduleModel},
    model::Entity,
};
use slog::{o, Logger};
use std::{borrow::Borrow, convert::TryFrom, convert::TryInto, fmt::Debug, future::Future};
use uuid::Uuid;
use xactor::{message, Addr, Broker, Context, Service};

#[message()]
#[derive(Clone, Debug)]
pub struct ObservableEventMessage {
    pub source: Uuid,
    pub event: ObservableEvent,
    pub stage: ObservableEventStage,
}

#[message()]
#[derive(Clone)]
struct HeartbeatMessage;

pub async fn observable_func<F, T, E, R>(source: Uuid, event: ObservableEvent, func: F) -> std::result::Result<T, E>
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

pub async fn start_observation(source: Uuid, event: ObservableEvent) -> StartedObservation {
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
    source: Uuid,
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
                self.failed(format!("{:?}", e));
            }
        };
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
    heartbeat_schedule: Option<Schedule>,
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

    fn schedule_next_heartbeat(&self, log: &Logger, ctx: &mut Context<BcActor<Self>>) {
        schedule_next_message(
            self.heartbeat_schedule.as_ref(),
            "heartbeat",
            HeartbeatMessage,
            log,
            ctx,
        );
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for HealthchecksActor {
    async fn started(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>) -> Result<()> {
        ctx.subscribe::<ObservableEventMessage>().await?;

        if let Some(config) = &self.heartbeat_config {
            self.heartbeat_schedule = Some(ScheduleModel::try_from(config.frequency)?.try_into()?);
            self.schedule_next_heartbeat(log, ctx);
        }

        Ok(())
    }

    async fn stopped(&mut self, _log: &Logger, ctx: &mut Context<BcActor<Self>>) -> TerminalState {
        ctx.unsubscribe::<ObservableEventMessage>()
            .await
            .expect("can always unsubscribe");

        TerminalState::Succeeded
    }
}

#[async_trait::async_trait]
impl BcHandler<ObservableEventMessage> for HealthchecksActor {
    async fn handle(&mut self, log: &Logger, _ctx: &mut Context<BcActor<Self>>, msg: ObservableEventMessage) {
        let observers = self.router.route(msg.source, msg.event);
        for observer in observers {
            let result = self.emitter.emit(observer.healthcheck_id, msg.stage.clone()).await;
            unhandled_result(log, result);
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<HeartbeatMessage> for HealthchecksActor {
    async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, _msg: HeartbeatMessage) {
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

        unhandled_result(log, result);
        self.schedule_next_heartbeat(log, ctx);
    }
}

#[async_trait::async_trait]
impl BcHandler<GetActorStatusMessage> for HealthchecksActor {
    async fn handle(
        &mut self,
        _log: &Logger,
        _ctx: &mut Context<BcActor<Self>>,
        _msg: GetActorStatusMessage,
    ) -> String {
        String::from("ok")
    }
}
