use anyhow::Result;
use libblkcapt::{
    core::ObservableEventStage,
    core::ObservationEmitter,
    core::ObservationRouter,
    model::entities::HealthchecksHeartbeat,
    model::entities::{HealthchecksObserverEntity, ObservableEvent},
    model::Entity,
};
use slog::{debug, error, o, trace, Logger};
use std::{fmt::Debug, future::Future};
use uuid::Uuid;
use xactor::{message, Actor, Broker, Context, Handler, Service};

#[message()]
#[derive(Clone, Debug)]
pub struct ObservableEventMessage {
    pub source: Uuid,
    pub event: ObservableEvent,
    pub stage: ObservableEventStage,
}

#[message()]
#[derive(Clone)]
struct HeartbeatMessage();

pub async fn observable_func<F, T, E, R>(source: Uuid, event: ObservableEvent, func: F) -> core::result::Result<T, E>
where
    F: FnOnce() -> R,
    R: Future<Output = core::result::Result<T, E>>,
    E: Debug,
{
    let mut broker = Broker::from_registry().await.expect("Broker could not be retrieved.");
    broker
        .publish(ObservableEventMessage {
            source,
            event,
            stage: ObservableEventStage::Starting,
        })
        .expect("Publish failed.");

    let result = func().await;

    if let core::result::Result::Err(ref e) = result {
        //trace!(self.log, "Publishing fail event for source {:?} event {:?}.", source, event);
        broker
            .publish(ObservableEventMessage {
                source,
                event,
                stage: ObservableEventStage::Failed(format!("{:?}", e)),
            })
            .expect("Publish failed.");
    } else {
        broker
            .publish(ObservableEventMessage {
                source,
                event,
                stage: ObservableEventStage::Succeeded,
            })
            .expect("Publish failed.");
    }

    result
}

pub struct HealthchecksActor {
    router: ObservationRouter,
    emitter: ObservationEmitter,
    heartbeat_config: Option<HealthchecksHeartbeat>,
    log: Logger,
}

impl HealthchecksActor {
    pub fn new(model: HealthchecksObserverEntity, log: &Logger) -> Self {
        Self {
            log: log.new(o!("actor" => "healthchecks", "observer_id" => model.id().to_string())),
            router: ObservationRouter::new(model.observations),
            emitter: model
                .custom_url
                .map_or_else(ObservationEmitter::default, ObservationEmitter::new),
            heartbeat_config: model.heartbeat,
        }
    }
}

#[async_trait::async_trait]
impl Actor for HealthchecksActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        debug!(self.log, "starting");
        ctx.subscribe::<ObservableEventMessage>().await?;

        if let Some(config) = &self.heartbeat_config {
            ctx.address().send(HeartbeatMessage())?;
            ctx.send_interval(HeartbeatMessage(), config.frequency);
        }

        debug!(self.log, "started");
        Ok(())
    }

    async fn stopped(&mut self, ctx: &mut Context<Self>) {
        debug!(self.log, "stopping");
        ctx.unsubscribe::<ObservableEventMessage>()
            .await
            .expect("Failed to unsubscribe from ObservableEvents.");

        debug!(self.log, "stopped");
    }
}

#[async_trait::async_trait]
impl Handler<ObservableEventMessage> for HealthchecksActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: ObservableEventMessage) {
        trace!(self.log, "received event {:?}", msg);
        let observers = self.router.route(msg.source, msg.event);
        for observer in observers {
            let result = self.emitter.emit(observer.healthcheck_id, msg.stage.clone()).await;
            if let Err(e) = result {
                error!(self.log, "Failed to send Healthchecks event {:?}: {}", msg, e);
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<HeartbeatMessage> for HealthchecksActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: HeartbeatMessage) {
        trace!(self.log, "heartbeat");
        let result = self
            .emitter
            .emit(
                self.heartbeat_config
                    .as_ref()
                    .expect("Heartbeat config must exist.")
                    .healthcheck_id,
                ObservableEventStage::Succeeded,
            )
            .await;
        if let Err(e) = result {
            error!(self.log, "Failed to send Healthchecks heartbeat: {}", e);
        }
    }
}
