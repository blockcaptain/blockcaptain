use crate::xactorext::{BcActor, BcActorCtrl, BoxBcWeakAddr, TerminalState};
use anyhow::Result;
use futures_util::{
    future::FutureExt,
    future::{ready, BoxFuture},
    stream::{self, FuturesUnordered, StreamExt},
};
use libblkcapt::core::system;
use once_cell::sync::OnceCell;
use slog::{error, trace, warn, Logger};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use xactor::{message, Actor, Addr, Context, Handler};

pub struct IntelActor {
    log: Logger,
    actors: HashMap<u64, Tractor>,
}

#[message]
pub struct ActorStartMessage(u64, BoxBcWeakAddr);

#[derive(Clone)]
enum ActorState {
    Started,
    Stopped,
    Dropped,
    Zombie,
}

impl ActorStartMessage {
    pub fn new<T: BcActorCtrl>(actor_id: u64, actor_address: Addr<BcActor<T>>) -> Self {
        Self(actor_id, actor_address.into())
    }
}

#[message]
pub struct ActorStopMessage(u64, TerminalState);

impl ActorStopMessage {
    pub fn new(actor_id: u64, state: TerminalState) -> Self {
        Self(actor_id, state)
    }
}

#[message]
pub struct ActorDropMessage(u64);

impl ActorDropMessage {
    pub fn new(actor_id: u64) -> Self {
        Self(actor_id)
    }
}

impl IntelActor {
    pub fn new(log: &Logger) -> Self {
        Self {
            log: log.clone(),
            actors: Default::default(),
        }
    }

    pub async fn start_default_and_register() -> Result<Addr<IntelActor>> {
        let maybe_actor = IntelActor::start_default().await;

        if let Ok(actor) = &maybe_actor {
            INTEL_ACTOR_SINGLETON
                .set(actor.clone())
                .map_err(|_| ())
                .expect("intel actor started only once");
        }

        maybe_actor
    }

    pub fn addr() -> Addr<IntelActor> {
        INTEL_ACTOR_SINGLETON.get().expect("intel actor always started").clone()
    }
}

static INTEL_ACTOR_SINGLETON: OnceCell<Addr<IntelActor>> = OnceCell::new();

#[derive(Clone)]
struct Tractor {
    actor: BoxBcWeakAddr,
    state: ActorState,
    terminal_state: Option<TerminalState>,
    changed: Instant,
}

impl Tractor {
    fn system_terminal_state(&self) -> system::TerminalState {
        self.terminal_state.map(|s| s.into()).unwrap_or_default()
    }
}

#[message]
#[derive(Clone)]
struct Update;

#[message(result = "BoxFuture<'static, system::SystemState>")]
pub struct GetStateMessage;

#[async_trait::async_trait]
impl Actor for IntelActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        trace!(self.log, "intel actor started");
        ctx.send_interval(Update, Duration::from_secs(60));
        Ok(())
    }

    async fn stopped(&mut self, _ctx: &mut Context<Self>) {
        trace!(self.log, "intel actor stopped");

        for (id, tractor) in self.actors.drain() {
            match tractor.state {
                ActorState::Started => error!(self.log, "unstopped actor"; "actor_id" => id),
                ActorState::Stopped | ActorState::Zombie => warn!(self.log, "zombie actor"; "actor_id" => id),
                ActorState::Dropped => {}
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<ActorStartMessage> for IntelActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: ActorStartMessage) {
        self.actors.insert(
            msg.0,
            Tractor {
                actor: msg.1,
                state: ActorState::Started,
                terminal_state: None,
                changed: Instant::now(),
            },
        );
    }
}

#[async_trait::async_trait]
impl Handler<ActorStopMessage> for IntelActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: ActorStopMessage) {
        if let Some(tractor) = self.actors.get_mut(&msg.0) {
            tractor.state = ActorState::Stopped;
            tractor.terminal_state = Some(msg.1);
        } else {
            error!(self.log, "stop message for untracked actor"; "actor_id" => msg.0)
        }
    }
}

#[async_trait::async_trait]
impl Handler<ActorDropMessage> for IntelActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: ActorDropMessage) {
        if let Some(tractor) = self.actors.get_mut(&msg.0) {
            tractor.state = ActorState::Dropped;
        } else {
            error!(self.log, "drop message for untracked actor"; "actor_id" => msg.0)
        }
    }
}

#[async_trait::async_trait]
impl Handler<Update> for IntelActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: Update) {
        const CHECK_AFTER: Duration = Duration::from_secs(30);
        let now = Instant::now();
        let mut remove = vec![];
        for (id, tractor) in self.actors.iter_mut() {
            match tractor.state {
                ActorState::Stopped if now - tractor.changed > CHECK_AFTER => {
                    tractor.state = ActorState::Zombie;
                    tractor.changed = now;
                    warn!(self.log, "zombie detected"; "actor_id" => id)
                }
                ActorState::Dropped if now - tractor.changed > CHECK_AFTER => remove.push(*id),
                _ => {}
            }
        }
        for id in remove {
            self.actors.remove(&id);
        }
    }
}

#[async_trait::async_trait]
impl Handler<GetStateMessage> for IntelActor {
    async fn handle(
        &mut self, _ctx: &mut Context<Self>, _msg: GetStateMessage,
    ) -> BoxFuture<'static, system::SystemState> {
        self.actors
            .clone()
            .into_iter()
            .map(|(id, tractor)| async move {
                system::SystemActor {
                    actor_id: id,
                    actor_state: match tractor.state {
                        ActorState::Started => {
                            let active_state = match tractor.actor.upgrade() {
                                Some(actor) => match tokio::time::timeout(Duration::from_secs(3), actor.status()).await
                                {
                                    Ok(status_result) => match status_result {
                                        Ok(data) => system::ActiveState::Custom(data),
                                        Err(_) => system::ActiveState::Stopping,
                                    },
                                    Err(_) => system::ActiveState::Unresponsive,
                                },
                                None => system::ActiveState::Stopping,
                            };
                            system::ActorState::Started(active_state)
                        }
                        ActorState::Stopped => system::ActorState::Stopped(tractor.system_terminal_state()),
                        ActorState::Dropped => system::ActorState::Dropped(tractor.system_terminal_state()),
                        ActorState::Zombie => system::ActorState::Zombie(tractor.system_terminal_state()),
                    },
                    actor_type: tractor.actor.actor_type(),
                }
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .map(|actors| system::SystemState { actors })
            .boxed()
    }
}

impl Default for IntelActor {
    fn default() -> Self {
        IntelActor::new(&slog_scope::logger())
    }
}

impl From<TerminalState> for libblkcapt::core::system::TerminalState {
    fn from(s: TerminalState) -> Self {
        match s {
            TerminalState::Succeeded => system::TerminalState::Succeeded,
            TerminalState::Failed => system::TerminalState::Failed,
            TerminalState::Cancelled => system::TerminalState::Cancelled,
            TerminalState::Faulted => system::TerminalState::Faulted,
        }
    }
}
