use anyhow::Result;
use once_cell::sync::OnceCell;
use slog::{debug, error, trace, warn, Logger};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use strum_macros::Display;
use xactor::{message, Actor, Addr, Context, Handler, Service, WeakAddr};

pub struct IntelActor {
    log: Logger,
    actors: HashMap<u64, Tractor>,
}

#[message]
pub struct ActorStartMessage(u64, Box<dyn BcWeakAddr + Send>);

impl ActorStartMessage {
    pub fn new<T: Actor>(actor_id: u64, actor_address: Addr<T>) -> Self {
        Self(actor_id, Box::new(BcWeakAddrImpl(actor_address.downgrade())))
    }
}

#[message]
pub struct ActorStopMessage(u64);

impl ActorStopMessage {
    pub fn new(actor_id: u64) -> Self {
        Self(actor_id)
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

struct Tractor {
    actor: Box<dyn BcWeakAddr + Send>,
    state: ActorState,
    changed: Instant,
}

#[derive(Display)]
enum ActorState {
    Started,
    Stopped,
    Dropped,
    Zombie,
}

#[message]
#[derive(Clone)]
struct Update;

#[message]
struct DebugNow;

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
                changed: Instant::now(),
            },
        );
    }
}

#[async_trait::async_trait]
impl Handler<ActorStopMessage> for IntelActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: ActorStopMessage) {
        self.actors.get_mut(&msg.0).unwrap().state = ActorState::Stopped;
    }
}

#[async_trait::async_trait]
impl Handler<ActorDropMessage> for IntelActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: ActorDropMessage) {
        self.actors.get_mut(&msg.0).unwrap().state = ActorState::Dropped;
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
impl Handler<DebugNow> for IntelActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: DebugNow) {
        for (id, tractor) in self.actors.iter() {
            println!("ACTOR DEBUG [{:04}][{}]", id, tractor.state);
        }
    }
}

impl Default for IntelActor {
    fn default() -> Self {
        IntelActor::new(&slog_scope::logger())
    }
}

trait BcWeakAddr {
    fn upgrade(&self) -> Option<Box<dyn BcAddr>>;
}

trait BcAddr {}

struct BcWeakAddrImpl<T>(WeakAddr<T>);

impl<T: 'static> BcWeakAddr for BcWeakAddrImpl<T> {
    fn upgrade(&self) -> Option<Box<dyn BcAddr>> {
        self.0.upgrade().map(|a| Box::new(BcAddrImpl(a)) as Box<dyn BcAddr>)
    }
}

struct BcAddrImpl<T>(Addr<T>);

impl<T> BcAddr for BcAddrImpl<T> {}
