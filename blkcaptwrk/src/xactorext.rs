use crate::{
    actorbase::unhandled_result,
    actors::intel::{ActorDropMessage, ActorStartMessage, ActorStopMessage, IntelActor},
};
use anyhow::{anyhow, Context as _, Result};
use futures_util::future::{join_all, FutureExt};
use heck::SnakeCase;
use paste::paste;
use slog::{crit, error, o, trace, Logger};
use std::{future::Future, marker::PhantomData, panic::AssertUnwindSafe, time::Duration};
use strum_macros::Display;
use uuid::Uuid;
use xactor::{message, Actor, Addr, Context, Handler, Message, WeakAddr};

// pub trait ActorAddrExt<T: Actor> {
//     fn get_child_actor<U, O>(&self, id: Uuid) -> U
//     where U: Future<Output = O>, T: Handler<GetChildActorMessage<T>> ;
// }

// impl<T: Actor> ActorAddrExt<T> for Addr<T> {
//     fn get_child_actor<U, O>(&self, id: Uuid) -> U
//     where U: Future<Output = O>, T: Handler<GetChildActorMessage<T>>
//      {
//         let x = self.call(GetChildActorMessage(id, PhantomData));
//         x
//     }
// }

pub fn stop_all_actors<'a, V: IntoIterator<Item = &'a mut A>, A: AnyAddr + 'a>(actors: V) {
    let actors_iter = actors.into_iter();
    for actor in actors_iter {
        actor
            .stop()
            .unwrap_or_else(|e| slog_scope::error!("Stopping actor failed: {}.", e));
    }
}

pub fn join_all_actors<V: IntoIterator<Item = A>, A: AnyAddr + 'static>(actors: V) -> impl Future {
    let futures_iter = actors.into_iter().map(|a| a.wait_for_stop());
    join_all(futures_iter)
}

pub struct GetChildActorMessage<T>(pub Uuid, PhantomData<T>);

impl<T> GetChildActorMessage<T> {
    pub fn new(id: Uuid) -> Self {
        Self(id, PhantomData)
    }
}

impl<T: Actor> xactor::Message for GetChildActorMessage<T> {
    type Result = Option<Addr<T>>;
}

#[async_trait::async_trait]
pub trait BcHandler<M: Message>: Sized {
    async fn handle(&mut self, ctx: BcContext<'_, Self>, msg: M) -> M::Result;
}

#[async_trait::async_trait]
#[allow(unused_variables)]
pub trait BcActorCtrl: BcHandler<GetActorStatusMessage> + Sized + Send + 'static {
    async fn started(&mut self, ctx: BcContext<'_, Self>) -> Result<()> {
        Ok(())
    }

    async fn stopped(&mut self, ctx: BcContext<'_, Self>) -> TerminalState {
        TerminalState::Succeeded
    }
}

#[derive(Clone, Copy, Display)]
#[strum(serialize_all = "snake_case")]
pub enum TerminalState {
    Succeeded,
    Failed,
    Cancelled,
    Faulted,
}

impl TerminalState {
    pub fn succeeded(self) -> bool {
        matches!(self, Self::Succeeded)
    }
}

// impl TerminalState {
//     fn dnr() -> Self {
//         TerminalState::Failed(false)
//     }

//     fn retry() -> Self {
//         TerminalState::Failed(true)
//     }
// }

impl<T, E> From<Result<T, E>> for TerminalState {
    fn from(result: std::result::Result<T, E>) -> Self {
        match result {
            Ok(_) => TerminalState::Succeeded,
            Err(_) => TerminalState::Failed,
        }
    }
}

pub struct BcActor<T> {
    inner: T,
    actor_id: u64,
    log: Logger,
}

// Replace with specialization when available?
macro_rules! notify_impl {
    ($f:ident, $t:ty) => {
        paste! {
            fn [<intel_notify_ $f>](&self, message: $t) {
                let intel_addr = IntelActor::addr();
                unhandled_result(
                    &self.log,
                    intel_addr.send(message).context("failed to notify intel actor"),
                )
            }
        }
    };
}

impl<T> BcActor<T> {
    pub fn new(inner: T, log: &Logger) -> Self {
        let log = log.new(o!("actor" => snek_type_name::<T>()));
        Self {
            inner,
            actor_id: 0,
            log,
        }
    }

    notify_impl!(start, ActorStartMessage);
    notify_impl!(stop, ActorStopMessage);
    notify_impl!(drop, ActorDropMessage);
}

pub async fn halt_and_catch_fire_on_panic<T>(future: impl Future<Output = T>) -> Result<T> {
    let maybe_output = AssertUnwindSafe(future).catch_unwind().await;
    if maybe_output.is_err() {
        use nix::sys::signal::{kill, Signal};
        use nix::unistd::Pid;

        let _ = kill(Pid::this(), Signal::SIGINT);
    }

    maybe_output.map_err(|payload| {
        let reason: String = payload
            .as_ref()
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| payload.as_ref().downcast_ref::<String>().map(|s| s.as_str()))
            .unwrap_or("unknown")
            .to_string();
        anyhow!("panic reason: {}", reason)
    })
}

#[message(result = "String")]
pub struct GetActorStatusMessage;

#[async_trait::async_trait]
impl<A, M> Handler<M> for BcActor<A>
where
    A: BcHandler<M> + BcActorCtrl,
    M: Message,
{
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: M) -> M::Result {
        let log = self.log.new(o!("message" => snek_type_name::<M>()));
        slog::trace!(log, "message received");
        let fut = self.inner.handle(
            BcContext {
                log: &self.log,
                native: ctx,
            },
            msg,
        );
        halt_and_catch_fire_on_panic(fut).await.unwrap_or_else(|error| {
            crit!(self.log, "actor paniced handling message"; "error" => %error);
            panic!("message handler paniced");
        })
    }
}

#[async_trait::async_trait]
impl<A> Actor for BcActor<A>
where
    A: BcActorCtrl,
{
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        self.log = self.log.new(o!("actor_id" => ctx.actor_id()));
        trace!(self.log, "actor starting");
        let fut = self.inner.started(BcContext {
            log: &self.log,
            native: ctx,
        });
        let result = halt_and_catch_fire_on_panic(fut).await.and_then(|r| r);
        if let Err(e) = &result {
            error!(self.log, "actor start failed"; "error" => %e);
        } else {
            trace!(self.log, "actor started");
            self.actor_id = ctx.actor_id();
            self.intel_notify_start(ActorStartMessage::new(ctx.actor_id(), ctx.address()));
        }
        result
    }

    async fn stopped(&mut self, ctx: &mut Context<Self>) {
        trace!(self.log, "actor stopping");
        let fut = self.inner.stopped(BcContext {
            log: &self.log,
            native: ctx,
        });

        let result = halt_and_catch_fire_on_panic(fut).await;
        let terminal_state = result.unwrap_or_else(|error| {
            crit!(self.log, "actor panic on stop"; "error" => %error);
            TerminalState::Faulted
        });
        self.intel_notify_stop(ActorStopMessage::new(self.actor_id, terminal_state));
        trace!(self.log, "actor stopped"; "terminal_state" => %terminal_state);
    }
}

impl<A> Drop for BcActor<A> {
    fn drop(&mut self) {
        if self.actor_id != 0 {
            self.intel_notify_drop(ActorDropMessage::new(self.actor_id));
        }
    }
}

fn snek_type_name<T>() -> String {
    inner_make_snek_type_name(std::any::type_name::<T>())
}

fn inner_make_snek_type_name(mut name: &str) -> String {
    const ACTOR_SUFFIX: &str = "Actor";
    const MESSAGE_SUFFIX: &str = "Message";

    if let Some(index) = name.find('<') {
        name = &name[..index];
    }
    if let Some(index) = name.rfind("::") {
        name = &name[index..];
    }

    if name.ends_with(ACTOR_SUFFIX) {
        name = &name[0..(name.len() - ACTOR_SUFFIX.len())]
    } else if name.ends_with(MESSAGE_SUFFIX) {
        name = &name[0..(name.len() - MESSAGE_SUFFIX.len())]
    }

    name.to_snake_case()
}

pub type BoxBcWeakAddr = Box<dyn BcWeakAddr>;
pub type BoxBcAddr = Box<dyn BcAddr>;

pub trait BcWeakAddr: BcWeakAddrClone + Sync + Send {
    fn actor_id(&self) -> u64;
    fn actor_type(&self) -> String;
    fn upgrade(&self) -> Option<BoxBcAddr>;
}

#[async_trait::async_trait]
pub trait BcAddr: Sync + Send {
    fn actor_id(&self) -> u64;
    fn actor_type(&self) -> String;
    fn stop(&mut self) -> Result<()>;
    async fn status(&self) -> Result<String>;
    async fn wait_for_stop(self: Box<Self>);
}

struct BcWeakAddrImpl<T>(WeakAddr<BcActor<T>>);

impl<T> Clone for BcWeakAddrImpl<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: BcActorCtrl> BcWeakAddr for BcWeakAddrImpl<T> {
    fn upgrade(&self) -> Option<BoxBcAddr> {
        self.0.upgrade().map(|a| a.into())
    }

    fn actor_type(&self) -> String {
        snek_type_name::<T>()
    }

    fn actor_id(&self) -> u64 {
        self.0.actor_id()
    }
}

struct BcAddrImpl<T>(Addr<BcActor<T>>);

#[async_trait::async_trait]
impl<T: BcActorCtrl> BcAddr for BcAddrImpl<T> {
    fn stop(&mut self) -> Result<()> {
        self.0.stop(None)
    }

    async fn wait_for_stop(self: Box<Self>) {
        self.0.wait_for_stop().await;
    }

    fn actor_type(&self) -> String {
        snek_type_name::<T>()
    }

    async fn status(&self) -> Result<String> {
        self.0.call(GetActorStatusMessage).await
    }

    fn actor_id(&self) -> u64 {
        self.0.actor_id()
    }
}

impl<T: BcActorCtrl> From<Addr<BcActor<T>>> for BoxBcWeakAddr {
    fn from(addr: Addr<BcActor<T>>) -> Self {
        addr.downgrade().into()
    }
}

impl<T: BcActorCtrl> From<&Addr<BcActor<T>>> for BoxBcWeakAddr {
    fn from(addr: &Addr<BcActor<T>>) -> Self {
        addr.downgrade().into()
    }
}

impl<T: BcActorCtrl> From<WeakAddr<BcActor<T>>> for BoxBcWeakAddr {
    fn from(addr: WeakAddr<BcActor<T>>) -> Self {
        Box::new(BcWeakAddrImpl(addr))
    }
}

impl<T: BcActorCtrl> From<Addr<BcActor<T>>> for BoxBcAddr {
    fn from(addr: Addr<BcActor<T>>) -> Self {
        Box::new(BcAddrImpl(addr))
    }
}

//https://stackoverflow.com/questions/30353462/how-to-clone-a-struct-storing-a-boxed-trait-object
//https://github.com/dtolnay/dyn-clone

pub trait BcWeakAddrClone {
    fn clone_box(&self) -> Box<dyn BcWeakAddr>;
}

impl<T> BcWeakAddrClone for T
where
    T: 'static + BcWeakAddr + Clone,
{
    fn clone_box(&self) -> Box<dyn BcWeakAddr> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn BcWeakAddr> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

#[async_trait::async_trait]
pub trait AnyAddr {
    fn stop(&mut self) -> Result<()>;
    async fn wait_for_stop(self);
}

#[async_trait::async_trait]
impl<T: Actor> AnyAddr for Addr<T> {
    fn stop(&mut self) -> Result<()> {
        Self::stop(self, None)
    }

    async fn wait_for_stop(self) {
        Self::wait_for_stop(self).await
    }
}

#[async_trait::async_trait]
impl AnyAddr for BoxBcAddr {
    fn stop(&mut self) -> Result<()> {
        BcAddr::stop(self.as_mut())
    }

    async fn wait_for_stop(self) {
        BcAddr::wait_for_stop(self).await
    }
}

pub struct BcContext<'a, A> {
    native: &'a mut Context<BcActor<A>>,
    log: &'a Logger,
}

impl<'a, A> BcContext<'a, A>
where
    A: BcActorCtrl,
{
    pub fn address(&self) -> Addr<BcActor<A>> {
        self.native.address()
    }

    pub fn actor_id(&self) -> u64 {
        self.native.actor_id()
    }

    pub fn stop(&self, err: Option<anyhow::Error>) {
        self.native.stop(err)
    }

    pub fn send_later<T>(&self, msg: T, after: Duration)
    where
        A: BcHandler<T>,
        T: Message<Result = ()>,
    {
        self.native.send_later(msg, after)
    }

    pub async fn subscribe<T: Message<Result = ()>>(&self) -> Result<()>
    where
        A: BcHandler<T>,
    {
        self.native.subscribe::<T>().await
    }

    pub async fn unsubscribe<T: Message<Result = ()>>(&self) -> Result<()> {
        self.native.unsubscribe::<T>().await
    }

    // Extensions
    pub fn log(&self) -> &Logger {
        self.log
    }
}
