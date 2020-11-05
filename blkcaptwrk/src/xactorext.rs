use anyhow::Result;
use futures_util::future::join_all;
use heck::SnakeCase;
use slog::{error, o, trace, Logger};
use std::{future::Future, marker::PhantomData, time::Duration};
use uuid::Uuid;
use xactor::{sleep, spawn, Actor, Addr, Context, Handler, Message};

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

pub fn stop_all_actors<'a, V: IntoIterator<Item = &'a mut Addr<T>>, T: xactor::Actor>(actors: V) {
    let actors_iter = actors.into_iter();
    for actor in actors_iter {
        actor
            .stop(None)
            .unwrap_or_else(|e| slog_scope::error!("Stopping actor failed: {}.", e));
    }
}

pub fn join_all_actors<V: IntoIterator<Item = Addr<T>>, T: xactor::Actor>(actors: V) -> impl Future {
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
pub trait BcHandler<M: Message>: BcActorCtrl {
    async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: M) -> M::Result;
}

#[async_trait::async_trait]
#[allow(unused_variables)]
pub trait BcActorCtrl: Sized + Send + 'static {
    async fn started(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>) -> Result<()> {
        Ok(())
    }

    async fn stopped(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>) {}
}

pub struct BcActor<T> {
    inner: T,
    log: Logger,
}

impl<T> BcActor<T> {
    pub fn new(inner: T, log: &Logger) -> Self {
        let log = log.new(o!("actor" => snek_type_name::<T>()));
        Self { inner, log }
    }
}

#[async_trait::async_trait]
impl<A, M> Handler<M> for BcActor<A>
where
    A: BcHandler<M> + BcActorCtrl,
    M: Message,
{
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: M) -> M::Result {
        let log = self.log.new(o!("message" => snek_type_name::<M>()));
        slog::trace!(log, "message received");
        self.inner.handle(&log, ctx, msg).await
    }
}

#[async_trait::async_trait]
impl<A> Actor for BcActor<A>
where
    A: BcActorCtrl,
{
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        trace!(self.log, "actor starting");
        let result = self.inner.started(&self.log, ctx).await;
        if let Err(e) = &result {
            error!(self.log, "actor start failed"; "error" => %e);
        } else {
            trace!(self.log, "actor started");
        }
        result
    }

    async fn stopped(&mut self, ctx: &mut Context<Self>) {
        trace!(self.log, "actor stopping");
        self.inner.stopped(&self.log, ctx).await;
        trace!(self.log, "actor stopped");
    }
}

fn snek_type_name<T>() -> String {
    inner_make_snek_type_name(std::any::type_name::<T>())
}

fn inner_make_snek_type_name(mut name: &str) -> String {
    const ACTOR_SUFFIX: &str = "Actor";
    const MESSAGE_SUFFIX: &str = "Message";

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
