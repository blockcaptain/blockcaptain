use futures_util::future::join_all;
use log::*;
use std::{future::Future, marker::PhantomData, time::Duration};
use uuid::Uuid;
use xactor::{sleep, spawn, Actor, Addr, Context, Handler, Message};

pub trait ActorContextExt<A> {
    fn send_interval_later<T>(&self, msg: T, interval: Duration, after: Duration)
    where
        A: Handler<T>,
        T: Message<Result = ()> + Clone + Sync;
}

impl<A> ActorContextExt<A> for Context<A> {
    fn send_interval_later<T>(&self, msg: T, interval: Duration, after: Duration)
    where
        A: Handler<T>,
        T: Message<Result = ()> + Clone + Sync,
    {
        let addr = self.address();
        spawn(async move {
            sleep(after).await;
            loop {
                if addr.send(msg.clone()).is_err() {
                    break;
                }
                sleep(interval).await;
            }
        });
    }
}

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
            .unwrap_or_else(|e| error!("Stopping actor failed: {}.", e));
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
