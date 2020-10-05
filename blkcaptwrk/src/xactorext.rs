use std::{future::Future, time::Duration};
use futures_util::future::join_all;
use log::*;
use xactor::{Addr, Context, Handler, Message, sleep, spawn};

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