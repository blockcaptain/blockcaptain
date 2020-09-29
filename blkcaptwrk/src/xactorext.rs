use std::time::Duration;
use xactor::{sleep, spawn, Context, Handler, Message};

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
