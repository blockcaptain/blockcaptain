use super::actorbase::unhandled_result;
use anyhow::{Context as AnyhowContext, Result};
use slog::{error, info, Logger};
use std::{future::Future, marker::PhantomData, panic};
use tokio::{sync::oneshot, task::JoinHandle};
use xactor::{Actor, Addr, Handler, Message, WeakAddr};

pub struct WorkerTask {
    handle: JoinHandle<()>,
    canceller: oneshot::Sender<()>,
}

pub struct WorkerTaskContext<A> {
    parent: WeakAddr<A>,
    cancellation: oneshot::Receiver<()>,
    log: Logger,
}

pub struct WorkerCompleteMessage<T>(pub T);

impl<T: Send + 'static> Message for WorkerCompleteMessage<T> {
    type Result = ();
}

pub trait BorrowingFn<'a, A, T> {
    type Fut: std::future::Future<Output = CancellableResult<T>> + Send + 'a;
    fn call(self, arg: &'a mut WorkerTaskContext<A>) -> Self::Fut;
}

impl<'a, Fu: 'a, F, A, T> BorrowingFn<'a, A, T> for F
where
    A: Actor,
    F: FnOnce(&'a mut WorkerTaskContext<A>) -> Fu,
    Fu: Future<Output = CancellableResult<T>> + Send + 'a,
{
    type Fut = Fu;
    fn call(self, rt: &'a mut WorkerTaskContext<A>) -> Fu {
        self(rt)
    }
}

// pub fn wrap<A: Actor, T, F, Fu>(closure: F) -> impl for<'a> BorrowingFn<'a, A, T>
// where
//     F: for<'a> FnOnce(&'a mut WorkerTaskContext<A>) -> Fu,
//     Fu: Future<Output = CancellableResult<T>> + Send + 'a,
// {
//     closure
// }

impl WorkerTask {
    pub fn run<A, F, T>(parent: Addr<A>, log: &Logger, func: F) -> Self
    where
        A: Actor + Handler<WorkerCompleteMessage<T>>,
        F: for<'a> BorrowingFn<'a, A, T> + Send + 'static,
        T: Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        let parent = parent.downgrade();
        let mut context = WorkerTaskContext {
            parent,
            cancellation: receiver,
            log: log.clone(),
        };
        let handle = tokio::spawn(async move {
            let parent = context.parent.clone();
            let log = context.log.clone();
            // I can't figure out lifetimes if i pass &mut context here :'(
            // Since we await the future here, it should make sense that context
            // reference is gone in this scope, but the future needs to be static
            // go it can live in the future.
            // https://stackoverflow.com/questions/63517250/specify-rust-closures-lifetime
            // https://github.com/rust-lang/rust/issues/70263
            let maybe_cancelled = func.call(&mut context).await;
            match maybe_cancelled {
                CancellableResult::Ok(result) => match parent.upgrade() {
                    Some(strong_parent) => {
                        unhandled_result(
                            &log,
                            strong_parent
                                .send(WorkerCompleteMessage(result))
                                .context("work task finished but failed to send completion message to parent"),
                        );
                    }
                    None => error!(log, "worker task finished but parent is gone"),
                },
                CancellableResult::Cancelled(_) => {
                    info!(log, "task cancelled");
                }
            }
        });
        Self {
            handle,
            canceller: sender,
        }
    }

    pub async fn wait(self) {
        if let Err(err) = self.handle.await {
            // Joinhandle should never be cancelled because we own the handle on self
            // rework ^ not true if we allow abort.
            match err.try_into_panic() {
                Ok(reason) => panic::resume_unwind(reason),
                Err(err) => panic!(err),
            }
        }
    }

    pub fn cancel(self) {
        let _ = self.canceller.send(());
    }

    pub fn abort(self) {
        //FIXME abort available in tokio 0.3
        //let _ = self.handle.abort()
    }
}

pub enum CancellableResult<T> {
    Ok(T),
    Cancelled(CancelledMarker),
}

impl<T> From<T> for CancellableResult<T> {
    fn from(value: T) -> Self {
        CancellableResult::Ok(value)
    }
}

pub struct CancelledMarker(PhantomData<()>);

impl<A> WorkerTaskContext<A> {
    pub async fn await_cancellable<T>(&mut self, fut: impl Future<Output = T> + Send) -> CancellableResult<T> {
        tokio::select! {
            _ = &mut self.cancellation => {
                CancellableResult::Cancelled(CancelledMarker(PhantomData))
            }
            result = fut => {
                CancellableResult::Ok(result)
            }
        }
    }
}
