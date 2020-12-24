use super::actorbase::unhandled_result;
use crate::xactorext::halt_and_catch_fire_on_panic;
use anyhow::Context as AnyhowContext;
use slog::{crit, debug, Logger};
use std::{cell::Cell, future::Future, marker::PhantomData, panic};
use tokio::{sync::oneshot, task::JoinHandle};
use xactor::{Actor, Addr, Handler, Message, WeakAddr};

pub struct WorkerTask {
    handle: JoinHandle<()>,
    canceller: Cell<Option<oneshot::Sender<()>>>,
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

impl WorkerTask {
    pub fn run<A, F, R, T>(parent: Addr<A>, log: &Logger, func: F) -> Self
    where
        A: Actor + Handler<WorkerCompleteMessage<T>>,
        F: FnOnce(WorkerTaskContext<A>) -> R + Send + 'static,
        R: Future<Output = CancellableResult<T>> + Send,
        T: Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        let parent = parent.downgrade();
        let context = WorkerTaskContext {
            parent,
            cancellation: receiver,
            log: log.clone(),
        };
        let handle = tokio::spawn(async move {
            let parent = context.parent.clone();
            let log = context.log.clone();

            let maybe_result = halt_and_catch_fire_on_panic(func(context)).await;
            match maybe_result {
                Ok(maybe_cancelled) => match maybe_cancelled {
                    CancellableResult::Ok(result) => match parent.upgrade() {
                        Some(strong_parent) => {
                            unhandled_result(
                                &log,
                                strong_parent
                                    .send(WorkerCompleteMessage(result))
                                    .context("work task finished but failed to send completion message to parent"),
                            );
                        }
                        None => debug!(log, "worker task finished but parent is gone"),
                    },
                    CancellableResult::Cancelled(_) => {
                        debug!(log, "task cancelled");
                    }
                },
                Err(error) => {
                    crit!(log, "worker paniced"; "error" => %error);
                }
            }
        });
        Self {
            handle,
            canceller: Cell::new(Some(sender)),
        }
    }

    pub async fn wait(self) {
        if let Err(err) = self.handle.await {
            if let Ok(reason) = err.try_into_panic() {
                panic::resume_unwind(reason)
            }
        }
    }

    pub fn cancel(&self) {
        if let Some(sender) = self.canceller.take() {
            let _ = sender.send(());
        }
    }

    pub fn abort(&self) {
        // TODO_ON_TOKIO03
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
