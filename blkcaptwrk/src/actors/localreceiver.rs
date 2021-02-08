use crate::{
    actorbase::{log_result, state_result, state_result_from_result, unhandled_result},
    tasks::{WorkerCompleteMessage, WorkerTask},
    xactorext::{BcActor, BcActorCtrl, BcContext, BcHandler, GetActorStatusMessage, TerminalState},
};
use anyhow::{anyhow, Result};
use libblkcapt::sys::btrfs::SnapshotReceiver;
use pin_project::{pin_project, pinned_drop};
use slog::Logger;
use std::mem;
use strum_macros::Display;
use tokio::io::AsyncWrite;
use xactor::{message, Sender};

#[message()]
pub struct LocalReceiverStoppedMessage(pub Result<()>);

#[message()]
pub struct LocalReceiverStoppedParentMessage(pub u64, pub Option<String>);

#[message(result = "Result<Box<dyn AsyncWrite + Send + Unpin>>")]
pub struct GetWriterMessage;

pub struct LocalReceiverActor {
    parent: Sender<LocalReceiverStoppedParentMessage>,
    requestor: Sender<LocalReceiverStoppedMessage>,
    state: State,
}

#[derive(Display)]
enum State {
    Holding(SnapshotReceiver),
    Receiving(WorkerTask),
    Draining(WorkerTask),
    Waiting(Result<String>),
    Finished(Result<String>),
    Faulted,
}

impl State {
    fn take(&mut self) -> Self {
        mem::replace(self, State::Faulted)
    }
}

type ReceiveWorkerCompleteMessage = WorkerCompleteMessage<Result<String>>;

#[message()]
struct WriterDropped;

impl LocalReceiverActor {
    pub fn new(
        parent: Sender<LocalReceiverStoppedParentMessage>, requestor: Sender<LocalReceiverStoppedMessage>,
        receiver: SnapshotReceiver, log: &Logger,
    ) -> BcActor<Self> {
        BcActor::new(
            Self {
                parent,
                requestor,
                state: State::Holding(receiver),
            },
            log,
        )
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for LocalReceiverActor {
    async fn started(&mut self, _ctx: BcContext<'_, Self>) -> Result<()> {
        Ok(())
    }

    async fn stopped(&mut self, ctx: BcContext<'_, Self>) -> TerminalState {
        let (terminal_state, result) = match self.state.take() {
            State::Holding(_) | State::Waiting(_) => state_result(TerminalState::Cancelled),
            State::Receiving(worker) | State::Draining(worker) => {
                worker.abort();
                state_result(TerminalState::Cancelled)
            }
            State::Finished(result) => state_result_from_result(result),
            State::Faulted => state_result(TerminalState::Faulted),
        };
        log_result(ctx.log(), &result);
        let (maybe_snapshot, result) = match result {
            Ok(snapshot) => (Some(snapshot), Ok(())),
            Err(error) => (None, Err(error)),
        };
        let parent_notify_result = self
            .parent
            .send(LocalReceiverStoppedParentMessage(ctx.actor_id(), maybe_snapshot));
        let requestor_notify_result = self.requestor.send(LocalReceiverStoppedMessage(result));
        if !matches!(terminal_state, TerminalState::Cancelled) {
            unhandled_result(ctx.log(), parent_notify_result);
            unhandled_result(ctx.log(), requestor_notify_result);
        }

        terminal_state
    }
}

#[async_trait::async_trait]
impl BcHandler<GetWriterMessage> for LocalReceiverActor {
    async fn handle(
        &mut self, ctx: BcContext<'_, Self>, _msg: GetWriterMessage,
    ) -> Result<Box<dyn AsyncWrite + Send + Unpin>> {
        if let State::Holding(receiver) = self.state.take() {
            let receiver = receiver.start();
            match receiver {
                Ok(mut receiver) => {
                    let writer = receiver.writer();
                    let task = WorkerTask::run(
                        ctx.address(),
                        ctx.log(),
                        |_| async move { receiver.wait().await.into() },
                    );
                    self.state = State::Receiving(task);
                    Ok(Box::new(OwnedReceiver::new(writer, ctx.address().sender())))
                }
                Err(error) => {
                    ctx.stop(None);
                    self.state = State::Finished(Err(error));
                    Err(anyhow!("local receiver failed to create writer"))
                }
            }
        } else {
            ctx.stop(None);
            Err(anyhow!("cant get writer in current state"))
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<ReceiveWorkerCompleteMessage> for LocalReceiverActor {
    async fn handle(&mut self, ctx: BcContext<'_, Self>, msg: ReceiveWorkerCompleteMessage) {
        self.state = match self.state.take() {
            State::Receiving(_) => State::Waiting(msg.0),
            State::Draining(_) => {
                ctx.stop(None);
                State::Finished(msg.0)
            }
            _ => {
                ctx.stop(None);
                State::Faulted
            }
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<WriterDropped> for LocalReceiverActor {
    async fn handle(&mut self, ctx: BcContext<'_, Self>, _msg: WriterDropped) {
        self.state = match self.state.take() {
            State::Receiving(worker_task) => State::Draining(worker_task),
            State::Waiting(result) => {
                ctx.stop(None);
                State::Finished(result)
            }
            _ => {
                ctx.stop(None);
                State::Faulted
            }
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<GetActorStatusMessage> for LocalReceiverActor {
    async fn handle(&mut self, _ctx: BcContext<'_, Self>, _msg: GetActorStatusMessage) -> String {
        self.state.to_string()
    }
}

#[pin_project(PinnedDrop)]
struct OwnedReceiver<T> {
    #[pin]
    inner: T,
    owner: Sender<WriterDropped>,
}

impl<T> OwnedReceiver<T> {
    fn new(inner: T, owner: Sender<WriterDropped>) -> Self {
        Self { inner, owner }
    }
}

#[pinned_drop]
impl<T> PinnedDrop for OwnedReceiver<T> {
    fn drop(self: std::pin::Pin<&mut Self>) {
        let _ = self.owner.send(WriterDropped);
    }
}

impl<T: AsyncWrite> AsyncWrite for OwnedReceiver<T> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>, buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let this = self.project();
        this.inner.poll_write(cx, buf)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let this = self.project();
        this.inner.poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let this = self.project();
        this.inner.poll_shutdown(cx)
    }
}
