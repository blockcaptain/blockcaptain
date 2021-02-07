use crate::{
    actorbase::{state_result, state_result_from_result, unhandled_result},
    tasks::{WorkerCompleteMessage, WorkerTask},
    xactorext::{BcActor, BcActorCtrl, BcContext, BcHandler, GetActorStatusMessage, TerminalState},
};
use anyhow::{anyhow, Result};
use libblkcapt::sys::btrfs::SnapshotSender;
use pin_project::{pin_project, pinned_drop};
use slog::Logger;
use std::mem;
use strum_macros::Display;
use tokio::io::AsyncRead;
use xactor::{message, Sender};

#[message()]
pub struct LocalSenderFinishedMessage(pub Result<()>);

#[message()]
pub struct LocalSenderParentFinishedMessage(pub u64);

#[message(result = "Result<Box<dyn AsyncRead + Send + Unpin>>")]
pub struct TakeReaderMessage;

pub struct LocalSenderActor {
    parent: Sender<LocalSenderParentFinishedMessage>,
    requestor: Sender<LocalSenderFinishedMessage>,
    state: State,
}

#[derive(Display)]
enum State {
    Holding(SnapshotSender),
    Sending(WorkerTask, ReaderState),
    Draining(Result<()>),
    Finished(Result<()>),
    Faulted,
}

#[derive(Display)]
enum ReaderState {
    InUse,
    Dropped,
}

impl State {
    fn take(&mut self) -> Self {
        mem::replace(self, State::Faulted)
    }
}

type SendWorkerCompleteMessage = WorkerCompleteMessage<Result<()>>;

#[message()]
struct ReaderDropped;

impl LocalSenderActor {
    pub fn new(
        parent: Sender<LocalSenderParentFinishedMessage>, requestor: Sender<LocalSenderFinishedMessage>,
        sender: SnapshotSender, log: &Logger,
    ) -> BcActor<Self> {
        BcActor::new(
            Self {
                parent,
                requestor,
                state: State::Holding(sender),
            },
            log,
        )
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for LocalSenderActor {
    async fn started(&mut self, _ctx: BcContext<'_, Self>) -> Result<()> {
        Ok(())
    }

    async fn stopped(&mut self, ctx: BcContext<'_, Self>) -> TerminalState {
        let (terminal_state, result) = match self.state.take() {
            State::Holding(_) | State::Draining(..) => state_result(TerminalState::Cancelled),
            State::Sending(worker, _) => {
                worker.abort();
                state_result(TerminalState::Cancelled)
            }
            State::Finished(result) => state_result_from_result(result),
            State::Faulted => state_result(TerminalState::Faulted),
        };

        let parent_notify_result = self.parent.send(LocalSenderParentFinishedMessage(ctx.actor_id()));
        let requestor_notify_result = self.requestor.send(LocalSenderFinishedMessage(result));
        if !matches!(terminal_state, TerminalState::Cancelled) {
            unhandled_result(ctx.log(), parent_notify_result);
            unhandled_result(ctx.log(), requestor_notify_result);
        }

        terminal_state
    }
}

#[async_trait::async_trait]
impl BcHandler<TakeReaderMessage> for LocalSenderActor {
    async fn handle(
        &mut self, ctx: BcContext<'_, Self>, _msg: TakeReaderMessage,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
        if let State::Holding(sender) = mem::replace(&mut self.state, State::Faulted) {
            let sender = sender.start();
            match sender {
                Ok(mut sender) => {
                    let reader = sender.reader();
                    let task = WorkerTask::run(ctx.address(), ctx.log(), |_| async move { sender.wait().await.into() });
                    self.state = State::Sending(task, ReaderState::InUse);
                    Ok(Box::new(OwnedSender::new(reader, ctx.address().sender())))
                }
                result => {
                    ctx.stop(None);
                    self.state = State::Finished(result.map(|_| ()));
                    Err(anyhow!("local sender failed to create reader"))
                }
            }
        } else {
            ctx.stop(None);
            Err(anyhow!("cant get reader in current state"))
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<SendWorkerCompleteMessage> for LocalSenderActor {
    async fn handle(&mut self, ctx: BcContext<'_, Self>, msg: SendWorkerCompleteMessage) {
        self.state = match self.state.take() {
            State::Sending(_, reader_state) => match reader_state {
                ReaderState::InUse => State::Draining(msg.0),
                ReaderState::Dropped => {
                    ctx.stop(None);
                    State::Finished(msg.0)
                }
            },
            _ => {
                ctx.stop(None);
                State::Faulted
            }
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<ReaderDropped> for LocalSenderActor {
    async fn handle(&mut self, ctx: BcContext<'_, Self>, _msg: ReaderDropped) {
        self.state = match self.state.take() {
            State::Sending(worker_task, ReaderState::InUse) => State::Sending(worker_task, ReaderState::Dropped),
            State::Draining(result) => {
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
impl BcHandler<GetActorStatusMessage> for LocalSenderActor {
    async fn handle(&mut self, _ctx: BcContext<'_, Self>, _msg: GetActorStatusMessage) -> String {
        self.state.to_string()
    }
}

#[pin_project(PinnedDrop)]
struct OwnedSender<T> {
    #[pin]
    inner: T,
    owner: Sender<ReaderDropped>,
}

impl<T> OwnedSender<T> {
    fn new(inner: T, owner: Sender<ReaderDropped>) -> Self {
        Self { inner, owner }
    }
}

#[pinned_drop]
impl<T> PinnedDrop for OwnedSender<T> {
    fn drop(self: std::pin::Pin<&mut Self>) {
        let _ = self.owner.send(ReaderDropped);
    }
}

impl<T: AsyncRead> AsyncRead for OwnedSender<T> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>, buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.project();
        this.inner.poll_read(cx, buf)
    }
}
