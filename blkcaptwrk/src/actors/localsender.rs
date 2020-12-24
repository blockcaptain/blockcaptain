use crate::{
    actorbase::{state_result, state_result_from_result, unhandled_result},
    tasks::{WorkerCompleteMessage, WorkerTask},
    xactorext::{BcActor, BcActorCtrl, BcContext, BcHandler, GetActorStatusMessage, TerminalState},
};
use anyhow::Result;
use libblkcapt::sys::btrfs::SnapshotSender;
use slog::Logger;
use std::mem;
use strum_macros::Display;
use tokio::io::AsyncRead;
use xactor::{message, Sender};

#[message()]
pub struct LocalSenderFinishedMessage(pub Result<()>);

#[message()]
pub struct LocalSenderParentFinishedMessage(pub u64);

#[message(result = "Box<dyn AsyncRead + Send + Unpin>")]
pub struct GetReaderMessage;

pub struct LocalSenderActor {
    parent: Sender<LocalSenderParentFinishedMessage>,
    requestor: Sender<LocalSenderFinishedMessage>,
    state: State,
}

#[derive(Display)]
enum State {
    Created(SnapshotSender),
    Running(WorkerTask, Option<Box<dyn AsyncRead + Send + Unpin>>),
    Finished(Result<()>),
    Faulted,
}

impl State {
    fn take(&mut self) -> Self {
        mem::replace(self, State::Faulted)
    }
}

type SendWorkerCompleteMessage = WorkerCompleteMessage<Result<()>>;

impl LocalSenderActor {
    pub fn new(
        parent: Sender<LocalSenderParentFinishedMessage>, requestor: Sender<LocalSenderFinishedMessage>,
        sender: SnapshotSender, log: &Logger,
    ) -> BcActor<Self> {
        BcActor::new(
            Self {
                parent,
                requestor,
                state: State::Created(sender),
            },
            log,
        )
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for LocalSenderActor {
    async fn started(&mut self, ctx: BcContext<'_, Self>) -> Result<()> {
        if let State::Created(sender) = mem::replace(&mut self.state, State::Faulted) {
            let mut sender = sender.start()?;
            let reader = sender.reader();
            let task = WorkerTask::run(ctx.address(), ctx.log(), |_| async move { sender.wait().await.into() });
            self.state = State::Running(task, Some(Box::new(reader)));
        } else {
            panic!("start called in invalid state");
        }
        Ok(())
    }

    async fn stopped(&mut self, ctx: BcContext<'_, Self>) -> TerminalState {
        let (terminal_state, result) = match self.state.take() {
            State::Created(_) => state_result(TerminalState::Cancelled),
            State::Running(worker, _) => {
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
impl BcHandler<GetReaderMessage> for LocalSenderActor {
    async fn handle(&mut self, _ctx: BcContext<'_, Self>, _msg: GetReaderMessage) -> Box<dyn AsyncRead + Send + Unpin> {
        if let State::Running(worker, Some(reader)) = mem::replace(&mut self.state, State::Faulted) {
            self.state = State::Running(worker, None);
            reader
        } else {
            panic!("getreader called in invalid state");
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<SendWorkerCompleteMessage> for LocalSenderActor {
    async fn handle(&mut self, ctx: BcContext<'_, Self>, msg: SendWorkerCompleteMessage) {
        self.state = State::Finished(msg.0);
        ctx.stop(None);
    }
}

#[async_trait::async_trait]
impl BcHandler<GetActorStatusMessage> for LocalSenderActor {
    async fn handle(&mut self, _ctx: BcContext<'_, Self>, _msg: GetActorStatusMessage) -> String {
        self.state.to_string()
    }
}
