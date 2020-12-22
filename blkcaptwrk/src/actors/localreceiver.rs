use crate::{
    actorbase::{state_result, state_result_from_result, unhandled_result},
    tasks::{WorkerCompleteMessage, WorkerTask},
    xactorext::{BcActor, BcActorCtrl, BcContext, BcHandler, GetActorStatusMessage, TerminalState},
};
use anyhow::Result;
use libblkcapt::core::{localsndrcv::SnapshotReceiver, BtrfsContainerSnapshot};
use slog::Logger;
use std::mem;
use strum_macros::Display;
use tokio::io::AsyncWrite;
use xactor::{message, Sender};

#[message()]
pub struct LocalReceiverStoppedMessage(pub Result<()>);

#[message()]
pub struct LocalReceiverStoppedParentMessage(pub u64, pub Option<BtrfsContainerSnapshot>);

#[message(result = "Box<dyn AsyncWrite + Send + Unpin>")]
pub struct GetWriterMessage;

pub struct LocalReceiverActor {
    parent: Sender<LocalReceiverStoppedParentMessage>,
    requestor: Sender<LocalReceiverStoppedMessage>,
    state: State,
}

#[derive(Display)]
enum State {
    Created(SnapshotReceiver),
    Running(WorkerTask, Option<Box<dyn AsyncWrite + Send + Unpin>>),
    Finished(Result<BtrfsContainerSnapshot>),
    Faulted,
}

impl State {
    fn take(&mut self) -> Self {
        mem::replace(self, State::Faulted)
    }
}

type ReceiveWorkerCompleteMessage = WorkerCompleteMessage<Result<BtrfsContainerSnapshot>>;

impl LocalReceiverActor {
    pub fn new(
        parent: Sender<LocalReceiverStoppedParentMessage>, requestor: Sender<LocalReceiverStoppedMessage>,
        receiver: SnapshotReceiver, log: &Logger,
    ) -> BcActor<Self> {
        BcActor::new(
            Self {
                parent,
                requestor,
                state: State::Created(receiver),
            },
            log,
        )
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for LocalReceiverActor {
    async fn started(&mut self, ctx: BcContext<'_, Self>) -> Result<()> {
        if let State::Created(receiver) = mem::replace(&mut self.state, State::Faulted) {
            let mut receiver = receiver.start()?;
            let writer = receiver.writer();
            let task = WorkerTask::run(
                ctx.address(),
                ctx.log(),
                |_| async move { receiver.wait().await.into() },
            );
            self.state = State::Running(task, Some(Box::new(writer)));
        } else {
            panic!("start called in invalid state");
        }
        Ok(())
    }

    async fn stopped(&mut self, ctx: BcContext<'_, Self>) -> TerminalState {
        let (terminal_state, result): (TerminalState, Result<BtrfsContainerSnapshot>) = match self.state.take() {
            State::Created(_) => state_result(TerminalState::Cancelled),
            State::Running(worker, _) => {
                worker.abort();
                state_result(TerminalState::Cancelled)
            }
            State::Finished(result) => state_result_from_result(result),
            State::Faulted => state_result(TerminalState::Faulted),
        };

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
        &mut self, _ctx: BcContext<'_, Self>, _msg: GetWriterMessage,
    ) -> Box<dyn AsyncWrite + Send + Unpin> {
        if let State::Running(worker, Some(writer)) = mem::replace(&mut self.state, State::Faulted) {
            self.state = State::Running(worker, None);
            writer
        } else {
            panic!("getwriter called in invalid state");
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<ReceiveWorkerCompleteMessage> for LocalReceiverActor {
    async fn handle(&mut self, ctx: BcContext<'_, Self>, msg: ReceiveWorkerCompleteMessage) {
        self.state = State::Finished(msg.0);
        ctx.stop(None);
    }
}

#[async_trait::async_trait]
impl BcHandler<GetActorStatusMessage> for LocalReceiverActor {
    async fn handle(&mut self, _ctx: BcContext<'_, Self>, _msg: GetActorStatusMessage) -> String {
        self.state.to_string()
    }
}
