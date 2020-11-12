use crate::{
    actorbase::unhandled_result,
    xactorext::{BcActor, BcActorCtrl, BcHandler},
};
use anyhow::Result;
use libblkcapt::core::{localsndrcv::SnapshotReceiver, BtrfsContainerSnapshot};
use slog::Logger;
use std::mem;
use tokio::io::AsyncWrite;
use xactor::{message, Caller, Context, Sender};

#[message()]
pub struct ReceiverFinishedMessage(pub Result<()>);

#[message()]
pub struct ReceiverFinishedParentMessage(pub u64, pub Result<BtrfsContainerSnapshot>);

#[message(result = "Box<dyn AsyncWrite + Send + Unpin>")]
pub struct GetWriterMessage;

pub struct LocalReceiverActor {
    parent: Caller<ReceiverFinishedParentMessage>,
    requestor: Sender<ReceiverFinishedMessage>,
    state: State,
}

enum State {
    Created(SnapshotReceiver),
    Started(Option<Box<dyn AsyncWrite + Send + Unpin>>),
    Finished,
    Faulted,
}

#[message()]
struct InternalReceiverFinished(Result<BtrfsContainerSnapshot>);

impl LocalReceiverActor {
    pub fn new(
        parent: Caller<ReceiverFinishedParentMessage>,
        requestor: Sender<ReceiverFinishedMessage>,
        receiver: SnapshotReceiver,
        log: &Logger,
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
    async fn started(&mut self, _log: &Logger, ctx: &mut Context<BcActor<Self>>) -> Result<()> {
        if let State::Created(receiver) = mem::replace(&mut self.state, State::Faulted) {
            let mut receiver = receiver.start()?;
            let reader = receiver.writer();
            let internal_callback = ctx.address().sender();
            tokio::spawn(async move {
                // TODO cancellation. tokio 0.3 has better process api.
                let result = receiver.wait().await;
                internal_callback.send(InternalReceiverFinished(result));
            });
            self.state = State::Started(Some(Box::new(reader)));
        } else {
            panic!("start called in invalid state");
        }
        Ok(())
    }

    async fn stopped(&mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>) {}
}

#[async_trait::async_trait]
impl BcHandler<GetWriterMessage> for LocalReceiverActor {
    async fn handle(
        &mut self,
        _log: &Logger,
        _ctx: &mut Context<BcActor<Self>>,
        _msg: GetWriterMessage,
    ) -> Box<dyn AsyncWrite + Send + Unpin> {
        if let State::Started(Some(reader)) = mem::replace(&mut self.state, State::Faulted) {
            self.state = State::Started(None);
            reader
        } else {
            panic!("getreader called in invalid state");
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<InternalReceiverFinished> for LocalReceiverActor {
    async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: InternalReceiverFinished) {
        self.state = State::Finished;
        let parent_notify_result = self
            .parent
            .call(ReceiverFinishedParentMessage(ctx.actor_id(), msg.0))
            .await;
        unhandled_result(log, parent_notify_result);

        // TODO need proper error sent below.
        let requestor_notify_result = self.requestor.send(ReceiverFinishedMessage(Ok(())));
        unhandled_result(log, requestor_notify_result);
    }
}
