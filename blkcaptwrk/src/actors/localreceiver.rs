use crate::xactorext::{BcActor, BcActorCtrl, BcHandler};
use anyhow::Result;
use libblkcapt::core::{
    localsndrcv::{SnapshotReceiver, StartedSnapshotReceiver},
    BtrfsContainerSnapshot,
};
use slog::Logger;
use std::mem;
use tokio::io::AsyncWrite;
use xactor::{message, Context, Sender};

#[message()]
pub struct LocalReceiverFinishedMessage(pub Result<()>);

#[message(result = "Box<dyn AsyncWrite + Send + Unpin>")]
pub struct GetWriterMessage;

pub struct LocalReceiverActor {
    parent: Sender<LocalReceiverFinishedMessage>,
    requestor: Sender<LocalReceiverFinishedMessage>,
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
        parent: Sender<LocalReceiverFinishedMessage>,
        requestor: Sender<LocalReceiverFinishedMessage>,
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
        ctx: &mut Context<BcActor<Self>>,
        msg: GetWriterMessage,
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
    async fn handle(&mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>, msg: InternalReceiverFinished) {
        self.state = State::Finished;
        self.parent.send(LocalReceiverFinishedMessage(Ok(()))).expect("FIXME");
        self.requestor
            .send(LocalReceiverFinishedMessage(Ok(())))
            .expect("FIXME");
    }
}
