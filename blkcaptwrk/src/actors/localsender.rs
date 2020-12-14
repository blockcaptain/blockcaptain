use crate::xactorext::{BcActor, BcActorCtrl, BcHandler, GetActorStatusMessage};
use anyhow::Result;
use libblkcapt::core::localsndrcv::SnapshotSender;
use slog::Logger;
use std::mem;
use tokio::io::AsyncRead;
use xactor::{message, Context, Sender};

#[message()]
pub struct LocalSenderFinishedMessage(pub u64, pub Result<()>);

#[message(result = "Box<dyn AsyncRead + Send + Unpin>")]
pub struct GetReaderMessage;

pub struct LocalSenderActor {
    parent: Sender<LocalSenderFinishedMessage>,
    requestor: Sender<LocalSenderFinishedMessage>,
    state: State,
}

enum State {
    Created(SnapshotSender),
    Started(Option<Box<dyn AsyncRead + Send + Unpin>>),
    Finished,
    Faulted,
}

#[message()]
struct InternalSenderFinished(Result<()>);

impl LocalSenderActor {
    pub fn new(
        parent: Sender<LocalSenderFinishedMessage>,
        requestor: Sender<LocalSenderFinishedMessage>,
        sender: SnapshotSender,
        log: &Logger,
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
    async fn started(&mut self, _log: &Logger, ctx: &mut Context<BcActor<Self>>) -> Result<()> {
        if let State::Created(sender) = mem::replace(&mut self.state, State::Faulted) {
            let mut sender = sender.start()?;
            let reader = sender.reader();
            let internal_callback = ctx.address().sender();
            tokio::spawn(async move {
                // TODO cancellation. tokio 0.3 has better process api.
                let result = sender.wait().await;
                let _ = internal_callback.send(InternalSenderFinished(result));
            });
            self.state = State::Started(Some(Box::new(reader)));
        } else {
            panic!("start called in invalid state");
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl BcHandler<GetReaderMessage> for LocalSenderActor {
    async fn handle(
        &mut self,
        _log: &Logger,
        _ctx: &mut Context<BcActor<Self>>,
        _msg: GetReaderMessage,
    ) -> Box<dyn AsyncRead + Send + Unpin> {
        if let State::Started(Some(reader)) = mem::replace(&mut self.state, State::Faulted) {
            self.state = State::Started(None);
            reader
        } else {
            panic!("getreader called in invalid state");
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<InternalSenderFinished> for LocalSenderActor {
    async fn handle(&mut self, _log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: InternalSenderFinished) {
        self.state = State::Finished;
        self.parent
            .send(LocalSenderFinishedMessage(ctx.actor_id(), Ok(())))
            .expect("FIXME");
        self.requestor
            .send(LocalSenderFinishedMessage(ctx.actor_id(), msg.0))
            .expect("FIXME");
    }
}

#[async_trait::async_trait]
impl BcHandler<GetActorStatusMessage> for LocalSenderActor {
    async fn handle(
        &mut self,
        _log: &Logger,
        _ctx: &mut Context<BcActor<Self>>,
        _msg: GetActorStatusMessage,
    ) -> String {
        String::from("ok")
    }
}
