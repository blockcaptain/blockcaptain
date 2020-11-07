use crate::{
    actorbase::{unhandled_error, unhandled_result},
    xactorext::{BcActor, BcActorCtrl, BcHandler},
};
use anyhow::{bail, Result};
use bytes::BytesMut;
use libblkcapt::{
    core::localsndrcv::{SnapshotReceiver, SnapshotSender, StartedSnapshotReceiver, StartedSnapshotSender},
    core::sync::find_parent,
    core::sync::find_ready,
    core::sync::FindMode,
    model::entities::{SnapshotSyncEntity, SnapshotSyncMode},
};
use slog::{debug, error, Logger};
use std::mem;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    task::JoinHandle,
};
use xactor::{message, Actor, Addr, Context, Handler, Sender};

use super::{
    container::ReceiverReadyMessage,
    dataset::SenderReadyMessage,
    localreceiver::GetWriterMessage,
    localreceiver::LocalReceiverActor,
    localreceiver::ReceiverFinishedMessage,
    localsender::GetReaderMessage,
    localsender::{LocalSenderActor, LocalSenderFinishedMessage},
};

pub struct TransferActor {
    state: State,
    parent: Sender<TransferComplete>,
}

#[derive(Default)]
struct ActorCompletions {
    sender: Option<Result<()>>,
    receiver: Option<Result<()>>,
}

struct Actors(Addr<BcActor<LocalSenderActor>>, Addr<BcActor<LocalReceiverActor>>);

enum State {
    WaitingForActors(
        Option<Addr<BcActor<LocalSenderActor>>>,
        Option<Addr<BcActor<LocalReceiverActor>>>,
    ),
    Transferring(JoinHandle<()>, ActorCompletions, Actors),
    WaitingForCompletion(Result<()>, ActorCompletions, Actors),
    Finished,
    Faulted,
}

impl TransferActor {
    pub fn new(parent: Sender<TransferComplete>, log: &Logger) -> BcActor<Self> {
        BcActor::new(
            Self {
                state: State::WaitingForActors(None, None),
                parent,
            },
            log,
        )
    }

    async fn start_transfer(
        ctx: &mut Context<BcActor<Self>>,
        sender_actor: &Addr<BcActor<LocalSenderActor>>,
        receiver_actor: &Addr<BcActor<LocalReceiverActor>>,
    ) -> Result<JoinHandle<()>> {
        let mut reader = sender_actor.call(GetReaderMessage).await?;
        let mut writer = receiver_actor.call(GetWriterMessage).await?;
        let self_addr = ctx.address();

        let handle = tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(1024 * 256);
            while let Ok(size) = reader.read_buf(&mut buf).await {
                if size == 0 {
                    break;
                }
                writer.write_all(&buf).await.unwrap();
                buf.clear();
            }

            let _ = self_addr.send(InternalTransferComplete(Ok(())));
        });

        Ok(handle)
    }

    fn finish_transfer(
        &self,
        ctx: &mut Context<BcActor<Self>>,
        transfer_result: Result<()>,
        sender_result: Result<()>,
        receiver_result: Result<()>,
    ) {
        let result = transfer_result.and(sender_result).and(receiver_result);
        self.parent.send(TransferComplete(result));
        ctx.stop(None);
    }
}

#[message()]
pub struct TransferComplete(pub Result<()>);

#[message()]
struct InternalTransferComplete(Result<()>);

#[async_trait::async_trait]
impl BcActorCtrl for TransferActor {
    async fn started(&mut self, _log: &Logger, ctx: &mut Context<BcActor<Self>>) -> Result<()> {
        Ok(())
    }

    async fn stopped(&mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>) {}
}

#[async_trait::async_trait]
impl BcHandler<SenderReadyMessage> for TransferActor {
    async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: SenderReadyMessage) {
        match (mem::replace(&mut self.state, State::Faulted), msg.0) {
            (State::WaitingForActors(None, None), Ok(sender)) => {
                self.state = State::WaitingForActors(Some(sender), None)
            }
            (State::WaitingForActors(None, Some(receiver)), Ok(sender)) => {
                match Self::start_transfer(ctx, &sender, &receiver).await {
                    Ok(handle) => {
                        self.state = State::Transferring(handle, Default::default(), Actors(sender, receiver));
                    }
                    Err(e) => {
                        unhandled_error(log, e);
                    }
                }
            }
            (State::WaitingForActors(_, _), Err(e)) => {
                // deal with error
            }
            _ => error!(log, "cant handle message in current state"),
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<ReceiverReadyMessage> for TransferActor {
    async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: ReceiverReadyMessage) {
        match (mem::replace(&mut self.state, State::Faulted), msg.0) {
            (State::WaitingForActors(None, None), Ok(receiver)) => {
                self.state = State::WaitingForActors(None, Some(receiver))
            }
            (State::WaitingForActors(Some(sender), None), Ok(receiver)) => {
                match Self::start_transfer(ctx, &sender, &receiver).await {
                    Ok(handle) => {
                        self.state = State::Transferring(handle, Default::default(), Actors(sender, receiver));
                    }
                    Err(e) => {
                        unhandled_error(log, e);
                    }
                }
            }
            (State::WaitingForActors(_, _), Err(e)) => {
                // deal with error
            }
            _ => error!(log, "cant handle message in current state"),
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<LocalSenderFinishedMessage> for TransferActor {
    async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: LocalSenderFinishedMessage) {
        match mem::replace(&mut self.state, State::Faulted) {
            State::Transferring(transfer, completions, actors) => {
                self.state = State::Transferring(
                    transfer,
                    ActorCompletions {
                        sender: Some(msg.0),
                        ..completions
                    },
                    actors,
                )
            }
            State::WaitingForCompletion(transfer, completions, actors) => {
                let completions = ActorCompletions {
                    sender: Some(msg.0),
                    ..completions
                };
                match completions {
                    ActorCompletions {
                        sender: Some(sender),
                        receiver: Some(receiver),
                    } => {
                        self.finish_transfer(ctx, transfer, sender, receiver);
                        self.state = State::Finished;
                    }
                    c => {
                        self.state = State::WaitingForCompletion(transfer, c, actors);
                    }
                }
            }
            _ => error!(log, "cant handle message in current state"),
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<ReceiverFinishedMessage> for TransferActor {
    async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: ReceiverFinishedMessage) {
        match mem::replace(&mut self.state, State::Faulted) {
            State::Transferring(transfer, completions, actors) => {
                self.state = State::Transferring(
                    transfer,
                    ActorCompletions {
                        receiver: Some(msg.0),
                        ..completions
                    },
                    actors,
                )
            }
            State::WaitingForCompletion(transfer, completions, actors) => {
                let completions = ActorCompletions {
                    receiver: Some(msg.0),
                    ..completions
                };
                match completions {
                    ActorCompletions {
                        sender: Some(sender),
                        receiver: Some(receiver),
                    } => {
                        self.finish_transfer(ctx, transfer, sender, receiver);
                        self.state = State::Finished;
                    }
                    c => {
                        self.state = State::WaitingForCompletion(transfer, c, actors);
                    }
                }
            }
            _ => error!(log, "cant handle message in current state"),
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<InternalTransferComplete> for TransferActor {
    async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: InternalTransferComplete) {
        match mem::replace(&mut self.state, State::Faulted) {
            State::Transferring(_, completions, actors) => {
                let transfer = msg.0;
                match completions {
                    ActorCompletions {
                        sender: Some(sender),
                        receiver: Some(receiver),
                    } => {
                        self.finish_transfer(ctx, transfer, sender, receiver);
                        self.state = State::Finished;
                    }
                    completions => {
                        self.state = State::WaitingForCompletion(transfer, completions, actors);
                    }
                }
            }
            _ => error!(log, "cant handle message in current state"),
        }
    }
}

//https://docs.rs/futures/0.3.6/futures/future/struct.Abortable.html
//https://docs.rs/futures/0.3.6/futures/future/struct.RemoteHandle.html
