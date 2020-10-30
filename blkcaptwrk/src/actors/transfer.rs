use std::mem;
use anyhow::{bail, Result};
use bytes::BytesMut;
use libblkcapt::{
    core::localsndrcv::{SnapshotReceiver, SnapshotSender, StartedSnapshotReceiver, StartedSnapshotSender},
    core::sync::find_parent,
    core::sync::FindMode,
    core::{localsndrcv::FinishedSnapshotReceiver, sync::find_ready},
    model::entities::{SnapshotSyncEntity, SnapshotSyncMode},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    task::JoinHandle,
};
use xactor::{message, Actor, Context, Handler, Sender};
use crate::{
    actorbase::unhandled_result,
    xactorext::{BcActor, BcActorCtrl, BcHandler},
};
use slog::Logger;

pub struct TransferActor {
    state: State,
    parent: Sender<TransferComplete>,
}

enum State {
    PreStart(SnapshotSender, SnapshotReceiver),
    Started(StartedSnapshotSender, StartedSnapshotReceiver, JoinHandle<()>),
    Finished,
    Stopped,
    Faulted,
}

impl TransferActor {
    pub fn new(parent: Sender<TransferComplete>, sender: SnapshotSender, receiver: SnapshotReceiver, log: &Logger) -> BcActor<Self> {
        BcActor::new(Self {
            state: State::PreStart(sender, receiver),
            parent,
        }, log)
    }
}

#[message()]
pub struct TransferComplete(pub Result<FinishedSnapshotReceiver>);

#[message()]
struct InternalTransferComplete();

#[async_trait::async_trait]
impl BcActorCtrl for TransferActor {
    async fn started(&mut self, _log: &Logger, ctx: &mut Context<BcActor<Self>>) -> Result<()> {
        let (mut sender, mut receiver) =
            if let State::PreStart(sender, receiver) = mem::replace(&mut self.state, State::Faulted) {
                (sender.start()?, receiver.start()?)
            } else {
                bail!("Already started.");
            };

        let mut reader = sender.reader();
        let mut writer = receiver.writer();
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

            let _ = self_addr.send(InternalTransferComplete());
        });

        self.state = State::Started(sender, receiver, handle);

        Ok(())
    }

    async fn stopped(&mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>) {
        self.state = State::Stopped;
    }
}

#[async_trait::async_trait]
impl BcHandler<InternalTransferComplete> for TransferActor {
    async fn handle(&mut self, _log: &Logger, ctx: &mut Context<BcActor<Self>>, _msg: InternalTransferComplete) {
        if let State::Started(sender, receiver, handle) = mem::replace(&mut self.state, State::Finished) {
            let _result = handle.await;
            let send_result = sender.wait().await;
            let receive_result = receiver.wait().await;

            self.parent.send(TransferComplete(send_result.and(receive_result)));
        } else {
            panic!("Received internal complete message on non-Started transfer actor.");
        }
        ctx.stop(None);

        // if let Err(e) = result {
        //     error!("Failed to start sync cycle: {}", e);
        // }
    }
}

//https://docs.rs/futures/0.3.6/futures/future/struct.Abortable.html
//https://docs.rs/futures/0.3.6/futures/future/struct.RemoteHandle.html
