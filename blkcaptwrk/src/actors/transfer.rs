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
use log::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    task::JoinHandle,
};

use xactor::{message, Actor, Context, Handler, Sender};

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
    pub fn new(parent: Sender<TransferComplete>, sender: SnapshotSender, receiver: SnapshotReceiver) -> Self {
        Self {
            state: State::PreStart(sender, receiver),
            parent,
        }
    }
}

#[message()]
pub struct TransferComplete(pub Result<FinishedSnapshotReceiver>);

#[message()]
struct InternalTransferComplete();

#[async_trait::async_trait]
impl Actor for TransferActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        trace!("SYNC ACTOR START");
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

    async fn stopped(&mut self, _ctx: &mut Context<Self>) {
        trace!("Transfer ACTOR STOP");
        self.state = State::Stopped;
    }
}

#[async_trait::async_trait]
impl Handler<InternalTransferComplete> for TransferActor {
    async fn handle(&mut self, ctx: &mut Context<Self>, _msg: InternalTransferComplete) {
        trace!("Transfer actor internal transfer complete message.");

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
