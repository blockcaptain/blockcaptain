use crate::xactorext::{BcActor, BcActorCtrl, BcContext, BcHandler, GetActorStatusMessage, TerminalState};
use anyhow::Result;
use futures_util::{FutureExt, TryFutureExt};
use slog::Logger;
use std::path::PathBuf;
use tokio::{net::UnixListener, sync::oneshot, task::JoinHandle};
use warp::{Filter, Rejection};

use super::intel::{GetStateMessage, IntelActor};

pub struct ServerActor {
    server: Option<(JoinHandle<()>, oneshot::Sender<()>)>,
}

impl ServerActor {
    pub fn new(log: &Logger) -> BcActor<Self> {
        BcActor::new(Self { server: None }, log)
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for ServerActor {
    async fn started(&mut self, _ctx: BcContext<'_, Self>) -> Result<()> {
        // TODO_ON_TOKIO03
        // let (sender, receiver) = oneshot::channel::<()>();
        // let signal = receiver.map(|_| ());

        // let socket_path = PathBuf::from("/var/lib/blkcapt/wrk.sock");
        // if socket_path.exists() {
        //     std::fs::remove_file(&socket_path)?;
        // }
        // let mut listener = UnixListener::bind(socket_path)?;

        // let handle = tokio::spawn(async move {
        //     let incoming = listener;

        //     let routes = warp::any().and_then(|| async {
        //         let addr = IntelActor::addr();
        //         let state = addr
        //             .call(GetStateMessage)
        //             .and_then(|fut| fut.map(Ok))
        //             .await
        //             .map_err(|_| warp::reject())?;
        //         Ok::<_, Rejection>(warp::reply::json(&state))
        //     });

        //     warp::serve(routes)
        //         .serve_incoming_with_graceful_shutdown(incoming, signal)
        //         .await;
        // });
        // self.server = Some((handle, sender));
        Ok(())
    }

    async fn stopped(&mut self, _ctx: BcContext<'_, Self>) -> TerminalState {
        if let Some((handle, sender)) = self.server.take() {
            if sender.send(()).is_ok() {
                let _ = handle.await;
            }
        }

        TerminalState::Succeeded
    }
}

#[async_trait::async_trait]
impl BcHandler<GetActorStatusMessage> for ServerActor {
    async fn handle(&mut self, _ctx: BcContext<'_, Self>, _msg: GetActorStatusMessage) -> String {
        String::from("listening")
    }
}
