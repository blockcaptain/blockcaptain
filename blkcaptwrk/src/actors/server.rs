use crate::xactorext::{BcActor, BcActorCtrl, BcContext, BcHandler, GetActorStatusMessage, TerminalState};
use anyhow::Result;
use futures_util::{FutureExt, TryFutureExt};
use libblkcapt::runtime_dir;
use slog::Logger;
use tokio::{net::UnixListener, sync::oneshot, task::JoinHandle};
use tokio_stream::wrappers::UnixListenerStream;
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
        let (sender, receiver) = oneshot::channel::<()>();
        let signal = receiver.map(|_| ());

        let runtime_dir = runtime_dir();
        std::fs::create_dir_all(&runtime_dir)?;

        let socket_path = runtime_dir.with_file_name("daemon.sock");
        if socket_path.exists() {
            std::fs::remove_file(&socket_path)?;
        }
        let listener = UnixListener::bind(socket_path)?;
        let handle = tokio::spawn(async move {
            let incoming = UnixListenerStream::new(listener);

            let routes = warp::any().and_then(|| async {
                let addr = IntelActor::addr();
                let state = addr
                    .call(GetStateMessage)
                    .and_then(|fut| fut.map(Ok))
                    .await
                    .map_err(|_| warp::reject())?;
                Ok::<_, Rejection>(warp::reply::json(&state))
            });

            warp::serve(routes)
                .serve_incoming_with_graceful_shutdown(incoming, signal)
                .await;
        });
        self.server = Some((handle, sender));
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
