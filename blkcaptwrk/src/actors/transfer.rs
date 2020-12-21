use super::{
    container::ReceiverReadyMessage,
    dataset::SenderReadyMessage,
    localreceiver::GetWriterMessage,
    localreceiver::LocalReceiverActor,
    localreceiver::LocalReceiverStoppedMessage,
    localsender::GetReaderMessage,
    localsender::{LocalSenderActor, LocalSenderFinishedMessage},
    observation::StartedObservation,
};
use crate::{
    actorbase::unhandled_result,
    tasks::{WorkerCompleteMessage, WorkerTask},
    xactorext::{BcActor, BcActorCtrl, BcHandler, GetActorStatusMessage, TerminalState},
};
use anyhow::Result;
use bytes::BytesMut;
use derive_more::From;
use slog::{debug, error, warn, Logger};
use std::mem;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use xactor::{message, Addr, Context, Sender};

pub struct TransferActor {
    requestor: Sender<TransferComplete>,
    state: State,
}

#[derive(Default)]
struct ActorCompletions {
    sender: Option<Result<()>>,
    receiver: Option<Result<()>>,
    transfer: Option<Result<()>>,
}

struct Actors(
    WorkerTask,
    Addr<BcActor<LocalSenderActor>>,
    Addr<BcActor<LocalReceiverActor>>,
);

enum State {
    WaitingForActors(
        Option<Addr<BcActor<LocalSenderActor>>>,
        Option<Addr<BcActor<LocalReceiverActor>>>,
        StartedObservation,
    ),
    Transferring(ActorCompletions, Actors, StartedObservation),
    Transferred(Result<()>),
    Faulted,
}

impl State {
    fn take(&mut self) -> Self {
        mem::replace(self, State::Faulted)
    }
}

type TransferWorkerCompleteMessage = WorkerCompleteMessage<Result<()>>;

impl TransferActor {
    pub fn new(parent: Sender<TransferComplete>, observation: StartedObservation, log: &Logger) -> BcActor<Self> {
        BcActor::new(
            Self {
                state: State::WaitingForActors(None, None, observation),
                requestor: parent,
            },
            log,
        )
    }

    async fn run_transfer(
        sender_actor: Addr<BcActor<LocalSenderActor>>, receiver_actor: Addr<BcActor<LocalReceiverActor>>,
    ) -> Result<()> {
        let mut reader = sender_actor.call(GetReaderMessage).await?;
        let mut writer = receiver_actor.call(GetWriterMessage).await?;

        let mut buf = BytesMut::with_capacity(1024 * 256);
        while let Ok(size) = reader.read_buf(&mut buf).await {
            if size == 0 {
                break;
            }
            writer.write_all(&buf).await.unwrap();
            buf.clear();
        }

        Ok(())
    }

    fn maybe_start_transfer(incoming: State, ctx: &mut Context<BcActor<Self>>, log: &Logger) -> State {
        if let State::WaitingForActors(Some(sender), Some(receiver), observation) = incoming {
            let mv_sender = sender.clone();
            let mv_receiver = receiver.clone();
            let task = WorkerTask::run(ctx.address(), log, |_| async move {
                Self::run_transfer(mv_sender, mv_receiver).await.into()
            });
            State::Transferring(Default::default(), Actors(task, sender, receiver), observation)
        } else {
            incoming
        }
    }

    fn input_ready(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, input: InputReady) {
        self.state = match (self.state.take(), input) {
            (State::WaitingForActors(maybe_sender, None, observation), InputReady::Receiver(Ok(receiver))) => {
                let updated_state = State::WaitingForActors(maybe_sender, Some(receiver), observation);
                Self::maybe_start_transfer(updated_state, ctx, log)
            }
            (State::WaitingForActors(None, maybe_receiver, observation), InputReady::Sender(Ok(sender))) => {
                let updated_state = State::WaitingForActors(Some(sender), maybe_receiver, observation);
                Self::maybe_start_transfer(updated_state, ctx, log)
            }
            (State::WaitingForActors(_, None, observation), InputReady::Receiver(Err(e)))
            | (State::WaitingForActors(None, _, observation), InputReady::Sender(Err(e))) => {
                ctx.stop(None);
                observation.error::<anyhow::Error, _>(&e);
                State::Transferred(Err(e))
            }
            _ => {
                ctx.stop(None);
                State::Faulted
            }
        };
    }

    fn actor_ready(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, result_ready: ResultReady) {
        self.state = match (self.state.take(), result_ready) {
            (State::Transferring(mut completions, actors, observation), ResultReady::Sender(result))
                if completions.sender.is_none() =>
            {
                completions.sender = Some(result);
                Self::maybe_finish_transfer(State::Transferring(completions, actors, observation), log, ctx)
            }
            (State::Transferring(mut completions, actors, observation), ResultReady::Receiver(result))
                if completions.receiver.is_none() =>
            {
                completions.receiver = Some(result);
                Self::maybe_finish_transfer(State::Transferring(completions, actors, observation), log, ctx)
            }
            (State::Transferring(mut completions, actors, observation), ResultReady::Transfer(result))
                if completions.transfer.is_none() =>
            {
                completions.transfer = Some(result);
                Self::maybe_finish_transfer(State::Transferring(completions, actors, observation), log, ctx)
            }
            _ => {
                ctx.stop(None);
                State::Faulted
            }
        };
    }

    fn maybe_finish_transfer(incoming: State, _log: &Logger, ctx: &mut Context<BcActor<Self>>) -> State {
        if let State::Transferring(
            ActorCompletions {
                sender: Some(sender),
                receiver: Some(receiver),
                transfer: Some(transfer),
            },
            _,
            observation,
        ) = incoming
        {
            let result = transfer.and(sender).and(receiver);
            ctx.stop(None);
            observation.result(&result);
            State::Transferred(result)
        } else {
            incoming
        }
    }
}

#[derive(From)]
enum InputReady {
    Sender(Result<Addr<BcActor<LocalSenderActor>>>),
    Receiver(Result<Addr<BcActor<LocalReceiverActor>>>),
}

enum ResultReady {
    Sender(Result<()>),
    Receiver(Result<()>),
    Transfer(Result<()>),
}

#[message()]
pub struct TransferComplete(pub TerminalState);

#[async_trait::async_trait]
impl BcActorCtrl for TransferActor {
    async fn started(&mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>) -> Result<()> {
        Ok(())
    }

    async fn stopped(&mut self, log: &Logger, _ctx: &mut Context<BcActor<Self>>) -> TerminalState {
        let terminal_state = match self.state.take() {
            State::Transferring(_, mut actors, observation) => {
                warn!(log, "cancelled during transfer");
                actors.0.abort();
                debug!(log, "waiting for worker");
                actors.0.wait().await;
                observation.cancelled();
                let _ = actors.1.stop(None);
                let _ = actors.2.stop(None);
                TerminalState::Cancelled
            }
            State::WaitingForActors(.., observation) => {
                warn!(log, "cancelled prior to transfer");
                observation.cancelled();
                TerminalState::Cancelled
            }
            State::Transferred(result) => result.as_ref().into(),
            State::Faulted => {
                error!(log, "actor faulted");
                TerminalState::Faulted
            }
        };

        let requestor_notify_result = self.requestor.send(TransferComplete(terminal_state));
        if !matches!(terminal_state, TerminalState::Cancelled) {
            unhandled_result(log, requestor_notify_result);
        }
        terminal_state
    }
}

#[async_trait::async_trait]
impl BcHandler<TransferWorkerCompleteMessage> for TransferActor {
    async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: TransferWorkerCompleteMessage) {
        self.actor_ready(log, ctx, ResultReady::Transfer(msg.0));
    }
}

#[async_trait::async_trait]
impl BcHandler<SenderReadyMessage> for TransferActor {
    async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: SenderReadyMessage) {
        self.input_ready(log, ctx, msg.0.into());
    }
}

#[async_trait::async_trait]
impl BcHandler<ReceiverReadyMessage> for TransferActor {
    async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: ReceiverReadyMessage) {
        self.input_ready(log, ctx, msg.0.into())
    }
}

#[async_trait::async_trait]
impl BcHandler<LocalSenderFinishedMessage> for TransferActor {
    async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: LocalSenderFinishedMessage) {
        self.actor_ready(log, ctx, ResultReady::Sender(msg.0));
    }
}

#[async_trait::async_trait]
impl BcHandler<LocalReceiverStoppedMessage> for TransferActor {
    async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: LocalReceiverStoppedMessage) {
        self.actor_ready(log, ctx, ResultReady::Receiver(msg.0));
    }
}

#[async_trait::async_trait]
impl BcHandler<GetActorStatusMessage> for TransferActor {
    async fn handle(
        &mut self, _log: &Logger, _ctx: &mut Context<BcActor<Self>>, _msg: GetActorStatusMessage,
    ) -> String {
        String::from("ok")
    }
}
