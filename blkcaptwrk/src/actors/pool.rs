use super::{container::ContainerActor, dataset::DatasetActor, observation::start_observation};
use crate::xactorext::{GetActorStatusMessage, GetChildActorMessage};
use crate::{
    actorbase::{log_result, schedule_next_message, unhandled_error, unhandled_result},
    xactorext::{BcActor, BcActorCtrl, BcHandler},
};
use anyhow::{bail, Context as _, Result};
use cron::Schedule;
use futures_util::stream::FuturesUnordered;
use futures_util::stream::StreamExt;
use libblkcapt::{
    core::BtrfsPool,
    model::entities::{BtrfsPoolEntity, FeatureState, ObservableEvent},
    model::Entity,
};
use scrub::{PoolScrubActor, ScrubCompleteMessage};
use slog::{debug, error, info, o, Logger};
use std::{collections::HashMap, convert::TryInto, mem, sync::Arc};
use uuid::Uuid;
use xactor::{message, Actor, Addr, Context};

pub struct PoolActor {
    pool: PoolState,
    scrub_schedule: Option<Schedule>,
    datasets: HashMap<Uuid, Addr<BcActor<DatasetActor>>>,
    containers: HashMap<Uuid, Addr<BcActor<ContainerActor>>>,
    log: Logger,
}

enum PoolState {
    Started(Arc<BtrfsPool>, State),
    Pending(BtrfsPoolEntity),
    Faulted,
}

enum State {
    Scrubbing(Addr<BcActor<PoolScrubActor>>),
    Idle,
}

impl PoolState {
    fn take(&mut self) -> Self {
        mem::replace(self, PoolState::Faulted)
    }
}

#[message()]
struct ScrubMessage;

impl PoolActor {
    pub fn new(model: BtrfsPoolEntity, log: &Logger) -> BcActor<Self> {
        BcActor::new(
            Self {
                log: log.new(o!("actor" => "pool", "pool_id" => model.id().to_string())),
                pool: PoolState::Pending(model),
                scrub_schedule: None,
                datasets: HashMap::<_, _>::default(),
                containers: HashMap::<_, _>::default(),
            },
            log,
        )
    }

    fn schedule_next_scrub(&self, log: &Logger, ctx: &mut Context<BcActor<Self>>) {
        schedule_next_message(self.scrub_schedule.as_ref(), "scrub", ScrubMessage, log, ctx);
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for PoolActor {
    async fn started(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>) -> Result<()> {
        let pool = if let PoolState::Pending(model) = self.pool.take() {
            BtrfsPool::validate(model.clone()).map(Arc::new)?
        } else {
            panic!("pool already started");
        };

        self.datasets = pool
            .model()
            .datasets
            .iter()
            .map(|m| (m.id(), DatasetActor::new(ctx.address(), &pool, m.clone(), &self.log)))
            .filter_map(|(id, actor)| match actor {
                Ok(dataset_actor) => Some((id, dataset_actor)),
                Err(error) => {
                    error!(self.log, "Failed to create dataset actor: {}", error);
                    None
                }
            })
            .map(|(id, actor)| async move {
                let addr = actor.start().await?;
                Result::<_>::Ok((id, addr))
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .filter_map(|sa| match sa {
                Ok(started_actor) => Some(started_actor),
                Err(error) => {
                    error!(self.log, "Failed to start dataset actor: {}", error);
                    None
                }
            })
            .collect();

        // How to do more code sharing with above???
        self.containers = pool
            .model()
            .containers
            .iter()
            .map(|m| (m.id(), ContainerActor::new(ctx.address(), &pool, m.clone(), &self.log)))
            .filter_map(|(id, actor)| match actor {
                Ok(container_actor) => Some((id, container_actor)),
                Err(error) => {
                    error!(self.log, "Failed to create container actor: {}", error);
                    None
                }
            })
            .map(|(id, actor)| async move {
                let addr = actor.start().await?;
                Result::<_>::Ok((id, addr))
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .filter_map(|sa| match sa {
                Ok(started_actor) => Some(started_actor),
                Err(error) => {
                    error!(self.log, "Failed to start container actor: {}", error);
                    None
                }
            })
            .collect();

        if pool.model().scrubbing_state() == FeatureState::Enabled {
            self.scrub_schedule = pool
                .model()
                .scrub_schedule
                .as_ref()
                .map_or(Ok(None), |s| s.try_into().map(Some))?;

            self.schedule_next_scrub(log, ctx);
        }

        self.pool = PoolState::Started(pool, State::Idle);
        Ok(())
    }
}

#[async_trait::async_trait]
impl BcHandler<GetChildActorMessage<BcActor<DatasetActor>>> for PoolActor {
    async fn handle(
        &mut self,
        _log: &Logger,
        _ctx: &mut Context<BcActor<Self>>,
        msg: GetChildActorMessage<BcActor<DatasetActor>>,
    ) -> Option<Addr<BcActor<DatasetActor>>> {
        self.datasets.get(&msg.0).cloned()
    }
}

#[async_trait::async_trait]
impl BcHandler<GetChildActorMessage<BcActor<ContainerActor>>> for PoolActor {
    async fn handle(
        &mut self,
        _log: &Logger,
        _ctx: &mut Context<BcActor<Self>>,
        msg: GetChildActorMessage<BcActor<ContainerActor>>,
    ) -> Option<Addr<BcActor<ContainerActor>>> {
        self.containers.get(&msg.0).cloned()
    }
}

#[async_trait::async_trait]
impl BcHandler<ScrubMessage> for PoolActor {
    async fn handle(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>, _msg: ScrubMessage) {
        self.pool = match self.pool.take() {
            PoolState::Started(pool, State::Idle) => {
                let observation = start_observation(pool.model().id(), ObservableEvent::PoolScrub).await;
                let scrub = pool.scrub();
                let scrub_actor = PoolScrubActor::new(ctx.address().downgrade(), scrub, observation, log);
                let start_result = scrub_actor.start().await.context("failed to start scrub actor");
                PoolState::Started(
                    pool,
                    match start_result {
                        Ok(actor) => State::Scrubbing(actor),
                        Err(err) => {
                            unhandled_error(log, err);
                            State::Idle
                        }
                    },
                )
            }
            PoolState::Started(pool, State::Scrubbing(actor)) => {
                info!(log, "skipping scrub. scrub already running");
                PoolState::Started(pool, State::Scrubbing(actor))
            }
            PoolState::Pending(_) | PoolState::Faulted => {
                ctx.stop(None);
                PoolState::Faulted
            }
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<ScrubCompleteMessage> for PoolActor {
    async fn handle(&mut self, _log: &Logger, ctx: &mut Context<BcActor<Self>>, _msg: ScrubCompleteMessage) {
        self.pool = match self.pool.take() {
            PoolState::Started(pool, State::Scrubbing(_)) => PoolState::Started(pool, State::Idle),
            PoolState::Pending(_) | PoolState::Started(..) | PoolState::Faulted => {
                ctx.stop(None);
                PoolState::Faulted
            }
        }
    }
}

#[async_trait::async_trait]
impl BcHandler<GetActorStatusMessage> for PoolActor {
    async fn handle(
        &mut self,
        _log: &Logger,
        _ctx: &mut Context<BcActor<Self>>,
        _msg: GetActorStatusMessage,
    ) -> String {
        String::from("fine")
    }
}

mod scrub {
    use crate::{
        actorbase::unhandled_result,
        actors::observation::StartedObservation,
        tasks::{WorkerCompleteMessage, WorkerTask},
        xactorext::TerminalState,
    };
    use libblkcapt::core::localsndrcv::{PoolScrub, ScrubError};
    use strum_macros::Display;
    use xactor::WeakAddr;

    use super::*;

    #[derive(Display)]
    enum State {
        Created(PoolScrub, StartedObservation),
        Scrubbing(WorkerTask, StartedObservation),
        Scrubbed(Result<(), ScrubError>),
        Faulted,
    }

    impl State {
        fn take(&mut self) -> Self {
            mem::replace(self, State::Faulted)
        }
    }

    pub struct PoolScrubActor {
        parent: WeakAddr<BcActor<PoolActor>>,
        state: State,
    }

    impl PoolScrubActor {
        pub fn new(
            pool: WeakAddr<BcActor<PoolActor>>,
            scrub: PoolScrub,
            observation: StartedObservation,
            log: &Logger,
        ) -> BcActor<Self> {
            BcActor::new(
                Self {
                    parent: pool,
                    state: State::Created(scrub, observation),
                },
                log,
            )
        }
    }

    #[message]
    pub struct ScrubCompleteMessage;

    type ScrubWorkerCompleteMessage = WorkerCompleteMessage<Result<(), ScrubError>>;

    #[async_trait::async_trait]
    impl BcActorCtrl for PoolScrubActor {
        async fn started(&mut self, log: &Logger, ctx: &mut Context<BcActor<Self>>) -> Result<()> {
            if let State::Created(scrub, observation) = self.state.take() {
                let scrub = match scrub.start() {
                    Ok(scrub) => scrub,
                    result => {
                        observation.result(&result);
                        return result.map(|_| ());
                    }
                };
                let task = WorkerTask::run(ctx.address(), log, |_| async move { scrub.wait().await.into() });
                self.state = State::Scrubbing(task, observation);
                Ok(())
            } else {
                panic!("multiple starts")
            }
        }

        async fn stopped(&mut self, log: &Logger, _ctx: &mut Context<BcActor<Self>>) -> TerminalState {
            let terminal_state = match self.state.take() {
                State::Created(_, observation) | State::Scrubbing(_, observation) => {
                    observation.cancelled();
                    TerminalState::Cancelled
                }
                State::Scrubbed(result) => result.into(),
                State::Faulted => TerminalState::Faulted,
            };

            if let Some(actor) = self.parent.upgrade() {
                let pool_notify_result = actor.send(ScrubCompleteMessage);
                if !matches!(terminal_state, TerminalState::Cancelled) {
                    unhandled_result(log, pool_notify_result);
                }
            }

            terminal_state
        }
    }

    #[async_trait::async_trait]
    impl BcHandler<ScrubWorkerCompleteMessage> for PoolScrubActor {
        async fn handle(&mut self, _log: &Logger, ctx: &mut Context<BcActor<Self>>, msg: ScrubWorkerCompleteMessage) {
            ctx.stop(None);
            let result = msg.0;
            self.state = match self.state.take() {
                State::Scrubbing(_, observation) => {
                    observation.result(&result);
                    State::Scrubbed(result)
                }
                State::Faulted | State::Scrubbed(_) | State::Created(..) => State::Faulted,
            }
        }
    }

    #[async_trait::async_trait]
    impl BcHandler<GetActorStatusMessage> for PoolScrubActor {
        async fn handle(
            &mut self,
            _log: &Logger,
            _ctx: &mut Context<BcActor<Self>>,
            _msg: GetActorStatusMessage,
        ) -> String {
            self.state.to_string()
        }
    }
}
