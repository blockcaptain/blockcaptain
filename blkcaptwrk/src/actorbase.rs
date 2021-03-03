use crate::xactorext::{BcActorCtrl, BcContext, BcHandler, TerminalState};
use anyhow::{anyhow, Error, Result};
use chrono::{DateTime, Utc};
use cron::Schedule;
use futures_util::{
    future,
    stream::{FuturesUnordered, StreamExt},
};
use libblkcapt::{
    error_cause,
    model::{Entity, EntityId, EntityStatic},
};
use slog::{debug, error, Logger};
use std::future::Future;
use std::{collections::HashMap, time::Duration};
use xactor::{Actor, Addr, Message};

pub fn unhandled_error(log: &Logger, error: Error) {
    log_error(log, &error);
}

pub fn unhandled_result<T>(log: &Logger, result: Result<T>) {
    log_result(log, &result);
}

pub fn log_error(log: &Logger, error: &Error) {
    error!(log, "unhandled error"; "error" => %error, "cause" => error_cause(error));
}

pub fn log_result<T>(log: &Logger, result: &Result<T>) {
    let _ = result.as_ref().map_err(|e| log_error(log, e));
}

pub fn logged_error(log: &Logger, error: Error) -> Error {
    log_error(log, &error);
    error
}

pub fn logged_result<T>(log: &Logger, result: Result<T>) -> Result<T> {
    log_result(log, &result);
    result
}

fn schedule_next_delay(schedule: &Schedule, after: DateTime<Utc>) -> Option<(DateTime<Utc>, Duration)> {
    schedule.after(&after).next().map(|next_datetime| {
        let delay_to_next = (next_datetime - after)
            .to_std()
            .expect("time to next schedule can always fit in std duration");
        (next_datetime, delay_to_next)
    })
}

pub struct ScheduledMessage {}

impl ScheduledMessage {
    pub fn new<M: Message<Result = ()> + Clone, A: BcHandler<M> + BcActorCtrl, S: Into<String>>(
        schedule: Schedule, what: S, message: M, ctx: &BcContext<'_, A>,
    ) -> Self {
        let sender = ctx.address().sender();
        let what = what.into();
        let log = ctx.log().clone();
        tokio::spawn(async move {
            loop {
                if let Some((next_datetime, interval)) = schedule_next_delay(&schedule, Utc::now()) {
                    let display_delay = Duration::from_secs(interval.as_secs());
                    debug!(
                        log,
                        "next {} scheduled at {} (in {})",
                        what,
                        next_datetime,
                        humantime::Duration::from(display_delay)
                    );
                    tokio::time::sleep(interval).await;
                    if sender.send(message.clone()).is_err() {
                        break;
                    }
                } else {
                    debug!(log, "no next {} in schedule", what);
                }
            }
        });
        Self {}
    }
}

impl<T> From<TerminalState> for Result<T> {
    fn from(ts: TerminalState) -> Self {
        match ts {
            TerminalState::Succeeded => panic!("TerminalState::Succeeded can't be converted to Result"),
            TerminalState::Failed => Err(anyhow!("actor failed")),
            TerminalState::Cancelled => Err(anyhow!("actor cancelled")),
            TerminalState::Faulted => Err(anyhow!("actor faulted")),
        }
    }
}

pub fn state_result<T>(state: TerminalState) -> (TerminalState, Result<T>) {
    (state, state.into())
}

pub fn state_result_from_result<T>(result: Result<T>) -> (TerminalState, Result<T>) {
    (
        match &result {
            Ok(_) => TerminalState::Succeeded,
            Err(_) => TerminalState::Failed,
        },
        result,
    )
}

pub async fn build_child_actors<'a, S, A, M, IM, B, BR>(
    ctx: &BcContext<'_, S>, models: IM, builder: B,
) -> HashMap<EntityId, Addr<A>>
where
    BR: Future<Output = Result<A>>,
    B: Fn(&M) -> BR,
    IM: Iterator<Item = &'a M>,
    M: 'a + Entity + EntityStatic,
    A: Actor,
    S: BcActorCtrl,
{
    models
        .map(|m| {
            let m = m;
            let builder = &builder;
            async move {
                let maybe_actor = builder(m).await;
                match maybe_actor {
                    Ok(actor) => match actor.start().await {
                        Ok(started_actor) => Some((m.id(), started_actor)),
                        Err(error) => {
                            logged_error(
                                ctx.log(),
                                error.context(format!(
                                    "failed to start {} actor '{}'",
                                    M::entity_type_static(),
                                    m.name()
                                )),
                            );
                            None
                        }
                    },
                    Err(error) => {
                        logged_error(
                            ctx.log(),
                            error.context(format!(
                                "failed to create {} actor '{}",
                                M::entity_type_static(),
                                m.name()
                            )),
                        );
                        None
                    }
                }
            }
        })
        .collect::<FuturesUnordered<_>>()
        .filter_map(future::ready)
        .collect::<HashMap<_, _>>()
        .await
}
