use std::time::Duration;

use anyhow::{anyhow, Error, Result};
use chrono::{DateTime, Utc};
use cron::Schedule;
use slog::{debug, error, info, Logger};
use xactor::{Context, Message};

use crate::xactorext::{BcActor, BcActorCtrl, BcHandler, TerminalState};

pub fn unhandled_error(log: &Logger, error: Error) {
    log_error(log, &error);
}

pub fn unhandled_result<T>(log: &Logger, result: Result<T>) {
    log_result(log, &result);
}

pub fn log_error(log: &Logger, error: &Error) {
    error!(log, "unhandled error"; "error" => %error);
    for cause in error.chain().skip(1) {
        info!(log, "error caused by"; "error" => %cause);
    }
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

fn schedule_next_delay(after: DateTime<Utc>, what: &str, schedule: &Schedule, log: &Logger) -> Option<Duration> {
    match schedule.after(&after).next() {
        Some(next_datetime) => {
            let delay_to_next = (next_datetime - after)
                .to_std()
                .expect("time to next schedule can always fit in std duration");

            let display_delay = Duration::from_secs(delay_to_next.as_secs());

            debug!(
                log,
                "next {} scheduled at {} (in {})",
                what,
                next_datetime,
                humantime::Duration::from(display_delay)
            );
            Some(delay_to_next)
        }
        None => {
            debug!(log, "no next {} in schedule", what);
            None
        }
    }
}

pub fn schedule_next_message<A: BcActorCtrl + BcHandler<M>, M: Message<Result = ()>>(
    schedule: Option<&Schedule>, what: &str, message: M, log: &Logger, ctx: &mut Context<BcActor<A>>,
) {
    if let Some(schedule) = schedule {
        if let Some(delay) = schedule_next_delay(Utc::now(), what, schedule, log) {
            ctx.send_later(message, delay);
        }
    } else {
        panic!("schedule_next_message called when no schedule was configured")
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
