pub mod slogext;
use anyhow::Result;
use libblkcapt::{error_cause, model::BcLogLevel};
use slog::{debug, error, o, trace, Drain, Level, Logger};
use slogext::{DedupDrain, SlogLogLogger};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::runtime::Runtime;

pub fn blkcaptapp_run<M, F>(main: M, log_level: BcLogLevel, slog_drain: slog_atomic::AtomicSwitch<()>) -> i32
where
    M: FnOnce(Logger) -> F,
    F: Future<Output = Result<()>>,
{
    let (internal_level, external_level_slog, external_level) = match log_level {
        BcLogLevel::Info => (Level::Info, Level::Info, log::LevelFilter::Info),
        BcLogLevel::Debug => (Level::Debug, Level::Info, log::LevelFilter::Info),
        BcLogLevel::Trace => (Level::Trace, Level::Info, log::LevelFilter::Info),
        BcLogLevel::TraceXdebug => (Level::Trace, Level::Debug, log::LevelFilter::Debug),
        BcLogLevel::TraceXtrace => (Level::Trace, Level::Trace, log::LevelFilter::Trace),
    };

    println!();

    let mut app_succeeded = true;

    {
        let slog_drain_ctrl = slog_drain.ctrl();
        let slog_drain = slog_drain.map(Arc::new);

        {
            let slog_internal_logger = {
                let drain = DedupDrain::new(Arc::clone(&slog_drain));
                let drain = drain.filter_level(internal_level).fuse();
                Logger::root(drain, o!())
            };

            let slog_external_logger = {
                let drain = Arc::clone(&slog_drain);
                let drain = drain.filter_level(external_level_slog).fuse();
                Logger::root(drain, o!())
            };

            slog_scope::set_global_logger(slog_internal_logger.clone()).cancel_reset();
            SlogLogLogger::install(slog_external_logger, external_level);

            debug!(slog_internal_logger, "debug messages enabled");
            trace!(slog_internal_logger, "trace messages enabled");
            debug!(slog_internal_logger, "process starting");

            {
                let runtime = Runtime::new().expect("can create runtime");
                let result = runtime.block_on(main(slog_internal_logger.clone()));
                if let Err(e) = result {
                    error!(slog_internal_logger, "{}", e);
                    error!(slog_internal_logger, "{}", error_cause(&e));
                    app_succeeded = false;
                }
                runtime.shutdown_timeout(Duration::from_secs(0));
            }

            debug!(slog_internal_logger, "process stopping");

            slog_scope::set_global_logger(Logger::root(slog::Discard, o!())).cancel_reset();
        }

        slog_drain_ctrl.set(Logger::root(slog::Discard, o!()));
    }

    println!();

    if app_succeeded {
        0
    } else {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::slogext::CustomFullFormat;
    use slog::info;

    #[test]
    fn runs_app() {
        let decorator = slog_term::PlainSyncDecorator::new(std::io::stdout());
        let drain = CustomFullFormat::new(decorator, false).fuse();
        let drain = slog_atomic::AtomicSwitch::new(drain);
        blkcaptapp_run(
            |log| async move {
                info!(log, "runs_app test");
                Ok(())
            },
            BcLogLevel::Info,
            drain,
        );
    }
}
