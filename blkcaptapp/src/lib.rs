pub mod slogext;
use anyhow::Result;
use slog::{debug, error, info, o, trace, Drain, Level, Logger};
use slogext::{DedupDrain, SlogLogLogger};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::runtime::Runtime;

pub fn blkcaptapp_run<M, F>(main: M, verbose_flag_count: usize, slog_drain: slog_atomic::AtomicSwitch<()>) -> i32
where
    M: FnOnce(Logger) -> F,
    F: Future<Output = Result<()>>,
{
    let (internal_level, external_level_slog, external_level) = match verbose_flag_count {
        0 => (Level::Info, Level::Info, log::LevelFilter::Info),
        1 => (Level::Debug, Level::Info, log::LevelFilter::Info),
        2 => (Level::Trace, Level::Info, log::LevelFilter::Info),
        3 => (Level::Trace, Level::Debug, log::LevelFilter::Debug),
        _ => (Level::Trace, Level::Trace, log::LevelFilter::Trace),
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
                    for cause in e.chain().skip(1) {
                        info!(slog_internal_logger, "error caused by: {}", cause);
                    }
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
            0,
            drain,
        );
    }
}
