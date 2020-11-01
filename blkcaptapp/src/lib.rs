mod slogext;
use anyhow::Result;
use human_panic::setup_panic;
use slog::{debug, error, info, o, trace, Drain, Level, Logger};
use slogext::{CustomFullFormat, DedupDrain, SlogLogLogger};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::runtime::Runtime;

pub fn blkcaptapp_run<M, F>(main: M, verbose_flag_count: usize)
where
    M: FnOnce(Logger) -> F,
    F: Future<Output = Result<()>>,
{
    setup_panic!();

    let (internal_level, external_level) = match verbose_flag_count {
        0 => (Level::Info, log::LevelFilter::Info),
        1 => (Level::Debug, log::LevelFilter::Info),
        2 => (Level::Trace, log::LevelFilter::Info),
        3 => (Level::Trace, log::LevelFilter::Debug),
        _ => (Level::Trace, log::LevelFilter::Trace),
    };

    println!();

    {
        let (slog_drain, slog_drain_ctrl) = {
            let decorator = slog_term::TermDecorator::new().build();
            let drain = CustomFullFormat::new(decorator).fuse();
            let drain = slog_async::Async::new(drain).build().fuse();
            let drain = slog_atomic::AtomicSwitch::new(drain);
            let ctrl = drain.ctrl();
            (drain.map(Arc::new), ctrl)
        };

        {
            let slog_internal_logger = {
                let drain = DedupDrain::new(Arc::clone(&slog_drain));
                let drain = drain.filter_level(internal_level).fuse();
                Logger::root(drain, o!())
            };

            let slog_external_logger = {
                let drain = Arc::clone(&slog_drain);
                Logger::root(drain, o!())
            };

            slog_scope::set_global_logger(slog_internal_logger.clone()).cancel_reset();
            SlogLogLogger::install(slog_external_logger, external_level);

            debug!(slog_internal_logger, "debug messages enabled");
            trace!(slog_internal_logger, "trace messages enabled");
            info!(slog_internal_logger, "process starting"; "blkcapt_version" => "0.1.0_a1");

            {
                let mut runtime = Runtime::new().expect("can create runtime");
                let result = runtime.block_on(main(slog_internal_logger.clone()));
                if let Err(e) = result {
                    error!(slog_internal_logger, "{}", e);
                    for cause in e.chain().skip(1) {
                        info!(slog_internal_logger, "error caused by: {}", cause);
                    }
                }
                runtime.shutdown_timeout(Duration::from_secs(0));
            }

            info!(slog_internal_logger, "process exiting");

            slog_scope::set_global_logger(Logger::root(slog::Discard, o!())).cancel_reset();
        }

        slog_drain_ctrl.set(Logger::root(slog::Discard, o!()));
    }

    println!();
}
