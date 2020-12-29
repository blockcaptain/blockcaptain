mod slogext;
use anyhow::Result;
use slog::{b, debug, error, info, o, record_static, trace, Drain, Level, Logger, Record};
use slogext::{CustomFullFormat, DedupDrain, SlogLogLogger};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::runtime::Runtime;

pub fn blkcaptapp_run<M, F>(main: M, verbose_flag_count: usize, interactive: bool)
where
    M: FnOnce(Logger) -> F,
    F: Future<Output = Result<()>>,
{
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
            let show_timestamp = !interactive;
            let drain = CustomFullFormat::new(decorator, show_timestamp).fuse();
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

            let process_msg_level = match interactive {
                true => Level::Debug,
                false => Level::Info,
            };

            debug!(slog_internal_logger, "debug messages enabled");
            trace!(slog_internal_logger, "trace messages enabled");
            slog_internal_logger.log(&Record::new(
                &record_static!(process_msg_level, ""),
                &format_args!("process starting"),
                b!("blkcapt_version" => env!("CARGO_PKG_VERSION")),
            ));

            {
                let runtime = Runtime::new().expect("can create runtime");
                let result = runtime.block_on(main(slog_internal_logger.clone()));
                if let Err(e) = result {
                    error!(slog_internal_logger, "{}", e);
                    for cause in e.chain().skip(1) {
                        info!(slog_internal_logger, "error caused by: {}", cause);
                    }
                }
                runtime.shutdown_timeout(Duration::from_secs(0));
            }

            slog_internal_logger.log(&Record::new(
                &record_static!(process_msg_level, ""),
                &format_args!("process exiting"),
                b!(),
            ));

            slog_scope::set_global_logger(Logger::root(slog::Discard, o!())).cancel_reset();
        }

        slog_drain_ctrl.set(Logger::root(slog::Discard, o!()));
    }

    println!();
}

#[macro_export]
macro_rules! slog_println( ($($args:tt)+) => {
    slog_scope::info!(#"bc_raw", $($args)+)
};);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runs_app() {
        blkcaptapp_run(
            |log| async move {
                info!(log, "runs_app test");
                Ok(())
            },
            0,
            false,
        );
    }
}
