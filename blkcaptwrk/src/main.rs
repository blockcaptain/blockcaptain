use anyhow::Result;
use blkcaptwrk::actors::captain::CaptainActor;
use blkcaptwrk::slogext::{CustomFullFormat, DedupDrain};
use human_panic::setup_panic;
use slog::{debug, error, info, o, trace, Drain, Level, Logger};
use std::{sync::Arc, time::Duration};
use tokio::runtime::Runtime;
use xactor::Actor;

fn main() {
    setup_panic!();

    let vcount = std::env::args().fold(0, |a, e| {
        a + if e.starts_with('-') && e.chars().skip(1).all(|c| c == 'v') {
            e.len() - 1
        } else {
            0
        }
    });

    let (internal_level, external_level) = match vcount {
        0 => (Level::Info, log::Level::Info),
        1 => (Level::Debug, log::Level::Info),
        2 => (Level::Trace, log::Level::Info),
        3 => (Level::Trace, log::Level::Debug),
        _ => (Level::Trace, log::Level::Trace),
    };

    println!();

    {
        let slog_drain = {
            let decorator = slog_term::TermDecorator::new().build();
            let drain = CustomFullFormat::new(decorator).fuse();
            slog_async::Async::new(drain).build().fuse().map(Arc::new)
        };

        {
            let (slog_internal_logger, slog_internal_ctrl) = {
                let drain = slog_atomic::AtomicSwitch::new(Arc::clone(&slog_drain));
                let ctrl = drain.ctrl();
                let drain = DedupDrain::new(drain);
                let drain = drain.filter_level(internal_level).fuse();
                (Logger::root(drain, o!()), ctrl)
            };

            let slog_external_logger = {
                // TEMPORARY FILTERING WHILE TRANSITIONING INTERNAL TO SLOG.
                let drain = Arc::clone(&slog_drain)
                    .filter(|r| r.module().contains("blkcapt"))
                    .fuse();
                // let drain = Arc::clone(&slog_drain);
                Logger::root(drain, o!("log" => "external"))
            };

            slog_scope::set_global_logger(slog_external_logger).cancel_reset();
            slog_stdlog::init_with_level(external_level).expect("can always init stdlog");

            debug!(slog_internal_logger, "debug messages enabled");
            trace!(slog_internal_logger, "trace messages enabled");
            info!(slog_internal_logger, "process starting"; "blkcapt_version" => "0.1.0_a1");

            {
                let mut runtime = Runtime::new().expect("can create runtime");
                let result = runtime.block_on(async_main(&slog_internal_logger));
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
            slog_internal_ctrl.set(Logger::root(slog::Discard, o!()));
        }

        Arc::try_unwrap(slog_drain)
            .map_err(|_| "leaked reference")
            .expect("all references to main async drain are dropped");
    }

    println!();
}

async fn async_main(log: &Logger) -> Result<()> {
    let mut captain = CaptainActor::new(log).start().await?;
    tokio::signal::ctrl_c().await?;
    info!(log, "process signaled");
    captain.stop(None)?;
    captain.wait_for_stop().await;
    Ok(())
}
