use anyhow::Result;
use blkcaptapp::{blkcaptapp_run, slogext::CustomFullFormat};
use blkcaptwrk::{
    actors::{captain::CaptainActor, intel::IntelActor},
    slogext::JournalDrain,
};
use libblkcapt::model::{storage::load_server_config, BcLogLevel};
use libsystemd::daemon::{self, NotifyState};
use slog::{error, info, Drain, Logger};
use std::{env, process::exit, time::Duration};
use tokio::signal::unix::{signal, SignalKind};
use xactor::Actor;

fn main() {
    let log_level = {
        let count = std::env::args().fold(0, |a, e| {
            a + if e.starts_with('-') && e.chars().skip(1).all(|c| c == 'v') {
                e.len() - 1
            } else {
                0
            }
        });
        if count > 0 {
            count.into()
        } else {
            let config = load_server_config();
            match config {
                Ok(c) => c.log_level,
                Err(e) => {
                    println!("reading server config failed: {:?}", e);
                    BcLogLevel::Info
                }
            }
        }
    };

    let slog_drain = if use_journal() {
        println!("logging to journald");
        let drain = JournalDrain.fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        slog_atomic::AtomicSwitch::new(drain)
    } else {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = CustomFullFormat::new(decorator, true).fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        slog_atomic::AtomicSwitch::new(drain)
    };

    exit(blkcaptapp_run(async_main, log_level, slog_drain));
}

async fn async_main(log: Logger) -> Result<()> {
    let mut intel = IntelActor::start_default_and_register().await?;
    {
        let mut captain = CaptainActor::new(&log).start().await?;
        let mut sigint_stream = signal(SignalKind::interrupt())?;
        let mut sigterm_stream = signal(SignalKind::terminate())?;
        systemd_notify(&log, &[NotifyState::Ready]);
        let signal = tokio::select! {
            _ = sigint_stream.recv() => "interrupt",
            _ = sigterm_stream.recv() => "terminate"
        };
        info!(log, "process {} signal received", signal);
        systemd_notify(&log, &[NotifyState::Stopping]);
        let _ = captain.stop(None);
        captain.wait_for_stop().await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    intel.stop(None)?;
    intel.wait_for_stop().await;
    Ok(())
}

fn systemd_notify(log: &Logger, state: &[NotifyState]) {
    if let Err(error) = daemon::notify(false, state) {
        error!(log, "failed to notify systemd"; "error" => %error);
    }
}

fn use_journal() -> bool {
    env::var("JOURNAL_STREAM").is_ok()
}
