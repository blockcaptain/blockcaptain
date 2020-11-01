use anyhow::Result;
use blkcaptapp::blkcaptapp_run;
use blkcaptwrk::actors::captain::CaptainActor;
use slog::{info, Logger};
use xactor::Actor;

fn main() {
    let vcount = std::env::args().fold(0, |a, e| {
        a + if e.starts_with('-') && e.chars().skip(1).all(|c| c == 'v') {
            e.len() - 1
        } else {
            0
        }
    });

    blkcaptapp_run(async_main, vcount);
}

async fn async_main(log: Logger) -> Result<()> {
    let mut captain = CaptainActor::new(&log).start().await?;
    tokio::signal::ctrl_c().await?;
    info!(log, "process signaled");
    captain.stop(None)?;
    captain.wait_for_stop().await;
    Ok(())
}
