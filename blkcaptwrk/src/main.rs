use anyhow::Result;
use blkcaptwrk::actors::captain::CaptainActor;
use human_panic::setup_panic;
use log::*;
use xactor::Actor;

#[tokio::main]
async fn main() {
    setup_panic!();

    let vcount = std::env::args().fold(0, |a, e| {
        a + if e.starts_with('-') && e.chars().skip(1).all(|c| c == 'v') {
            e.len() - 1
        } else {
            0
        }
    });

    let (blkcapt_level, all_level) = match vcount {
        0 => (LevelFilter::Info, LevelFilter::Info),
        1 => (LevelFilter::Debug, LevelFilter::Info),
        2 => (LevelFilter::Trace, LevelFilter::Info),
        3 => (LevelFilter::Trace, LevelFilter::Debug),
        _ => (LevelFilter::Trace, LevelFilter::Trace),
    };
    pretty_env_logger::formatted_builder()
        .filter_level(all_level)
        .filter_module("blkcaptwrk", blkcapt_level)
        .filter_module("libblkcapt", blkcapt_level)
        .init();

    debug!("Debug verbosity enabled.");
    trace!("Trace verbosity enabled.");

    let result = run().await;
    if let Err(e) = result {
        error!("{}", e);
        for cause in e.chain().skip(1) {
            info!("Caused by: {}", cause);
        }
    }
}

async fn run() -> Result<()> {
    let mut captain = CaptainActor::start_default().await?;
    tokio::signal::ctrl_c().await?;
    info!("Got signal. Captain standing down...");
    captain.stop(None)?;
    captain.wait_for_stop().await;
    Ok(())
}
