use human_panic::setup_panic;
use log::*;
mod commands;
mod worker;
use commands::service;

#[tokio::main]
async fn main() {
    setup_panic!();

    let vcount = std::env::args().fold(0, |a, e| {
        a + match e.as_str() {
            "-v" => 1,
            "-vv" => 2,
            _ => 0,
        }
    });

    let level = match vcount {
        0 => LevelFilter::Info,
        1 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };
    pretty_env_logger::formatted_builder().filter_level(level).init();

    debug!("Debug verbosity enabled.");
    trace!("Trace verbosity enabled.");

    let result = service().await;
    if let Err(e) = result {
        error!("{}", e);
        for cause in e.chain().skip(1) {
            info!("Caused by: {}", cause);
        }
    }
}
