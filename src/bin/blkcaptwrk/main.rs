use anyhow::{Context, Result};
use clap::{crate_version, Clap};
use human_panic::setup_panic;
use log::*;
use pretty_env_logger;
mod commands;
use commands::service;

fn main() {
    setup_panic!();

    let options: CliOptions = CliOptions::parse();
    let level = match options.verbose {
        0 => LevelFilter::Info,
        1 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };
    pretty_env_logger::formatted_builder().filter_level(level).init();

    debug!("Debug verbosity enabled.");
    trace!("Trace verbosity enabled.");

    let result = service();
    if let Err(e) = result {
        error!("{}", e);
        for cause in e.chain().skip(1) {
            info!("Caused by: {}", cause);
        }
    }
}

#[derive(Clap)]
#[clap(version = crate_version!(), author = "rebeagle")]
struct CliOptions {
    /// Enable debug logs. Use twice to enable trace logs.
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32
}