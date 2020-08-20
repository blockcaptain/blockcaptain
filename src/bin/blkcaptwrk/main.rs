use anyhow::{Context, Result};
use clap::{crate_version, Clap};
use human_panic::setup_panic;
use log::*;
use pretty_env_logger;
mod sync;
use sync::sync;

fn main() {
    setup_panic!();

    info!("Worker Start");
    sync();
    info!("Worker Stop");
}