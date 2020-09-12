use anyhow::Result;

use clap::{crate_version, Clap};
use human_panic::setup_panic;
use log::*;
//use blkcapt::contextualize::Validation;

use pretty_env_logger;

mod commands;
mod ui;
use commands::*;

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

    let result = command_dispath(options);
    if let Err(e) = result {
        error!("{}", e);
        for cause in e.chain().skip(1) {
            debug!("Caused by: {}", cause);
        }
    }
}

fn command_dispath(options: CliOptions) -> Result<()> {
    match options.subcmd {
        TopCommands::Pool(top_options) => match top_options.subcmd {
            PoolSubCommands::Attach(options) => attach_pool(options)?,
            PoolSubCommands::Create(options) => create_pool(options)?,
        },
        TopCommands::Dataset(top_options) => match top_options.subcmd {
            DatasetSubCommands::Attach(options) => attach_dataset(options)?,
            DatasetSubCommands::List(options) => list_dataset(options)?,
            DatasetSubCommands::Update(options) => update_dataset(options)?,
        },
        TopCommands::Container(top_options) => match top_options.subcmd {
            ContainerSubCommands::Attach(options) => attach_container(options)?,
        },
        TopCommands::Observer(top_options) => match top_options.subcmd {
            ObserverSubCommands::Create(options) => create_observer(options)?,
        },
    }

    Ok(())
}

#[derive(Clap)]
#[clap(version = crate_version!(), author = "rebeagle")]
struct CliOptions {
    /// Enable debug logs. Use twice to enable trace logs.
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    #[clap(subcommand)]
    subcmd: TopCommands,
}

#[derive(Clap)]
enum TopCommands {
    Pool(PoolCommands),
    Dataset(DatasetCommands),
    Container(ContainerCommands),
    Observer(ObserverCommands),
}

#[derive(Clap)]
struct PoolCommands {
    #[clap(subcommand)]
    subcmd: PoolSubCommands,
}

#[derive(Clap)]
enum PoolSubCommands {
    Create(PoolCreateOptions),
    Attach(PoolAttachOptions),
}

#[derive(Clap)]
struct DatasetCommands {
    #[clap(subcommand)]
    subcmd: DatasetSubCommands,
}

#[derive(Clap)]
#[clap()]
enum DatasetSubCommands {
    Attach(DatasetAttachOptions),
    List(DatasetListOptions),
    Update(DatasetUpdateOptions),
}

#[derive(Clap)]
struct ContainerCommands {
    #[clap(subcommand)]
    subcmd: ContainerSubCommands,
}

#[derive(Clap)]
enum ContainerSubCommands {
    Attach(ContainerAttachOptions),
}

#[derive(Clap)]
struct ObserverCommands {
    #[clap(subcommand)]
    subcmd: ObserverSubCommands,
}

#[derive(Clap)]
enum ObserverSubCommands {
    Create(ObserverCreateOptions),
}
