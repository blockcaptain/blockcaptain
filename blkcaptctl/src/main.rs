use anyhow::Result;
use clap::{crate_version, Clap};
use human_panic::setup_panic;
use log::*;
mod commands;
mod ui;
use commands::*;

#[tokio::main]
async fn main() {
    setup_panic!();

    let options: CliOptions = CliOptions::parse();
    let (blkcapt_level, all_level) = match options.verbose {
        0 => (LevelFilter::Info, LevelFilter::Info),
        1 => (LevelFilter::Debug, LevelFilter::Info),
        2 => (LevelFilter::Trace, LevelFilter::Info),
        3 => (LevelFilter::Trace, LevelFilter::Debug),
        _ => (LevelFilter::Trace, LevelFilter::Trace),
    };
    pretty_env_logger::formatted_builder()
        .filter_level(all_level)
        .filter_module("blkcaptctl", blkcapt_level)
        .filter_module("libblkcapt", blkcapt_level)
        .init();

    debug!("Debug verbosity enabled.");
    trace!("Trace verbosity enabled.");

    let result = command_dispath(options);
    if let Err(e) = result.await {
        error!("{}", e);
        for cause in e.chain().skip(1) {
            debug!("Caused by: {}", cause);
        }
    }
}

async fn command_dispath(options: CliOptions) -> Result<()> {
    match options.subcmd {
        TopCommands::Pool(top_options) => match top_options.subcmd {
            PoolSubCommands::Attach(options) => attach_pool(options)?,
            PoolSubCommands::Create(options) => create_pool(options)?,
            PoolSubCommands::List(options) => list_pool(options)?,
        },
        TopCommands::Dataset(top_options) => match top_options.subcmd {
            DatasetSubCommands::Attach(options) => attach_dataset(options)?,
            DatasetSubCommands::List(options) => list_dataset(options)?,
            DatasetSubCommands::Update(options) => update_dataset(options)?,
            DatasetSubCommands::Show(options) => show_dataset(options)?,
        },
        TopCommands::Container(top_options) => match top_options.subcmd {
            ContainerSubCommands::Attach(options) => attach_container(options)?,
            ContainerSubCommands::List(options) => list_container(options)?,
        },
        TopCommands::Observer(top_options) => match top_options.subcmd {
            ObserverSubCommands::Create(options) => create_observer(options)?,
            ObserverSubCommands::Test(options) => test_observer(options).await?,
            ObserverSubCommands::List(options) => list_observer(options)?,
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
    List(PoolListOptions),
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
    Show(DatasetShowOptions),
}

#[derive(Clap)]
struct ContainerCommands {
    #[clap(subcommand)]
    subcmd: ContainerSubCommands,
}

#[derive(Clap)]
enum ContainerSubCommands {
    Attach(ContainerAttachOptions),
    List(ContainerListOptions),
}

#[derive(Clap)]
struct ObserverCommands {
    #[clap(subcommand)]
    subcmd: ObserverSubCommands,
}

#[derive(Clap)]
enum ObserverSubCommands {
    Create(ObserverCreateOptions),
    Test(ObserverTestOptions),
    List(ObserverListOptions),
}
