use anyhow::Result;
use blkcaptapp::blkcaptapp_run;
use clap::{crate_version, Clap};
mod commands;
mod ui;
use commands::*;

fn main() {
    match CliOptions::try_parse() {
        Ok(options) => {
            let vcount = options.verbose as usize;
            blkcaptapp_run(|_| command_dispath(options), vcount, true);
        }
        Err(e) => {
            let message = e.to_string();
            println!("{}", message.replace("error:", "ERRO:"));
            println!();
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
