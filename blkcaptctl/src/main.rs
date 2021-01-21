use std::{
    error::Error,
    fmt::{Debug, Display},
};

use anyhow::{anyhow, Result};
use blkcaptapp::blkcaptapp_run;
use clap::{crate_version, Clap};
mod commands;
mod ui;
use commands::observer::*;
use commands::pool::*;
use commands::service::*;
use commands::sync::*;

fn main() {
    let maybe_options = CliOptions::try_parse();
    let vcount = maybe_options.as_ref().map(|o| o.verbose as usize).unwrap_or_default();
    blkcaptapp_run(|_| async_main(maybe_options), vcount, true);
}

async fn async_main(options: clap::Result<CliOptions>) -> Result<()> {
    match options {
        Ok(options) => command_dispath(options).await,
        Err(e) => {
            if e.use_stderr() {
                Err(anyhow!(ClapErrorWrapper(e)))
            } else {
                println!("{}", e);
                Ok(())
            }
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
            DatasetSubCommands::Create(options) => create_dataset(options)?,
            DatasetSubCommands::List(options) => list_dataset(options)?,
            DatasetSubCommands::Update(options) => update_dataset(options)?,
            DatasetSubCommands::Show(options) => show_dataset(options)?,
        },
        TopCommands::Container(top_options) => match top_options.subcmd {
            ContainerSubCommands::Attach(options) => attach_container(options)?,
            ContainerSubCommands::Create(options) => create_container(options)?,
            ContainerSubCommands::List(options) => list_container(options)?,
        },
        TopCommands::Observer(top_options) => match top_options.subcmd {
            ObserverSubCommands::Create(options) => create_observer(options)?,
            ObserverSubCommands::Update(options) => update_observer(options)?,
            ObserverSubCommands::Delete(options) => delete_observer(options)?,
            ObserverSubCommands::Show(options) => show_observer(options)?,
            ObserverSubCommands::Test(options) => test_observer(options).await?,
            ObserverSubCommands::List(options) => list_observer(options)?,
        },
        TopCommands::Sync(top_options) => match top_options.subcmd {
            SyncSubCommands::Create(options) => create_sync(options)?,
            SyncSubCommands::Update(options) => update_sync(options)?,
            SyncSubCommands::Delete(options) => delete_sync(options)?,
            SyncSubCommands::Show(options) => show_sync(options)?,
            SyncSubCommands::List(options) => list_sync(options)?,
        },
        TopCommands::Service(top_options) => match top_options.subcmd {
            ServiceSubCommands::Status(options) => service_status(options).await?,
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
    Sync(SyncCommands),
    Service(ServiceCommands),
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
    Create(DatasetCreateOptions),
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
    Create(ContainerCreateOptions),
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
    Update(ObserverUpdateOptions),
    Delete(ObserverDeleteOptions),
    Show(ObserverShowOptions),
    Test(ObserverTestOptions),
    List(ObserverListOptions),
}

#[derive(Clap)]
struct SyncCommands {
    #[clap(subcommand)]
    subcmd: SyncSubCommands,
}

#[derive(Clap)]
enum SyncSubCommands {
    Create(SyncCreateOptions),
    Update(SyncUpdateOptions),
    Delete(SyncDeleteOptions),
    Show(SyncShowOptions),
    List(SyncListOptions),
}

#[derive(Clap)]
struct ServiceCommands {
    #[clap(subcommand)]
    subcmd: ServiceSubCommands,
}

#[derive(Clap)]
enum ServiceSubCommands {
    Status(ServiceStatusOptions),
}

struct ClapErrorWrapper(clap::Error);

impl Error for ClapErrorWrapper {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.0.source().and_then(|e| e.source())
    }
}

impl Display for ClapErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.0.to_string();
        write!(
            f,
            "{}",
            inner
                .replace("error: ", "")
                .replace("For more information try --help", "")
                .trim()
        )
    }
}

impl Debug for ClapErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}
