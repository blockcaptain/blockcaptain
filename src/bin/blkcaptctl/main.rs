use anyhow::{Context, Result};
use clap::{crate_version, Clap};
use human_panic::setup_panic;
use log::*;
use pnsystem::btrfs;
use pnsystem::contextualize::Validation;
use pnsystem::filesystem::{self, BlockDeviceIds, BtrfsMountEntry};
use pnsystem::model::btrfs::{BtrfsDataset, BtrfsContainer, BtrfsPool};
use pnsystem::state;
use pretty_env_logger;
use std::convert::TryFrom;
use std::path::PathBuf;
use uuid::Uuid;

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
            PoolSubCommands::Attach(options) => {
                attach_pool(options)?;
            }
        },
        TopCommands::Dataset(top_options) => match top_options.subcmd {
            DatasetSubCommands::Attach(options) => {
                attach_dataset(options)?;
            }
        },
        TopCommands::Container(top_options) => match top_options.subcmd {
            ContainerSubCommands::Attach(options) => {
                attach_container(options)?;
            }
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
}

#[derive(Clap)]
struct PoolCommands {
    #[clap(subcommand)]
    subcmd: PoolSubCommands,
}

#[derive(Clap)]
enum PoolSubCommands {
    Attach(PoolAttachOptions),
}

#[derive(Clap)]
struct DatasetCommands {
    #[clap(subcommand)]
    subcmd: DatasetSubCommands,
}

#[derive(Clap)]
enum DatasetSubCommands {
    Attach(DatasetAttachOptions),
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

// #[derive(Clap, Debug)]
// struct PoolAttachOptions {
//     /// Name of the filesystem.
//     #[clap(default_value=DEFAULT_OS_NAME)]
//     name: String,
//     /// New mountpoint for the filesystem.
//     #[clap(default_value = "/mnt/os_fs")]
//     fs_mountpoint: PathBuf,
// }

// const DEFAULT_OS_NAME: &str = "OS";

// fn initialize_operatingsystem(mut options: PoolAttachOptions) {
//     if options.name != DEFAULT_OS_NAME {
//         let re = Regex::new(r"[^0-9a-z]+").unwrap();
//         options.fs_mountpoint = format!("/mnt/{}_fs", re.replace_all(options.name.to_lowercase().as_str(), "_"))
//             .parse()
//             .unwrap();
//     }
//     let options = options;
//     debug!("initialize_operatingsystem: {:?}", options);

//     let mut validation = Validation::new("new devices");
//     let root_mountentry = filesystem::lookup_mountentry(Path::new("/"))
//         .expect("Root mountpoint is parsable.")
//         .expect("A root mountpoint must exists.");
//     let root_mountentry = BtrfsMountEntry::try_from(root_mountentry);
//     validation.require("root mountpoint must be a btrfs subvolume", root_mountentry.is_ok());
//     validation.require(
//         "root mountpoint must not be the top-level subvolume",
//         root_mountentry.map_or(true, |m| !m.is_toplevel_subvolume()),
//     );
//     validation.validate();
// }

// Pool Attach

const DEFAULT_POOL_NAME: &str = "default";

#[derive(Clap, Debug)]
struct PoolAttachOptions {
    /// Existing mountpoint for the filesystem.
    mountpoint: PathBuf,

    /// Name of the pool.
    #[clap(default_value=DEFAULT_POOL_NAME)]
    name: String,
}

fn attach_pool(options: PoolAttachOptions) -> Result<()> {
    debug!("Command 'attach_pool': {:?}", options);
    let mut entities = state::load_entity_state();

    let new_pool = BtrfsPool::new(options.name, options.mountpoint)?;

    entities.attach_pool(new_pool)?;

    state::store_entity_state(entities);
    Ok(())
}

#[derive(Clap, Debug)]
struct DatasetAttachOptions {
    /// Existing path to subvolume to attach to.
    path: PathBuf,

    /// Name of the dataset. [default: path basename]
    name: Option<String>,
}

fn attach_dataset(options: DatasetAttachOptions) -> Result<()> {
    debug!("Command 'attach_dataset': {:?}", options);

    let mut entities = state::load_entity_state();

    let mountentry = filesystem::find_mountentry(&options.path)
        .context(format!("Failed to detect mountpoint for {:?}.", options.path))?;

    let path = &options.path;
    let name = options.name.unwrap_or_else(|| {
        path.file_name()
            .expect("Path should end with a directory name.")
            .to_string_lossy()
            .to_string()
    });
    let dataset = BtrfsDataset::new(name, options.path)?;

    let pool = entities
        .pool_by_mountpoint_mut(mountentry.file.as_path())
        .context(format!("No pool found for mountpoint {:?}.", mountentry.file))?;

    pool.attach_dataset(dataset)?;
    state::store_entity_state(entities);

    Ok(())
}

#[derive(Clap, Debug)]
struct ContainerAttachOptions {
    /// Existing path to subvolume to attach to.
    path: PathBuf,

    /// Name of the container. [default: path basename]
    name: Option<String>,
}

fn attach_container(options: ContainerAttachOptions) -> Result<()> {
    debug!("Command 'attach_container': {:?}", options);

    let mut entities = state::load_entity_state();

    let mountentry = filesystem::find_mountentry(&options.path)
        .context(format!("Failed to detect mountpoint for {:?}.", options.path))?;

    let path = &options.path;
    let name = options.name.unwrap_or_else(|| {
        path.file_name()
            .expect("Path should end with a directory name.")
            .to_string_lossy()
            .to_string()
    });
    let dataset = BtrfsContainer::new(name, options.path)?;

    let pool = entities
        .pool_by_mountpoint_mut(mountentry.file.as_path())
        .context(format!("No pool found for mountpoint {:?}.", mountentry.file))?;

    pool.attach_container(dataset)?;
    state::store_entity_state(entities);

    Ok(())
}