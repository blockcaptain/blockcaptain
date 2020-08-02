use anyhow::{Context, Result};
use clap::{crate_version, Clap};
use log::*;
use pnsystem::contextualize::Validation;
use pnsystem::filesystem::{self, BtrfsMountEntry, BlockDeviceIds};
use pnsystem::managed::BtrfsPool;
use pretty_env_logger;
use regex::Regex;
use std::path::PathBuf;
use std::{convert::TryFrom, path::Path};

fn main() {
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
        if let Some(se) = e.source() {
            debug!("Source of error: {:#?}", se);
        }
    }
}

fn command_dispath(options: CliOptions) -> Result<()> {
    match options.subcmd {
        TopCommands::Pool(initialize_options) => match initialize_options.subcmd {
            PoolSubCommands::Attach(options) => {
                attach_pool(options)?;
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
    Pool(PoolCommand),
}

#[derive(Clap)]
struct PoolCommand {
    #[clap(subcommand)]
    subcmd: PoolSubCommands,
}

#[derive(Clap)]
enum PoolSubCommands {
    Attach(PoolAttachOptions),
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

    let mountentry = filesystem::lookup_mountentry(options.mountpoint.as_path())
        .expect("All mount points are parsable.")
        .context("Mountpoint does not exist.")?;
    let mountentry = BtrfsMountEntry::try_from(mountentry);

    let mut validation = Validation::new("existing mountpoint");
    validation.require("mountpoint must be a btrfs subvolume", mountentry.is_ok());
    validation.require(
        "mountpoint must not be the fstree subvolume",
        mountentry.map_or(true, |m| !m.is_toplevel_subvolume()),
    );
    validation.validate()?;

    // pull info about the filesystem devices and get their IDs.
    // let ids = 

    // let foo = BtrfsPool {
    //     name: options.name,
    //     mountpoint_path: options.mountpoint,
    // };
    
    Ok(())
}
