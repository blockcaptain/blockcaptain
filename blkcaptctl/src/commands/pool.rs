use anyhow::{bail, Context, Result};
use clap::Clap;
use comfy_table::Cell;
use dialoguer::Confirm;
use libblkcapt::model::entities::{IntervalSpec, KeepSpec};
use libblkcapt::{
    core::{BtrfsContainer, BtrfsDataset, BtrfsPool},
    model::{entity_by_id_mut, entity_by_name_mut, entity_by_name_or_id, storage, Entity},
};
use libblkcapt::{
    model::entities::ScheduleModel,
    sys::{
        btrfs::{add_to_fstab, AllocationMode, Filesystem},
        fs::{find_mountentry, BlockDeviceIds, BlockDeviceInfo, DevicePathBuf},
    },
};
use slog_scope::*;
use std::{convert::TryInto, num::NonZeroU32, path::PathBuf, str::FromStr, sync::Arc};

use super::{dataset_search, pool_search};
use crate::ui::{
    comfy_feature_state_cell, comfy_id_header, comfy_id_value, comfy_id_value_full, comfy_name_value, comfy_value_or,
    print_comfy_info, print_comfy_table,
};

#[derive(Clap, Debug)]
pub struct PoolListOptions {}

pub fn list_pool(options: PoolListOptions) -> Result<()> {
    debug!("Command 'list_pool': {:?}", options);

    let entities = storage::load_entity_state();

    print_comfy_table(
        vec![
            comfy_id_header(),
            Cell::new("Pool Name"),
            Cell::new("Filesystem UUID"),
            Cell::new("Disks"),
            Cell::new("Datasets"),
            Cell::new("Containers"),
        ],
        entities.btrfs_pools.iter().map(|p| {
            vec![
                comfy_id_value(p.id()),
                comfy_name_value(p.name()),
                Cell::new(p.uuid),
                Cell::new(p.uuid_subs.len()),
                Cell::new(p.datasets.len()),
                Cell::new(p.containers.len()),
            ]
        }),
    );

    Ok(())
}

const DEFAULT_POOL_NAME: &str = "default";

#[derive(Clap, Debug)]
pub struct PoolCreateOptions {
    /// Name of the pool.
    #[clap(short, long, default_value=DEFAULT_POOL_NAME)]
    name: String,

    #[clap(long)]
    metadata: Option<AllocationMode>,

    #[clap(long)]
    data: Option<AllocationMode>,

    /// New mountpoint for the filesystem.
    #[clap(short, long)]
    mountpoint: Option<PathBuf>,

    /// Devices to format for the filesystem.
    #[clap(required(true))]
    devices: Vec<DevicePathBuf>,
}

pub fn create_pool(options: PoolCreateOptions) -> Result<()> {
    debug!("Command 'create_pool': {:?}", options);
    let mut entities = storage::load_entity_state();

    if options.devices.is_empty() {
        bail!("at least one device is required")
    }

    let infos = options
        .devices
        .iter()
        .map(|d| {
            let (name, uuid, label) = BlockDeviceIds::lookup(d)?
                .map(|ids| (ids.name, ids.uuid, ids.label))
                .unwrap_or_else(|| (d.to_string(), None, None));
            let info = BlockDeviceInfo::lookup(d)?;
            Ok((name, uuid, label, info))
        })
        .collect::<Result<Vec<_>>>()?;

    print_comfy_table(
        vec![
            Cell::new("Name"),
            Cell::new("Current UUID"),
            Cell::new("Current Label"),
            Cell::new("Model"),
            Cell::new("Serial"),
        ],
        infos.into_iter().map(|i| {
            vec![
                comfy_name_value(i.0),
                comfy_value_or(i.1, "None"),
                comfy_value_or(i.2, "None"),
                comfy_value_or(i.3.model, "Unknown"),
                comfy_value_or(i.3.serial_short, "Unknown"),
            ]
        }),
    );

    println!();
    if !Confirm::new()
        .with_prompt("Are you sure you want to destory all data on the devices above?")
        .interact()?
    {
        println!();
        bail!("user aborted");
    }
    println!();

    let filesystem = Filesystem::make(&options.devices, &options.name, options.data, options.metadata)?;
    let mountpoint = options.mountpoint.clone().unwrap_or_else(|| {
        let mut path = PathBuf::from("/mnt");
        path.push(&options.name);
        path
    });
    std::fs::create_dir_all(&mountpoint)?;
    let filesystem = filesystem.mount(&mountpoint)?;
    add_to_fstab(&filesystem)?;

    let new_pool = BtrfsPool::new(options.name, mountpoint)?;
    entities.attach_pool(new_pool.take_model())?;

    storage::store_entity_state(entities);
    Ok(())
}

#[derive(Clap, Debug)]
pub struct PoolAttachOptions {
    /// Existing mountpoint for the filesystem.
    mountpoint: PathBuf,

    /// Name of the pool.
    #[clap(default_value=DEFAULT_POOL_NAME)]
    name: String,
}

pub fn attach_pool(options: PoolAttachOptions) -> Result<()> {
    debug!("Command 'attach_pool': {:?}", options);
    let mut entities = storage::load_entity_state();

    let new_pool = BtrfsPool::new(options.name, options.mountpoint)?;

    entities.attach_pool(new_pool.take_model())?;

    storage::store_entity_state(entities);
    Ok(())
}

#[derive(Clap, Debug)]
pub struct DatasetAttachOptions {
    /// Existing path to subvolume to attach to.
    path: PathBuf,

    /// Name of the dataset. [default: path basename]
    name: Option<String>,
}

pub fn attach_dataset(options: DatasetAttachOptions) -> Result<()> {
    debug!("Command 'attach_dataset': {:?}", options);

    let mut entities = storage::load_entity_state();

    let mountentry =
        find_mountentry(&options.path).context(format!("Failed to detect mountpoint for {:?}.", options.path))?;
    let pool_model = entities
        .pool_by_mountpoint_mut(mountentry.file.as_path())
        .context(format!("No pool found for mountpoint {:?}.", mountentry.file))?;

    let path = &options.path;
    let name = options.name.unwrap_or_else(|| {
        path.file_name()
            .expect("Path should end with a directory name.")
            .to_string_lossy()
            .to_string()
    });

    let pool = Arc::new(BtrfsPool::validate(pool_model.clone())?);
    let dataset = BtrfsDataset::new(&pool, name, options.path)?;

    pool_model.attach_dataset(dataset.take_model())?;
    storage::store_entity_state(entities);

    Ok(())
}

#[derive(Clap, Debug)]
pub struct DatasetCreateOptions {
    /// The pool [pool|id]
    pool: String,

    /// Name of the dataset
    name: String,
}

pub fn create_dataset(options: DatasetCreateOptions) -> Result<()> {
    debug!("Command 'create_dataset': {:?}", options);

    let mut entities = storage::load_entity_state();
    let pool_id = pool_search(&entities, &options.pool)?.id();
    let pool_model = entity_by_id_mut(&mut entities.btrfs_pools, pool_id).expect("always exists if path found");

    let pool = Arc::new(BtrfsPool::validate(pool_model.clone())?);
    let dataset = pool.create_dataset(options.name)?;

    pool_model.attach_dataset(dataset.take_model())?;
    storage::store_entity_state(entities);

    Ok(())
}

#[derive(Clap, Debug)]
pub struct DatasetShowOptions {
    /// The dataset to show
    #[clap(value_name("[pool/]dataset|id"))]
    dataset: String,
}

pub fn show_dataset(options: DatasetShowOptions) -> Result<()> {
    debug!("Command 'show_dataset': {:?}", options);

    let entities = storage::load_entity_state();
    let dataset = dataset_search(&entities, &options.dataset)?;

    print_comfy_info(vec![
        (comfy_id_header(), comfy_id_value_full(dataset.id()).into()),
        (Cell::new("Pool Name"), comfy_name_value(dataset.name()).into()),
        (Cell::new("Dataset Name"), comfy_name_value(dataset.name()).into()),
        (
            Cell::new("Snaps"),
            vec![Cell::new("Test1"), Cell::new("Test2"), Cell::new("Test5")].into(),
        ),
    ]);

    Ok(())
}

#[derive(Clap, Debug)]
pub struct DatasetListOptions {}

pub fn list_dataset(options: DatasetListOptions) -> Result<()> {
    debug!("Command 'list_dataset': {:?}", options);

    let entities = storage::load_entity_state();

    print_comfy_table(
        vec![
            comfy_id_header(),
            Cell::new("Pool Name"),
            Cell::new("Dataset Name"),
            Cell::new("Snapshotting"),
            Cell::new("Pruning"),
        ],
        entities.datasets().map(|ds| {
            vec![
                comfy_id_value(ds.entity.id()),
                comfy_name_value(ds.parent.name()),
                comfy_name_value(ds.entity.name()),
                comfy_feature_state_cell(ds.entity.snapshotting_state()),
                comfy_feature_state_cell(ds.entity.pruning_state()),
            ]
        }),
    );

    Ok(())
}

const AFTER_HELP: &str = r"RETENTION

The retention interval format is [<Repeat>x]<Duration>[:<Count>]. The default Repeat and Count values are 1.
";

/// Update an existing dataset
#[derive(Clap, Debug)]
#[clap(after_help(AFTER_HELP))]
pub struct DatasetUpdateOptions {
    /// Set the snapshots schedule using a simple frequency (e.g. 1hour, 2days)
    #[clap(short('f'), long, value_name("duration"), conflicts_with("snapshot-schedule"))]
    snapshot_frequency: Option<humantime::Duration>,

    /// Set the schedule for taking snapshots of this dataset
    #[clap(short('s'), long, value_name("cron"))]
    snapshot_schedule: Option<ScheduleModel>,

    /// Prevent starting new snapshot creation jobs on this dataset
    #[clap(long, conflicts_with("resume-snapshotting"))]
    pause_snapshotting: bool,

    #[clap(long)]
    resume_snapshotting: bool,

    /// Specify one or more snapshot retention time intervals
    #[clap(short('i'), long, value_name("interval"))]
    retention_intervals: Option<Vec<IntervalSpecArg>>,

    /// Specify the minimum number of snapshots to retains
    #[clap(short('m'), long, value_name("count"))]
    retain_minimum: Option<NonZeroU32>,

    /// Prevent starting new snapshot pruning jobs on this dataset
    #[clap(long, conflicts_with("resume-pruning"))]
    pause_pruning: bool,

    #[clap(long)]
    resume_pruning: bool,

    /// The dataset to update
    #[clap(value_name("[pool/]dataset|id"))]
    dataset: String,
}

#[derive(Debug)]
pub struct IntervalSpecArg(IntervalSpec);

impl FromStr for IntervalSpecArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let outter = s.split(':').collect::<Vec<_>>();
        let inner = outter[0].split('x').collect::<Vec<_>>();
        if inner.len() > 2 || outter.len() > 2 {
            bail!("Interval format is [<Repeat>x]<Duration>[:<Count>].");
        };
        let (repeat, duration) = match inner.len() {
            2 => (NonZeroU32::from_str(inner[0])?, inner[1]),
            1 => (NonZeroU32::new(1).expect("constant always nonzero"), inner[0]),
            _ => unreachable!(),
        };
        Ok(Self(IntervalSpec {
            repeat,
            duration: *humantime::Duration::from_str(duration)?,
            keep: match outter.len() {
                2 => match outter[1] {
                    "all" => KeepSpec::All,
                    s => KeepSpec::Newest(NonZeroU32::from_str(s)?),
                },
                1 => KeepSpec::Newest(NonZeroU32::new(1).expect("constant always nonzero")),
                _ => unreachable!(),
            },
        }))
    }
}

pub fn update_dataset(options: DatasetUpdateOptions) -> Result<()> {
    debug!("Command 'update_dataset': {:?}", options);

    let mut entities = storage::load_entity_state();

    let parts = options.dataset.splitn(2, '/').collect::<Vec<_>>();
    let dataset = if parts.len() == 2 {
        let filesystem = entity_by_name_mut(&mut entities.btrfs_pools, parts[0]).context("Filesystem not found.")?;
        entity_by_name_mut(&mut filesystem.datasets, parts[1]).context("Dataset not found in filesystem.")?
    } else {
        let dataset_path = entity_by_name_or_id(entities.datasets(), parts[0])
            .map(|e| e.into_id_path())
            .context("Dataset not found.")?;
        let filesystem =
            entity_by_id_mut(&mut entities.btrfs_pools, dataset_path.parent).expect("always exists if path found");
        entity_by_id_mut(&mut filesystem.datasets, dataset_path.entity).expect("always exists if path found")
    };

    if let Some(f) = options.snapshot_frequency {
        let std_duration = *f;
        dataset.snapshot_schedule = Some(std_duration.try_into().context(
            "The specified frequency can't be converted into a schedule. \
            For more advanced schedule creation use the -s option. \
            See --help for more details.",
        )?);
    }

    if options.pause_snapshotting || options.resume_snapshotting {
        dataset.pause_snapshotting = options.pause_snapshotting
    }

    if options.pause_pruning || options.resume_pruning {
        dataset.pause_pruning = options.pause_pruning
    }

    if options.retain_minimum.is_some() || options.retention_intervals.is_some() {
        let retention = dataset.snapshot_retention.get_or_insert_with(Default::default);
        if let Some(intervals) = options.retention_intervals {
            retention.interval = intervals.into_iter().map(|i| i.0).collect();
        }

        if let Some(minimum) = options.retain_minimum {
            retention.newest_count = minimum;
        }
    }

    storage::store_entity_state(entities);

    Ok(())
}

#[derive(Clap, Debug)]
pub struct ContainerAttachOptions {
    /// Existing path to subvolume to attach to.
    path: PathBuf,

    /// Name of the container. [default: path basename]
    name: Option<String>,
}

pub fn attach_container(options: ContainerAttachOptions) -> Result<()> {
    debug!("Command 'attach_container': {:?}", options);

    let mut entities = storage::load_entity_state();

    let mountentry =
        find_mountentry(&options.path).context(format!("Failed to detect mountpoint for {:?}.", options.path))?;
    let pool_model = entities
        .pool_by_mountpoint_mut(mountentry.file.as_path())
        .context(format!("No pool found for mountpoint {:?}.", mountentry.file))?;

    let path = &options.path;
    let name = options.name.unwrap_or_else(|| {
        path.file_name()
            .expect("Path should end with a directory name.")
            .to_string_lossy()
            .to_string()
    });

    let pool = Arc::new(BtrfsPool::validate(pool_model.clone())?);
    let container = BtrfsContainer::new(&pool, name, options.path)?;

    let pool = entities
        .pool_by_mountpoint_mut(mountentry.file.as_path())
        .context(format!("No pool found for mountpoint {:?}.", mountentry.file))?;

    pool.attach_container(container.take_model())?;
    storage::store_entity_state(entities);

    Ok(())
}

#[derive(Clap, Debug)]
pub struct ContainerCreateOptions {
    /// The pool [pool|id]
    pool: String,

    /// Name of the container
    name: String,
}

pub fn create_container(options: ContainerCreateOptions) -> Result<()> {
    debug!("Command 'create_container': {:?}", options);

    let mut entities = storage::load_entity_state();
    let pool_id = pool_search(&entities, &options.pool)?.id();
    let pool_model = entity_by_id_mut(&mut entities.btrfs_pools, pool_id).expect("always exists if path found");

    let pool = Arc::new(BtrfsPool::validate(pool_model.clone())?);
    let container = pool.create_container(options.name)?;

    pool_model.attach_container(container.take_model())?;
    storage::store_entity_state(entities);

    Ok(())
}

#[derive(Clap, Debug)]
pub struct ContainerListOptions {}

pub fn list_container(options: ContainerListOptions) -> Result<()> {
    debug!("Command 'list_container': {:?}", options);

    let entities = storage::load_entity_state();

    print_comfy_table(
        vec![
            comfy_id_header(),
            Cell::new("Pool Name"),
            Cell::new("Container Name"),
            Cell::new("Pruning"),
        ],
        entities.containers().map(|c| {
            vec![
                comfy_id_value(c.entity.id()),
                comfy_name_value(c.parent.name()),
                comfy_name_value(c.entity.name()),
                comfy_feature_state_cell(c.entity.pruning_state()),
            ]
        }),
    );

    Ok(())
}
