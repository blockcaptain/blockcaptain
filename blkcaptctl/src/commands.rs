use anyhow::{bail, Context, Result};
use clap::Clap;
use futures_util::future::FutureExt;
use libblkcapt::sys::fs::{find_mountentry, DevicePathBuf};
use libblkcapt::{
    core::ObservationEmitter,
    model::{
        entities::HealthchecksObserverEntity,
        entities::{FeatureState, HealthchecksObservation, ObservableEvent, Observation},
        Entities,
    },
};
use libblkcapt::{
    core::ObservationRouter,
    model::entities::{IntervalSpec, KeepSpec},
};
use libblkcapt::{
    core::{BtrfsContainer, BtrfsDataset, BtrfsPool},
    model::{entity_by_id_mut, entity_by_name_mut, entity_by_name_or_id, storage, Entity},
};
use log::*;
use std::{num::NonZeroU32, path::PathBuf, rc::Rc, str::FromStr, time::Duration};
use uuid::Uuid;

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
pub struct PoolCreateOptions {
    /// Name of the pool.
    #[clap(short, long, default_value=DEFAULT_POOL_NAME)]
    name: String,

    /// Devices to format for the filesystem.
    #[clap(required(true))]
    devices: Vec<DevicePathBuf>,
}

pub fn create_pool(options: PoolCreateOptions) -> Result<()> {
    debug!("Command 'create_pool': {:?}", options);
    //let mut entities = storage::load_entity_state();

    todo!();
    // create filesystem (via fs?)
    // mount (via fs)
    // let new_pool = BtrfsPool::new(options.name, options.mountpoint)?;
    // entities.attach_pool(new_pool.take_model())?;

    //storage::store_entity_state(entities);
    //Ok(())
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

    let pool = Rc::new(BtrfsPool::validate(pool_model.clone())?);
    let dataset = BtrfsDataset::new(&pool, name, options.path)?;

    pool_model.attach_dataset(dataset.take_model())?;
    storage::store_entity_state(entities);

    Ok(())
}

#[derive(Clap, Debug)]
pub struct DatasetListOptions {}

pub fn list_dataset(options: DatasetListOptions) -> Result<()> {
    use comfy_table::presets::UTF8_FULL;
    use comfy_table::*;
    debug!("Command 'list_dataset': {:?}", options);

    let entities = storage::load_entity_state();

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("ID").add_attribute(Attribute::Bold),
            Cell::new("Pool Name"),
            Cell::new("Dataset Name"),
            Cell::new("Snapshotting"),
            Cell::new("Pruning"),
        ]);

    for dataset in entities.datasets() {
        table.add_row(vec![
            Cell::new(&dataset.entity.id().to_string()[0..8])
                .fg(Color::Blue)
                .add_attribute(Attribute::Bold),
            Cell::new(dataset.parent.name()).fg(Color::Blue),
            Cell::new(dataset.entity.name()).fg(Color::Blue),
            feature_state_cell(dataset.entity.snapshotting_state()),
            feature_state_cell(dataset.entity.pruning_state()),
        ]);
    }

    println!("{}", table);

    Ok(())
}

pub fn feature_state_cell(state: FeatureState) -> comfy_table::Cell {
    comfy_table::Cell::new(state).fg(match state {
        FeatureState::Enabled => comfy_table::Color::Green,
        FeatureState::Paused => comfy_table::Color::Yellow,
        FeatureState::Unconfigured => comfy_table::Color::Red,
    })
}

const AFTER_HELP: &str = r"RETENTION:

The retention interval format is [<Repeat>x]<Duration>[:<Count>]. The default Repeat and Count values are 1.
";

/// Update an existing dataset
#[derive(Clap, Debug)]
#[clap(after_help(AFTER_HELP))]
pub struct DatasetUpdateOptions {
    /// Set the frequency at which snapshots are taken (e.g. 1hour, 2days)
    #[clap(short("f"), long, value_name("duration"))]
    snapshot_frequency: Option<humantime::Duration>,

    /// Prevent starting new snapshot creation jobs on this dataset
    #[clap(long, conflicts_with("resume-snapshotting"))]
    pause_snapshotting: bool,

    #[clap(long)]
    resume_snapshotting: bool,

    /// Specify one or more snapshot retention time intervals
    #[clap(short("i"), long, value_name("interval"))]
    retention_intervals: Option<Vec<IntervalSpecArg>>,

    /// Specify the minimum number of snapshots to retains
    #[clap(short("m"), long, value_name("count"))]
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
            1 => (NonZeroU32::new(1).unwrap(), inner[0]),
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
                1 => KeepSpec::Newest(NonZeroU32::new(1).unwrap()),
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
        let dataset_path = entity_by_name_or_id(entities.datasets(), parts[0])?
            .map(|e| e.into_id_path())
            .context("Dataset not found.")?;
        let filesystem = entity_by_id_mut(&mut entities.btrfs_pools, dataset_path.parent).unwrap();
        entity_by_id_mut(&mut filesystem.datasets, dataset_path.entity).unwrap()
    };

    if let Some(f) = options.snapshot_frequency {
        dataset.snapshot_frequency = Some(*f);
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

    let pool = Rc::new(BtrfsPool::validate(pool_model.clone())?);
    let container = BtrfsContainer::new(&pool, name, options.path)?;

    let pool = entities
        .pool_by_mountpoint_mut(mountentry.file.as_path())
        .context(format!("No pool found for mountpoint {:?}.", mountentry.file))?;

    pool.attach_container(container.take_model())?;
    storage::store_entity_state(entities);

    Ok(())
}

#[derive(Clap, Debug)]
pub struct ObserverCreateOptions {
    /// Name of the observer.
    #[clap(short, long, default_value = "default")]
    name: String,

    /// Type of observer (must be "healthchecks").
    #[clap(short("t"), long("type"), required(true))]
    observer_type: String,

    /// Observations specifications.
    #[clap()]
    observations: Vec<ObservationArg>,
}

#[derive(Debug)]
pub struct ObservationArg {
    healthcheck_id: Uuid,
    entity_id: Uuid,
    event: ObservableEvent,
}

impl FromStr for ObservationArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let outter = s.split('=').collect::<Vec<_>>();
        let inner = outter[0].split(':').collect::<Vec<_>>();
        if inner.len() != 2 || outter.len() != 2 {
            bail!("Interval format is <EntityID>:<Event>=<HealthcheckID>.");
        };
        Ok(Self {
            entity_id: Uuid::parse_str(inner[0])?,
            healthcheck_id: Uuid::parse_str(outter[1])?,
            event: ObservableEvent::from_str(inner[1])?,
        })
    }
}

pub fn create_observer(options: ObserverCreateOptions) -> Result<()> {
    debug!("Command 'create_observer': {:?}", options);

    let mut entities = storage::load_entity_state();

    if options.observer_type != "healthchecks" {
        bail!("Only healthchecks is supported.");
    }

    let observations = options
        .observations
        .iter()
        .map(|o| HealthchecksObservation {
            healthcheck_id: o.healthcheck_id,
            observation: Observation {
                entity_id: o.entity_id,
                event: o.event,
            },
        })
        .collect::<Vec<_>>();

    for observation in observations.iter() {
        let entity = find_entity(&entities, observation.observation.entity_id).context("Id not found.")?;
        info!("Found {:?}.", entity);
    }

    let observer = HealthchecksObserverEntity::new(options.name, observations);

    entities.observers.push(observer);

    storage::store_entity_state(entities);

    Ok(())
}

fn any_entity<T: Entity>(entity: &T) -> &dyn Entity {
    entity as &dyn Entity
}

fn find_entity(entities: &Entities, id: Uuid) -> Option<&dyn Entity> {
    entities
        .pool(id)
        .map(any_entity)
        .or_else(|| entities.dataset(id).map(|e| any_entity(e.entity)))
        .or_else(|| entities.container(id).map(|e| any_entity(e.entity)))
}

#[derive(Clap, Debug)]
pub struct ObserverTestOptions {
    /// Id of the source entity.
    #[clap()]
    entity: Uuid,

    /// Fail instead of pass.
    #[clap(short, long)]
    fail: bool,

    /// Event to emit.
    #[clap()]
    event: ObservableEvent,
}

pub async fn test_observer(options: ObserverTestOptions) -> Result<()> {
    debug!("Command 'create_observer': {:?}", options);

    let entities = storage::load_entity_state();
    let entity = find_entity(&entities, options.entity).context("Id not found.")?;
    info!("Found {:?}.", entity);

    let router = ObservationRouter::new(entities.observers);
    let matches = router.route(options.entity, options.event);
    if matches.is_empty() {
        bail!("No matching observations found.");
    }

    let emitter = ObservationEmitter::default();
    for observation_match in matches {
        emitter
            .observe_work(vec![observation_match], || {
                tokio::time::delay_for(Duration::from_millis(300)).map(|_| Result::<_>::Ok(()))
            })
            .await?;
    }

    Ok(())
}