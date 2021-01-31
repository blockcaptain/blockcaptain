use anyhow::{anyhow, Result};
use clap::Clap;
use humantime::Duration;
use libblkcapt::model::entities::{SnapshotSyncEntity, SnapshotSyncMode};
use libblkcapt::model::{storage, Entity};

use crate::ui::ScheduleArg;

use super::{container_search, dataset_search, restic_search};

#[derive(Clap, Debug)]
pub struct SyncCreateUpdateOptions {
    /// Sync mode
    #[clap(short, long, value_name("mode"))]
    mode: Option<SnapshotSyncMode>,

    /// Schedule for all_scheduled or latest_scheduled modes
    #[clap(short, long, value_name("schedule"))]
    schedule: Option<ScheduleArg>,

    /// Interval for interval_immediate mode
    #[clap(short, long, value_name("interval"))]
    interval: Option<Duration>,
}

impl SyncCreateUpdateOptions {
    fn configure_mode(&self, mode: SnapshotSyncMode) -> Result<SnapshotSyncMode> {
        match (mode, &self.schedule, self.interval) {
            (SnapshotSyncMode::AllScheduled(_), Some(schedule), None) => {
                Ok(SnapshotSyncMode::AllScheduled(schedule.clone().into()))
            }
            (SnapshotSyncMode::LatestScheduled(_), Some(schedule), None) => {
                Ok(SnapshotSyncMode::LatestScheduled(schedule.clone().into()))
            }
            (SnapshotSyncMode::IntervalImmediate(_), None, Some(duration)) => {
                Ok(SnapshotSyncMode::IntervalImmediate(duration.into()))
            }
            _ => Err(anyhow!("invalid schedule or interval option for sync mode")),
        }
    }
}

#[derive(Clap, Debug)]
pub struct SyncCreateOptions {
    /// Name of the sync
    #[clap(short, long, default_value = "default")]
    name: String,

    /// The name or id of the source dataset
    #[clap(value_name("dataset|id"))]
    dataset: String,

    /// The name or id of the destination container
    #[clap(value_name("container|id"))]
    container: String,

    #[clap(flatten)]
    shared: SyncCreateUpdateOptions,
}

pub fn create_sync(options: SyncCreateOptions) -> Result<()> {
    let mut entities = storage::load_entity_state();

    let dataset_id = dataset_search(&entities, &options.dataset).map(|d| d.id())?;
    // TODO: entity refactor needed. this doesn't error if a container and restic container have
    // the same name so user may accidentally select wrong target.
    let container_id = container_search(&entities, &options.container)
        .map(|c| c.id())
        .or_else(|_| restic_search(&entities, &options.container).map(|c| c.id()))?;
    let maybe_mode = options
        .shared
        .mode
        .clone()
        .map(|m| options.shared.configure_mode(m))
        .transpose()?;

    let mut sync = SnapshotSyncEntity::new(options.name, dataset_id, container_id);
    if let Some(mode) = maybe_mode {
        sync.sync_mode = mode;
    }

    entities.snapshot_syncs.push(sync);

    storage::store_entity_state(entities);
    Ok(())
}

#[derive(Clap, Debug)]
pub struct SyncUpdateOptions {
    /// The name or id of the sync
    #[clap(value_name("sync|id"))]
    sync: String,

    #[clap(flatten)]
    shared: SyncCreateUpdateOptions,
}

pub fn update_sync(_options: SyncUpdateOptions) -> Result<()> {
    //let mut entities = storage::load_entity_state();

    //storage::store_entity_state(entities);
    Ok(())
}

#[derive(Clap, Debug)]
pub struct SyncListOptions {}

pub fn list_sync(_options: SyncListOptions) -> Result<()> {
    //let mut entities = storage::load_entity_state();

    //storage::store_entity_state(entities);
    Ok(())
}

#[derive(Clap, Debug)]
pub struct SyncShowOptions {
    /// The name or id of the sync
    #[clap(value_name("sync|id"))]
    sync: String,
}

pub fn show_sync(_options: SyncShowOptions) -> Result<()> {
    //let mut entities = storage::load_entity_state();

    //storage::store_entity_state(entities);
    Ok(())
}

#[derive(Clap, Debug)]
pub struct SyncDeleteOptions {
    /// The name or id of the sync
    #[clap(value_name("sync|id"))]
    sync: String,
}

pub fn delete_sync(_options: SyncDeleteOptions) -> Result<()> {
    //let mut entities = storage::load_entity_state();

    //storage::store_entity_state(entities);
    Ok(())
}
