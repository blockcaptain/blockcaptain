use std::{num::NonZeroU32, str::FromStr};

use anyhow::{bail, Context, Result};
use clap::Clap;
use libblkcapt::model::{
    entities::BtrfsDatasetEntity,
    entities::BtrfsPoolEntity,
    entities::{
        BtrfsContainerEntity, IntervalSpec, KeepSpec, ResticContainerEntity, RetentionRuleset, SnapshotSyncEntity,
    },
    entity_by_name, EntityId, EntityPath, EntityPath1, EntityPath2, EntityStatic, EntityType,
};
use libblkcapt::{
    model::{entities::HealthchecksObserverEntity, Entities},
    model::{entity_by_name_or_id, Entity},
};

use crate::ui::ScheduleArg;
pub mod observer;
pub mod pool;
pub mod restic;
pub mod sync;

pub fn dataset_search<'a>(
    entities: &'a Entities, query: &str,
) -> Result<EntityPath2<'a, BtrfsDatasetEntity, BtrfsPoolEntity>> {
    entity_search2(
        &entities.btrfs_pools,
        |p| p.datasets.as_slice(),
        entities.datasets(),
        query,
    )
}

pub fn container_search<'a>(
    entities: &'a Entities, query: &str,
) -> Result<EntityPath2<'a, BtrfsContainerEntity, BtrfsPoolEntity>> {
    entity_search2(
        &entities.btrfs_pools,
        |p| p.containers.as_slice(),
        entities.containers(),
        query,
    )
}

pub fn restic_search<'a>(entities: &'a Entities, query: &str) -> Result<&'a ResticContainerEntity> {
    entity_search1(entities.restic_containers.iter(), query)
}

pub fn pool_search<'a>(entities: &'a Entities, query: &str) -> Result<&'a BtrfsPoolEntity> {
    entity_search1(entities.btrfs_pools.iter(), query)
}

pub fn snapshot_sync_search<'a>(entities: &'a Entities, query: &str) -> Result<&'a SnapshotSyncEntity> {
    entity_search1(entities.snapshot_syncs.iter(), query)
}

pub fn observer_search<'a>(entities: &'a Entities, query: &str) -> Result<&'a HealthchecksObserverEntity> {
    entity_search1(entities.observers.iter(), query)
}

pub fn entity_by_type_lookup(entities: &Entities, etype: EntityType, id: EntityId) -> Option<String> {
    match etype {
        EntityType::Pool => entities.pool(id).map(|p| p.name().to_owned()),
        EntityType::Dataset => entities.dataset(id).map(|d| d.path()),
        EntityType::Container => entities.container(id).map(|d| d.path()),
        EntityType::SnapshotSync => entities.snapshot_sync(id).map(|s| s.name().to_owned()),
        EntityType::Observer => entities.observer(id).map(|o| o.name().to_owned()),
    }
}

pub fn entity_by_type_search<'a>(
    entities: &'a Entities, etype: EntityType, query: &str,
) -> Result<Box<dyn EntityPath + 'a>> {
    match etype {
        EntityType::Pool => {
            pool_search(entities, query).map(|entity| Box::new(EntityPath1 { entity }) as Box<dyn EntityPath>)
        }
        EntityType::Dataset => dataset_search(entities, query).map(|path| Box::new(path) as Box<dyn EntityPath>),
        EntityType::Container => container_search(entities, query).map(|path| Box::new(path) as Box<dyn EntityPath>),
        EntityType::SnapshotSync => {
            snapshot_sync_search(entities, query).map(|entity| Box::new(EntityPath1 { entity }) as Box<dyn EntityPath>)
        }
        EntityType::Observer => {
            observer_search(entities, query).map(|entity| Box::new(EntityPath1 { entity }) as Box<dyn EntityPath>)
        }
    }
}

fn entity_search1<'a, T1, I1>(all_entities: I1, query: &str) -> Result<&'a T1>
where
    T1: Entity + EntityStatic + AsRef<dyn Entity + 'a> + 'a,
    I1: Iterator<Item = &'a T1>,
{
    entity_by_name_or_id(all_entities, query)
}

fn entity_search2<'a, T1, T2, F2, I2>(
    parent_entities: &'a [T1], get_children: F2, all_children: I2, query: &str,
) -> Result<EntityPath2<'a, T2, T1>>
where
    T1: Entity + EntityStatic + 'a,
    T2: Entity + EntityStatic + 'a,
    F2: FnOnce(&'a T1) -> &'a [T2],
    I2: Iterator<Item = EntityPath2<'a, T2, T1>>,
{
    let parts = query.splitn(2, '/').collect::<Vec<_>>();
    if parts.len() == 2 {
        let parent = entity_by_name(parent_entities, parts[0])
            .with_context(|| format!("{} '{}' not found", T1::entity_type_static(), parts[0]))?;
        let entity = entity_by_name(get_children(parent), parts[1]).with_context(|| {
            format!(
                "{} '{}' not found in {} '{}'",
                T2::entity_type_static(),
                parts[1],
                T1::entity_type_static(),
                parts[0]
            )
        })?;
        Ok(EntityPath2 { entity, parent })
    } else {
        entity_by_name_or_id(all_children, parts[0])
    }
}

#[derive(Clap, Debug)]
pub struct RetentionCreateUpdateOptions {
    /// Specify one or more snapshot retention time intervals
    #[clap(short('i'), long, value_name("interval"))]
    retention_intervals: Option<Vec<IntervalSpecArg>>,

    /// Specify the minimum number of snapshots to retain
    #[clap(short('m'), long, value_name("count"))]
    retain_minimum: Option<NonZeroU32>,

    /// Set the schedule for pruning snapshots
    #[clap(long, value_name("cron"))]
    prune_schedule: Option<ScheduleArg>,
}

impl RetentionCreateUpdateOptions {
    fn update_retention(&self, retention: &mut Option<RetentionRuleset>) {
        if self.retain_minimum.is_some() || self.retention_intervals.is_some() {
            let retention = retention.get_or_insert_with(Default::default);
            if let Some(intervals) = self.retention_intervals.clone() {
                retention.interval = intervals.into_iter().map(|i| i.0).collect();
            }

            if let Some(minimum) = self.retain_minimum {
                retention.newest_count = minimum;
            }

            if let Some(schedule) = self.prune_schedule.clone() {
                retention.evaluation_schedule = schedule.into();
            }
        }
    }
}

#[derive(Clap, Debug)]
pub struct RetentionUpdateOptions {
    /// Prevent starting new snapshot pruning jobs on this dataset
    #[clap(long, conflicts_with("resume-pruning"))]
    pause_pruning: bool,

    #[clap(long)]
    resume_pruning: bool,
}

impl RetentionUpdateOptions {
    fn update_pruning(&self, pause_pruning: &mut bool) {
        if self.pause_pruning || self.resume_pruning {
            *pause_pruning = self.pause_pruning
        }
    }
}

#[derive(Debug, Clone)]
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

pub mod service {
    use anyhow::Result;
    use bytes::buf::Buf;
    use clap::Clap;
    use comfy_table::Cell;
    use libblkcapt::{
        core::system::{ActiveState, ActorState, SystemState, TerminalState},
        sys::net::ServiceClient,
    };

    use crate::ui::{comfy_id_header, comfy_name_value, print_comfy_table};

    #[derive(Clap, Debug)]
    pub struct ServiceStatusOptions {}

    pub async fn service_status(_: ServiceStatusOptions) -> Result<()> {
        let client = ServiceClient::default();
        let result = client.get("/").await?;
        let body = hyper::body::aggregate(result).await?;
        let mut system: SystemState = serde_json::from_reader(body.reader())?;
        system.actors.sort_by_key(|a| a.actor_id);

        print_comfy_table(
            vec![
                comfy_id_header(),
                Cell::new("Actor Type"),
                Cell::new("State"),
                Cell::new("Substate"),
            ],
            system.actors.into_iter().map(|a| {
                vec![
                    comfy_name_value(a.actor_id),
                    Cell::new(&a.actor_type),
                    actor_state_cell(&a.actor_state),
                    actor_substate_cell(a.actor_state),
                ]
            }),
        );

        Ok(())
    }

    pub fn actor_state_cell(state: &ActorState) -> Cell {
        Cell::new(state).fg(match state {
            ActorState::Started(..) => comfy_table::Color::Green,
            ActorState::Stopped(..) => comfy_table::Color::Yellow,
            ActorState::Dropped(..) => comfy_table::Color::Cyan,
            ActorState::Zombie(..) => comfy_table::Color::Red,
        })
    }

    pub fn actor_substate_cell(state: ActorState) -> Cell {
        let (message, color) = match state {
            ActorState::Started(active_state) => match active_state {
                ActiveState::Custom(state) => (state, comfy_table::Color::Green),
                ActiveState::Unresponsive => (ActiveState::Unresponsive.to_string(), comfy_table::Color::Red),
                ActiveState::Stopping => (ActiveState::Stopping.to_string(), comfy_table::Color::Yellow),
            },
            ActorState::Stopped(terminal_state)
            | ActorState::Dropped(terminal_state)
            | ActorState::Zombie(terminal_state) => (
                terminal_state.to_string(),
                match terminal_state {
                    TerminalState::Succeeded => comfy_table::Color::Green,
                    TerminalState::Cancelled => comfy_table::Color::Yellow,
                    TerminalState::Failed | TerminalState::Faulted | TerminalState::Indeterminate => {
                        comfy_table::Color::Red
                    }
                },
            ),
        };
        Cell::new(message).fg(color)
    }
}
