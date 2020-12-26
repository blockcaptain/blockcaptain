use anyhow::{Context, Result};
use libblkcapt::model::{
    entities::BtrfsDatasetEntity,
    entities::BtrfsPoolEntity,
    entities::{BtrfsContainerEntity, SnapshotSyncEntity},
    entity_by_name, EntityId, EntityPath, EntityPath1, EntityPath2, EntityStatic, EntityType,
};
use libblkcapt::{
    model::{entities::HealthchecksObserverEntity, Entities},
    model::{entity_by_name_or_id, Entity},
};
pub mod observer;
pub mod pool;

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

pub mod service {
    use anyhow::Result;
    use bytes::buf::BufExt;
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
