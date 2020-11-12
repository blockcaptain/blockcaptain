use anyhow::{Context, Result};
use libblkcapt::model::{
    entities::BtrfsDatasetEntity,
    entities::BtrfsPoolEntity,
    entities::{BtrfsContainerEntity, SnapshotSyncEntity},
    entity_by_name, EntityPath, EntityPath1, EntityPath2, EntityStatic, EntityType,
};
use libblkcapt::{
    model::{entities::HealthchecksObserverEntity, Entities},
    model::{entity_by_name_or_id, Entity},
};
use uuid::Uuid;
pub mod observer;
pub mod pool;

pub fn dataset_search<'a>(
    entities: &'a Entities,
    query: &str,
) -> Result<EntityPath2<'a, BtrfsDatasetEntity, BtrfsPoolEntity>> {
    entity_search2(
        &entities.btrfs_pools,
        |p| p.datasets.as_slice(),
        entities.datasets(),
        query,
    )
}

pub fn container_search<'a>(
    entities: &'a Entities,
    query: &str,
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

pub fn entity_by_type_lookup(entities: &Entities, etype: EntityType, id: Uuid) -> Option<String> {
    match etype {
        EntityType::Pool => entities.pool(id).map(|p| p.name().to_owned()),
        EntityType::Dataset => entities.dataset(id).map(|d| d.path()),
        EntityType::Container => entities.container(id).map(|d| d.path()),
        EntityType::SnapshotSync => entities.snapshot_sync(id).map(|s| s.name().to_owned()),
        EntityType::Observer => entities.observer(id).map(|o| o.name().to_owned()),
    }
}

pub fn entity_by_type_search<'a>(
    entities: &'a Entities,
    etype: EntityType,
    query: &str,
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
    parent_entities: &'a [T1],
    get_children: F2,
    all_children: I2,
    query: &str,
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
