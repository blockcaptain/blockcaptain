use pnsystem::model::btrfs::{BtrfsDataset, BtrfsContainer, BtrfsPool, full_path, SubvolumeEntity};
use pnsystem::state;
use pnsystem::btrfs;
use anyhow::Result;

pub fn sync() -> Result<()> {
    let entities = state::load_entity_state();

    let sync = entities.snapshot_syncs().next().unwrap();
    let (dataset, dataset_pool) = entities.dataset_by_id(&sync.dataset_id()).unwrap();
    let (container, container_pool) = entities.container_by_id(&sync.container_id()).unwrap();
    
    println!("> {:?}", full_path(dataset_pool, dataset));
    println!("> {:?}", full_path(container_pool, container));

    let src_fs = btrfs::Filesystem::query_uuid(&dataset_pool.uuid)?;
    let src_subvol = src_fs.subvolume_by_uuid(dataset.uuid())?;

    println!("{:#?} ==== {:#?}", src_fs, src_subvol);
    // validate fs is mounted and generally validate that fs and subvolume mount and paths havent changed since config.
    // make a snapshot taking managed pool and dataset
    // make a snapshot  taking a btrfs Filesystem and Subvolume with a pathbuf that is a path within Fileystem.
    Ok(())

}

