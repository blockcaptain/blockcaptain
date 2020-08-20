use pnsystem::model::btrfs::{BtrfsDataset, BtrfsContainer, BtrfsPool, full_path};
use pnsystem::state;

pub fn sync() {
    let entities = state::load_entity_state();

    let sync = entities.snapshot_syncs().next().unwrap();
    let dataset = entities.dataset_by_id(&sync.dataset_id()).unwrap();
    let container = entities.container_by_id(&sync.container_id()).unwrap();
    
    println!("{:?}", full_path(dataset.1, dataset.0));
    println!("{:?}", full_path(container.1, container.0));
}