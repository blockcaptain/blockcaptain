use std::sync::Arc;
use xactor::Addr;
use libblkcapt::{core::{BtrfsContainer, BtrfsPool}, model::entities::BtrfsContainerEntity};
use super::pool::PoolActor;
use anyhow::Result;

pub struct ContainerActor {
    pool: Addr<PoolActor>,
    container: Arc<BtrfsContainer>,
}


// else if let Ok(container) = self.container(msg.dataset_or_container_id) {
//     let rules = container
//         .model()
//         .snapshot_retention
//         .as_ref()
//         .expect("Exists based on actor start.");
//     observable_func(container.model().id(), ObservableEvent::ContainerPrune, move || {
//         let result = container.source_dataset_ids().and_then(|ids| {
//             ids.iter()
//                 .map(|id| {
//                     trace!("Running prune for dataset id {} in container {}.", id, container);
//                     container
//                         .snapshots(*id)
//                         .and_then(|snapshots| evaluate_retention(snapshots, rules))
//                         .and_then(prune_snapshots)
//                 })
//                 .collect::<Result<()>>()
//         });
//         ready(result)
//     })
//     .await