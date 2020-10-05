use libblkcapt::model::entities::SnapshotSyncEntity;
use xactor::{message, Actor, Addr, Context, Handler};
use super::{container::ContainerActor, dataset::DatasetActor};


pub struct SyncActor {
    dataset: Addr<DatasetActor>,
    container: Addr<ContainerActor>,
    model: SnapshotSyncEntity,
}

impl SyncActor {
    pub fn new(dataset: Addr<DatasetActor>, container: Addr<ContainerActor>, model: SnapshotSyncEntity) -> Self {
        Self {
            dataset,
            container,
            model,
        }
    }
}

#[async_trait::async_trait]
impl Actor for SyncActor {
}