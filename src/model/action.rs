use serde::{Deserialize, Serialize};
use uuid::Uuid;
use super::{Entity, EntityType};

#[derive(Serialize, Deserialize, Debug)]
pub struct SnapshotSync {
    id: Uuid,
    name: String,
    dataset: Uuid,
    container: Uuid,
}

impl SnapshotSync {
    pub fn dataset_id(&self) -> Uuid {
        self.dataset
    }
    pub fn container_id(&self) -> Uuid {
        self.container
    }
}

impl Entity for SnapshotSync {
    fn name(&self) -> &str {
        &self.name
    }
    fn id(&self) -> Uuid {
        self.id
    }
    fn entity_type(&self) -> EntityType {
        EntityType::SnapshotSync
    }
}