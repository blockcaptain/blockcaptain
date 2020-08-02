use uuid::Uuid;
use std::path::PathBuf;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct BtrfsPool 
{
    mountpoint_path: PathBuf,
    name: String,
    uuid: Uuid,
    uuid_subs: Vec<Uuid>
}