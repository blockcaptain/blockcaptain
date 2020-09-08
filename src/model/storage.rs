use crate::model;
use serde_json;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::path::Path;

pub fn load_entity_state() -> model::Entities {
    let path = Path::new("/etc/blkcapt/entities.json");
    if !path.exists() {
        return model::Entities::default();
    }

    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);

    serde_json::from_reader(reader).unwrap()
}

pub fn store_entity_state(entities: model::Entities) -> () {
    // need the libc renameat2 PR merged to make this safe.
    // need to use humantime serde, but the dependency versions were too specific which would cause downgrades.
    // store any state seperate from entities.

    let path = Path::new("/etc/blkcapt");
    if !path.exists() {
        fs::create_dir(path).unwrap();
    }
    let file = File::create("/etc/blkcapt/entities.json").unwrap();
    let writer = BufWriter::new(file);

    serde_json::to_writer_pretty(writer, &entities).unwrap()
}
