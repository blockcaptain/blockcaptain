use crate::{data_dir, model};
use once_cell::sync::Lazy;
use std::io::{BufReader, BufWriter};
use std::{
    fs::{self, File},
    path::PathBuf,
};

static CONFIG_PATH: Lazy<PathBuf> = Lazy::new(|| {
    let mut path = data_dir();
    path.push("config");
    path.push("entities.json");
    path
});

pub fn load_entity_state() -> model::Entities {
    if !CONFIG_PATH.exists() {
        return model::Entities::default();
    }

    let file = File::open(CONFIG_PATH.as_path()).expect("FIXME");
    let reader = BufReader::new(file);

    serde_json::from_reader(reader).expect("FIXME")
}

pub fn store_entity_state(entities: model::Entities) {
    // need the libc renameat2 PR merged to make this safe.
    // need to use humantime serde, but the dependency versions were too specific which would cause downgrades.
    // store any state seperate from entities.

    if !CONFIG_PATH.exists() {
        fs::create_dir_all(CONFIG_PATH.parent().expect("config file always has a parent directory")).expect("FIXME");
    }
    let file = File::create(CONFIG_PATH.as_path()).expect("FIXME");
    let writer = BufWriter::new(file);

    serde_json::to_writer_pretty(writer, &entities).expect("FIXME");
}
