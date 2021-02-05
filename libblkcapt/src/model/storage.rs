use crate::{data_dir, model};
use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fs::{self, File},
    path::PathBuf,
};
use std::{
    io::{BufReader, BufWriter},
    path::Path,
};

static SERVER_PATH: Lazy<PathBuf> = Lazy::new(|| {
    let mut path = data_dir();
    path.push("config");
    path.push("server.json");
    path
});

static ENTITY_PATH: Lazy<PathBuf> = Lazy::new(|| {
    let mut path = data_dir();
    path.push("config");
    path.push("entities.json");
    path
});

pub fn load_entity_config() -> model::Entities {
    read_state(&ENTITY_PATH).expect("FIXME")
}

pub fn store_entity_config(entities: model::Entities) {
    write_state(&ENTITY_PATH, &entities).expect("FIXME")
}

pub fn load_server_config() -> Result<model::ServerConfig> {
    read_state(&SERVER_PATH)
}

pub fn store_server_config(entities: model::ServerConfig) -> Result<()> {
    write_state(&SERVER_PATH, &entities)
}

fn write_state(path: &Path, state: &impl Serialize) -> Result<()> {
    // need the libc renameat2 PR merged to make this transactional.
    // write new file then swap in to place.
    if !path.exists() {
        fs::create_dir_all(path.parent().expect("config file always has a parent directory"))
            .context("failed to create directory structure for state")?;
    }
    let file = File::create(path).context("failed to create updated json state file")?;
    let writer = BufWriter::new(file);

    serde_json::to_writer_pretty(writer, state).context("failed to write json state data")
}

fn read_state<T: DeserializeOwned + Default>(path: &Path) -> Result<T> {
    if !path.exists() {
        return Ok(T::default());
    }

    let file = File::open(path).context("failed to open json state file")?;
    let reader = BufReader::new(file);

    serde_json::from_reader(reader).context("failed to read json state data")
}
