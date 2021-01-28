use anyhow::{anyhow, Result};
use clap::Clap;
use libblkcapt::model::entities::{ResticContainerEntity, ResticRepository};
use libblkcapt::model::storage;

use super::{RetentionCreateUpdateOptions, RetentionUpdateOptions};

#[derive(Clap, Debug)]
pub struct ResticCreateUpdateOptions {
    #[clap(flatten)]
    retention: RetentionCreateUpdateOptions,

    /// Environment variable to set for the restic process
    #[clap(
        short,
        long,
        multiple_occurrences(true),
        multiple_values(false),
        takes_value(true),
        value_name("name=value")
    )]
    environment_variable: Vec<String>,
}

#[derive(Clap, Debug)]
pub struct ResticAttachOptions {
    /// Name of the restic container
    #[clap(short, long, default_value = "default")]
    name: String,

    #[clap(long)]
    custom: Option<String>,

    #[clap(flatten)]
    shared: ResticCreateUpdateOptions,
}

pub fn attach_restic(options: ResticAttachOptions) -> Result<()> {
    let mut entities = storage::load_entity_state();

    let repository = options
        .custom
        .ok_or_else(|| anyhow!("only custom is supported"))
        .map(ResticRepository::Custom)?;
    let mut restic = ResticContainerEntity::new(options.name, repository);

    restic.custom_environment = options
        .shared
        .environment_variable
        .iter()
        .map(|p| {
            // Simplify with nightly split_once
            let parts: Vec<_> = p.splitn(2, '=').collect();
            if parts.len() == 2 {
                Ok((parts[0].to_owned(), parts[1].to_owned()))
            } else {
                Err(anyhow!("environment variable definitions must contain '='"))
            }
        })
        .collect::<Result<_>>()?;

    options
        .shared
        .retention
        .update_retention(&mut restic.snapshot_retention);

    entities.restic_containers.push(restic);

    storage::store_entity_state(entities);
    Ok(())
}

#[derive(Clap, Debug)]
pub struct ResticUpdateOptions {
    /// The name or id of the restic container
    #[clap(value_name("sync|id"))]
    sync: String,

    #[clap(flatten)]
    retention_update: RetentionUpdateOptions,

    #[clap(flatten)]
    shared: ResticCreateUpdateOptions,
}

pub fn update_restic(_options: ResticUpdateOptions) -> Result<()> {
    //let mut entities = storage::load_entity_state();

    //storage::store_entity_state(entities);
    Ok(())
}
