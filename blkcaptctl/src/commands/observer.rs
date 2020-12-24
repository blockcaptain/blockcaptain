use super::{entity_by_type_lookup, entity_by_type_search, observer_search};
use crate::ui::*;
use anyhow::{bail, Context, Result};
use clap::Clap;
use comfy_table::Cell;
use hyper::Uri;
use libblkcapt::core::ObservationRouter;
use libblkcapt::model::{entity_by_id_mut, entity_by_name_or_id, storage, Entity};
use libblkcapt::{core::ObservableEventStage, model::entities::HealthchecksHeartbeat};
use libblkcapt::{
    core::ObservationEmitter,
    model::{
        entities::HealthchecksObserverEntity,
        entities::{HealthchecksObservation, ObservableEvent, Observation},
        Entities,
    },
};
use slog_scope::*;
use std::{str::FromStr, time::Duration};
use uuid::Uuid;

#[derive(Clap, Debug)]
pub struct ObserverCreateUpdateOptions {
    /// Custom URL (healthchecks self-host)
    #[clap(short, long, value_name("url"))]
    custom_url: Option<Uri>,

    /// Heartbeat healthchecks ID
    #[clap(short, long, value_name("healthchecks_id"))]
    heartbeat: Option<UuidArg>,

    /// Heartbeat frequency
    #[clap(long, value_name("duration"))]
    heartbeat_frequency: Option<humantime::Duration>,
}

impl ObserverCreateUpdateOptions {
    fn validate_frequency(&self) -> Result<()> {
        if self.heartbeat_frequency.is_some() && self.heartbeat.is_none() {
            bail!("heartbeat-frequency requires the heartbeat option")
        }
        Ok(())
    }

    fn maybe_custom_url(&self) -> Option<String> {
        self.custom_url
            .as_ref()
            .map(|u| u.to_string())
            .filter(|s| s != ObservationEmitter::DEFAULT_URL)
    }

    fn maybe_frequency(&self) -> Option<Duration> {
        self.heartbeat_frequency.map(|f| *f)
    }

    fn maybe_heartbeat(&self) -> Option<Uuid> {
        self.heartbeat.as_ref().map(|h| h.uuid())
    }

    fn maybe_heartbeat_model(&self) -> Result<Option<HealthchecksHeartbeat>> {
        self.validate_frequency()?;
        self.maybe_heartbeat().map_or::<Result<_>, _>(Ok(None), |id| {
            let mut model = HealthchecksHeartbeat::new(id);
            if let Some(duration) = self.maybe_frequency() {
                model.set_frequency(duration)?
            }
            Ok(Some(model))
        })
    }
}

#[derive(Clap, Debug)]
pub struct ObserverCreateOptions {
    /// Name of the observer
    #[clap(short, long, default_value = "default")]
    name: String,

    /// Type of observer (must be "healthchecks")
    #[clap(short('t'), long("type"), value_name("type"), required(true))]
    observer_type: String,

    #[clap(flatten)]
    shared: ObserverCreateUpdateOptions,

    /// Observations specifications
    #[clap()]
    observations: Vec<ObservationArg>,
}

pub fn create_observer(options: ObserverCreateOptions) -> Result<()> {
    let mut entities = storage::load_entity_state();

    if options.observer_type != "healthchecks" {
        bail!("only healthchecks is supported");
    }

    let observations = build_observation_models(&entities, &options.observations)?;

    let mut observer = HealthchecksObserverEntity::new(options.name.clone(), observations);
    observer.custom_url = options.shared.maybe_custom_url();
    observer.heartbeat = options.shared.maybe_heartbeat_model()?;

    entities.attach_observer(observer)?;

    storage::store_entity_state(entities);

    Ok(())
}

#[derive(Clap, Debug)]
pub struct ObserverUpdateOptions {
    /// The name or id of the observer
    #[clap(value_name("observer|id"))]
    observer: String,

    #[clap(flatten)]
    shared: ObserverCreateUpdateOptions,

    /// Observation to add
    #[clap(
        long,
        multiple_occurrences(true),
        multiple_values(false),
        takes_value(true),
        value_name("observation")
    )]
    add: Vec<ObservationArg>,

    /// Observation to remove by index
    #[clap(
        long,
        multiple_occurrences(true),
        multiple_values(false),
        takes_value(true),
        value_name("index")
    )]
    remove: Vec<usize>,

    #[clap(long, conflicts_with_all(&["heartbeat", "heartbeat-frequency"]))]
    remove_heartbeat: bool,
}

pub fn update_observer(options: ObserverUpdateOptions) -> Result<()> {
    let mut entities = storage::load_entity_state();

    let observations = build_observation_models(&entities, &options.add)?;

    let observer = observer_search(&entities, &options.observer).map(|o| o.id())?;
    let observer =
        entity_by_id_mut(entities.observers.as_mut_slice(), observer).expect("entity exists, found in search");

    let mut removes = options.remove.clone();
    removes.sort_unstable();
    for index in removes.iter().rev() {
        observer.observations.remove(*index);
    }

    observer.observations.extend_from_slice(&observations);

    if options.shared.custom_url.is_some() {
        observer.custom_url = options.shared.maybe_custom_url();
    }

    if let Some(heartbeat) = &mut observer.heartbeat {
        if let Some(id) = options.shared.heartbeat.as_ref() {
            heartbeat.healthcheck_id = id.uuid();
        }
        if let Some(duration) = options.shared.maybe_frequency() {
            heartbeat.set_frequency(duration)?;
        }
    } else {
        observer.heartbeat = options.shared.maybe_heartbeat_model()?;
    }

    if options.remove_heartbeat {
        observer.heartbeat = None;
    }

    storage::store_entity_state(entities);

    Ok(())
}

#[derive(Clap, Debug)]
pub struct ObserverTestOptions {
    /// Send a failure instead of success
    #[clap(short, long)]
    fail: bool,

    /// Send a test heartbeat
    #[clap(short, long)]
    heartbeat: bool,

    /// The name or id of the observer
    #[clap(value_name("observer|id"))]
    observer: String,

    /// Source entity of the event
    #[clap(value_name("[path/]entity|id"))]
    entity: String,

    /// Event to emit
    #[clap()]
    event: ObservableEvent,
}

pub async fn test_observer(options: ObserverTestOptions) -> Result<()> {
    debug!("Command 'create_observer': {:?}", options);

    let entities = storage::load_entity_state();

    let observer = observer_search(&entities, &options.observer)?;

    let entity = entity_by_type_search(&entities, options.event.entity_type(), &options.entity)?;
    info!("Found {}.", entity.path());

    let emitter = observer
        .custom_url
        .clone()
        .map_or_else(ObservationEmitter::default, ObservationEmitter::new);

    if options.heartbeat {
        if let Some(heartbeat_config) = &observer.heartbeat {
            info!("Testing heartbeat...");
            emitter
                .emit(heartbeat_config.healthcheck_id, ObservableEventStage::Succeeded)
                .await?;
        } else {
            bail!("Heartbeat requested, but not heartbeat configured on this observer");
        }
    }

    let router = ObservationRouter::new(observer.observations.clone());
    let matches = router.route(entity.id(), options.event);
    if matches.is_empty() {
        bail!("No matching observations found");
    }

    for observation_match in matches {
        info!("Testing match: {:?}", observation_match);
        emitter
            .emit(observation_match.healthcheck_id, ObservableEventStage::Starting)
            .await?;
        tokio::time::delay_for(Duration::from_millis(300)).await;

        let end_stage = match options.fail {
            true => ObservableEventStage::Failed(String::from("This is a test failure.")),
            false => ObservableEventStage::Succeeded,
        };
        emitter.emit(observation_match.healthcheck_id, end_stage).await?;
        info!("Test succeeded.");
    }

    Ok(())
}

#[derive(Clap, Debug)]
pub struct ObserverListOptions {}

pub fn list_observer(options: ObserverListOptions) -> Result<()> {
    debug!("Command 'list_pool': {:?}", options);

    let entities = storage::load_entity_state();

    if entities.observers.is_empty() {
        info!("No observers configured")
    } else {
        print_comfy_table(
            vec![
                comfy_id_header(),
                Cell::new("Observer Name"),
                Cell::new("Observations"),
                Cell::new("Heartbeat"),
            ],
            entities.observers.iter().map(|p| {
                vec![
                    comfy_id_value(p.id()),
                    comfy_name_value(p.name()),
                    Cell::new(p.observations.len()),
                    comfy_feature_state_cell(p.heartbeat_state()),
                ]
            }),
        );
    }

    Ok(())
}

#[derive(Clap, Debug)]
pub struct ObserverDeleteOptions {
    /// The name or id of the observer
    #[clap(value_name("observer|id"))]
    observer: String,
}

pub fn delete_observer(options: ObserverDeleteOptions) -> Result<()> {
    let mut entities = storage::load_entity_state();

    let (id, name) = {
        let observer = entity_by_name_or_id(entities.observers.iter(), &options.observer)?;
        (observer.id(), observer.name().to_owned())
    };

    entities.observers.remove(
        entities
            .observers
            .iter()
            .position(|h| h.id() == id)
            .expect("id always exists"),
    );

    storage::store_entity_state(entities);
    info!("Deleted observer '{}'", name);

    Ok(())
}

#[derive(Clap, Debug)]
pub struct ObserverShowOptions {
    /// The name or id of the observer
    #[clap(value_name("observer|id"))]
    observer: String,
}

pub fn show_observer(options: ObserverShowOptions) -> Result<()> {
    let entities = storage::load_entity_state();

    let observer = observer_search(&entities, &options.observer)?;

    print_comfy_info(vec![
        (comfy_id_header(), comfy_id_value_full(observer.id()).into()),
        (Cell::new("Name"), comfy_name_value(observer.name()).into()),
        (Cell::new("Type"), Cell::new("healthchecks").into()),
        (
            Cell::new("Custom URL"),
            Cell::new(
                observer
                    .custom_url
                    .as_ref()
                    .unwrap_or(&format!("None (using default: {})", ObservationEmitter::DEFAULT_URL)),
            )
            .into(),
        ),
        (
            Cell::new("Heartbeat"),
            Cell::new(
                observer
                    .heartbeat
                    .as_ref()
                    .map(|s| {
                        format!(
                            "Every {} (to Healthcheck ID {})",
                            humantime::Duration::from(s.frequency),
                            s.healthcheck_id
                        )
                    })
                    .unwrap_or_else(|| "Disabled".to_owned()),
            )
            .into(),
        ),
    ]);

    info!(#"bc_raw", "");

    print_comfy_table(
        vec![
            comfy_index_header(),
            Cell::new("Entity"),
            Cell::new("Event"),
            Cell::new("Healthcheck ID"),
        ],
        observer.observations.iter().enumerate().map(|(i, model)| {
            vec![
                comfy_name_value(i),
                Cell::new(
                    find_observed_entity(&entities, &model.observation)
                        .unwrap_or_else(|| format!("{} <MISSING>", model.observation.entity_id)),
                ),
                Cell::new(model.observation.event),
                Cell::new(model.healthcheck_id),
            ]
        }),
    );

    Ok(())
}

#[derive(Debug)]
pub struct ObservationArg {
    healthcheck_id: Uuid,
    entity: String,
    event: ObservableEvent,
}

impl FromStr for ObservationArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let outter = s.split('=').collect::<Vec<_>>();
        let inner = outter[0].split(':').collect::<Vec<_>>();
        if inner.len() != 2 || outter.len() != 2 {
            bail!("Observation format is <[path/]entity|id>:<event>=<healthchecks_id>");
        };
        Ok(Self {
            entity: inner[0].to_owned(),
            healthcheck_id: UuidArg::parse(outter[1]).context("Healthcheck ID is invalid")?,
            event: ObservableEvent::from_str(inner[1]).context(format!("Event name '{}' is invalid", inner[1]))?,
        })
    }
}

fn build_observation_models(entities: &Entities, args: &[ObservationArg]) -> Result<Vec<HealthchecksObservation>> {
    args.iter()
        .map(|o| {
            entity_by_type_search(&entities, o.event.entity_type(), &o.entity).map(|e| HealthchecksObservation {
                healthcheck_id: o.healthcheck_id,
                observation: Observation {
                    entity_id: e.id(),
                    event: o.event,
                },
            })
        })
        .collect::<Result<Vec<_>>>()
}

fn find_observed_entity(entities: &Entities, observation: &Observation) -> Option<String> {
    entity_by_type_lookup(&entities, observation.event.entity_type(), observation.entity_id)
}
