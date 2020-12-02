use serde::{Deserialize, Serialize};
use strum_macros::Display;

#[derive(Serialize, Deserialize)]
pub struct SystemState {
    pub actors: Vec<SystemActor>,
}

#[derive(Serialize, Deserialize)]
pub struct SystemActor {
    pub actor_id: u64,
    pub actor_state: ActorState,
    pub actor_type: String,
}

#[derive(Serialize, Deserialize, Display, Clone)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ActorState {
    Started(ActiveState),
    Stopped(TerminalState),
    Dropped(TerminalState),
    Zombie(TerminalState),
}

#[derive(Serialize, Deserialize, Display, Clone)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ActiveState {
    Custom(String),
    Unresponsive,
    Stopping,
}

#[derive(Serialize, Deserialize, Display, Clone, Copy)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum TerminalState {
    Succeeded,
    Failed,
    Cancelled,
    Faulted,
    Indeterminate,
}

impl Default for TerminalState {
    fn default() -> Self {
        TerminalState::Indeterminate
    }
}
