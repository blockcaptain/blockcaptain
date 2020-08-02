use log::*;
use anyhow::{anyhow, Context, Result};

pub struct Validation {
    name: String,
    is_valid: bool,
}

impl Validation {
    pub fn new(name: &str) -> Self {
        info!("Validating {}.", name);

        Self {
            name: name.to_owned(),
            is_valid: true,
        }
    }

    pub fn require(&mut self, name: &str, state: bool) {
        self.is_valid &= state;
        if state {
            debug!("Requirement satisified: {}", name);
        } else {
            error!("{}", name);
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.is_valid {
            debug!("Fully validated: {}", self.name);
            Ok(())
        } else {
            let mut upper = self.name.to_string();
            if let Some(c) = upper.get_mut(0..1) {
                c.make_ascii_uppercase();
            }
            Err(anyhow!("{} failed validation.", upper))
        }
    }
}