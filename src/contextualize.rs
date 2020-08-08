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
            error!("{}", Self::capitolize(self.name.clone()));
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.is_valid {
            debug!("Fully validated: {}", self.name);
            Ok(())
        } else {
            Err(anyhow!("{} failed validation.", Self::capitolize(self.name.clone())))
        }
    }

    fn capitolize(mut string: String) -> String {
        if let Some(c) = string.get_mut(0..1) {
            c.make_ascii_uppercase();
        }
        string
    }
}