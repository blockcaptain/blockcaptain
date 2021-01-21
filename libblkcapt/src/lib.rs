use anyhow::{Context, Result};
use std::path::PathBuf;
pub mod core;
pub mod model;
pub mod parsing;
pub mod sys;

pub fn data_dir() -> PathBuf {
    PathBuf::from("/var/lib/blockcaptain")
}

pub fn create_data_dir() -> Result<PathBuf> {
    let data_dir = data_dir();
    std::fs::create_dir_all(&data_dir).context("failed to create the blockcaptain data directory")?;
    Ok(data_dir)
}

#[cfg(test)]
mod tests {
    pub mod prelude {
        pub use indoc::indoc;
        pub use serial_test::serial;
    }
}
