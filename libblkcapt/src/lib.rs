use anyhow::{Context, Result};
use std::path::PathBuf;
pub mod core;
pub mod model;
pub mod parsing;
pub mod sys;

pub fn data_dir() -> PathBuf {
    PathBuf::from("/var/lib/blockcaptain")
}

pub fn runtime_dir() -> PathBuf {
    PathBuf::from("/run/blockcaptain")
}

pub fn create_data_dir() -> Result<PathBuf> {
    let data_dir = data_dir();
    std::fs::create_dir_all(&data_dir).context("failed to create the blockcaptain data directory")?;
    Ok(data_dir)
}

pub fn error_cause(error: &anyhow::Error) -> String {
    use std::fmt::Write;

    let mut cause_string = String::new();
    for (index, cause) in error.chain().enumerate().skip(1) {
        writeln!(&mut cause_string, "cause {}: {}", index, cause).expect("infallible");
    }
    cause_string
}

#[cfg(test)]
mod tests {
    pub mod prelude {
        pub use indoc::indoc;
        pub use serial_test::serial;
    }
}
