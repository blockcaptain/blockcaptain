pub mod btrfs;
pub mod fs;
pub mod net;

pub mod process {
    use anyhow::{anyhow, Result};
    use std::process::ExitStatus;

    pub fn exit_status_as_result(status: ExitStatus) -> Result<()> {
        match status {
            s if s.success() => Ok(()),
            s => Err(match s.code() {
                Some(c) => anyhow!("process exited with exit code: {}", c),
                None => anyhow!("process terminated by signal"),
            }),
        }
    }
}
