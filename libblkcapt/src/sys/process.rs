use anyhow::{anyhow, Context as _, Result};
use std::process::{Command, ExitStatus, Output, Stdio};

pub fn exit_status_as_result(status: ExitStatus) -> Result<()> {
    match status {
        s if s.success() => Ok(()),
        s => Err(match s.code() {
            Some(c) => anyhow!("process exited with exit code: {}", c),
            None => anyhow!("process terminated by signal"),
        }),
    }
}

pub fn output_as_result(output: Output) -> Result<String> {
    if !output.status.success() {
        let stderr_string = String::from_utf8_lossy(output.stderr.as_slice());
        let output_error = Err(match stderr_string.is_empty() {
            true => anyhow!("Unknown error in command. Command produced no stderr output."),
            false => anyhow!("{}", stderr_string),
        });
        return output_error.context(match output.status.code() {
            Some(c) => anyhow!("process exited with code {}", c),
            None => anyhow!("process terminated by signal"),
        });
    }
    String::from_utf8(output.stdout).context("Failed to parse command output to utf8.")
}

#[cfg_attr(test, mockall::automock)]
pub mod double {
    use super::*;

    pub fn run_command(mut command: Command) -> std::io::Result<Output> {
        command.output()
    }

    pub fn run_command_as_result(mut command: Command) -> Result<String> {
        command.stderr(Stdio::piped());
        command.stdout(Stdio::piped());
        output_as_result(command.output()?)
    }
}
