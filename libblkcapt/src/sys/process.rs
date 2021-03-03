use anyhow::{anyhow, Context as _, Result};
use std::process::{Command, ExitStatus, Output, Stdio};

pub fn exit_status_as_result(status: ExitStatus) -> Result<()> {
    match status {
        s if s.success() => Ok(()),
        s => Err(exit_code_error(s)),
    }
}

pub fn output_as_result(output: Output) -> Result<Output> {
    if !output.status.success() {
        let stderr_string = String::from_utf8_lossy(output.stderr.as_slice());
        let output_error = Err(match stderr_string.is_empty() {
            true => anyhow!("unknown error in command. command produced no stderr output"),
            false => anyhow!("{}", stderr_string),
        });
        return output_error.context(exit_code_error(output.status));
    }
    Ok(output)
}

pub fn output_stdout_to_result(result: std::io::Result<Output>) -> Result<String> {
    convert_result(result)
        .and_then(output_as_result)
        .and_then(|o| String::from_utf8(o.stdout).context("failed to parse command output to utf8"))
}

pub fn output_to_result(result: std::io::Result<Output>) -> Result<()> {
    convert_result(result).and_then(output_as_result).map(|_| ())
}

pub fn output_self_to_result(result: std::io::Result<Output>) -> Result<Output> {
    convert_result(result)
}

fn convert_result(result: std::io::Result<Output>) -> Result<Output> {
    result.context("waiting for subprocess result failed")
}

fn exit_code_error(status: ExitStatus) -> anyhow::Error {
    match status.code() {
        Some(c) => anyhow!("process exited with exit code: {}", c),
        None => anyhow!("process terminated by signal"),
    }
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
        output_stdout_to_result(command.output())
    }
}
