use anyhow::{anyhow, Context, Result};
use duct::Expression;

#[cfg(test)]
pub mod mocks {
    use mockall::automock;
    #[automock]
    pub trait FakeCmd {
        fn data() -> String;
    }
}

// Test Macro Support {{{
#[cfg(test)]
macro_rules! duct_cmd {
    ( $program:expr $(, $arg:expr )* ) => {
        {
        use crate::process::mocks::FakeCmd;
        $( let _ = $arg; )*
        duct::cmd!("echo", crate::process::mocks::MockFakeCmd::data())
        }
    };
}

#[cfg(not(test))]
macro_rules! duct_cmd {
    ( $program:expr $(, $arg:expr )* ) => {
        duct::cmd!($program, $($arg),*)
    };
}
// }}}

pub fn read_with_stderr_context(expression: Expression) -> Result<String> {
    let output = expression
        .unchecked()
        .stderr_capture()
        .run()
        .context("External command failed to launch.")?;
    if !output.status.success() {
        let stderr_string = String::from_utf8_lossy(output.stderr.as_slice());
        let output_error =  Err(match stderr_string.is_empty() {
            true => anyhow!("Unknown error in command. Command produced no stderr output."),
            false => anyhow!("{}", stderr_string)
        });
        return output_error.context(match output.status.code() {
            Some(c) => anyhow!("{:?} exited with code {}.", expression, c),
            None => anyhow!("{:?} terminated by signal.", expression),
        });
    }
    String::from_utf8(output.stdout).context("Failed to parse command output to utf8.")
}
