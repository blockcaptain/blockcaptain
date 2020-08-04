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
