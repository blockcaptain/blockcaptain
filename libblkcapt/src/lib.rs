pub mod core;
pub mod model;
pub mod parsing;
pub mod sys;

#[cfg(test)]
mod tests {
    pub mod prelude {
        pub use indoc::indoc;
        pub use serial_test::serial;
    }
}
