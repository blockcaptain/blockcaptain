use anyhow::{Error, Result};
use slog::{error, info, Logger};

pub fn unhandled_error(log: &Logger, error: Error) {
    error!(log, "unhandled error"; "error" => %error);
    for cause in error.chain().skip(1) {
        info!(log, "error caused by"; "error" => %cause);
    }
}

pub fn unhandled_result<T>(log: &Logger, result: Result<T>) {
    let _ = result.map_err(|e| unhandled_error(log, e));
}
