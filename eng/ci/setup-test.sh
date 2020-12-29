#!/bin/bash
set -euo pipefail

function program_exists {
  builtin type -P $1 &> /dev/null
}

if ! program_exists cargo-tarpaulin; then
    sudo apt-get install libssl-dev pkg-config build-essential
    cargo install cargo-tarpaulin --version ${CARGO_CARGO_TARPAULIN_VERSION}
fi

if ! program_exists cargo2junit; then
    cargo install cargo2junit --version ${CARGO_CARGO2JUNIT_VERSION}
fi
