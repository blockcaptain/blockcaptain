#!/bin/bash
set -euo pipefail

function program_exists {
  builtin type -P $1 &> /dev/null
}

if ! program_exists cargo-deb; then
    cargo install cargo-deb --version ${CARGO_CARGO_DEB_VERSION}
fi
