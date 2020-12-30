#!/bin/bash

cargo build --release
cargo deb --manifest-path ./blkcaptctl/Cargo.toml --no-build --fast
dpkg -c ./target/debian/blockcaptain_*.deb
dpkg -I ./target/debian/blockcaptain_*.deb
