#!/bin/bash
set -euo pipefail

cargo clippy -- -A dead-code -D clippy::unwrap_used -D warnings