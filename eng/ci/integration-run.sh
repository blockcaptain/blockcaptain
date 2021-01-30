#!/bin/bash
set -euo pipefail

mkdir -p target/debian
wget "https://github.com/blockcaptain/restic/releases/latest/download/restic" -O target/debian/restic
blkcapteng test --container
