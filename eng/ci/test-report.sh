#!/bin/bash
set -euo pipefail

cat test-results.json | cargo2junit > test-results.xml