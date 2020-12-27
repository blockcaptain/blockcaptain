#!/bin/bash
set -euo pipefail

cargo tarpaulin -p libblkcapt,blkcaptwrk -v --workspace --exclude blkcaptctl --exclude-files blkcaptctl --coveralls ${COVERALLS_KEY} -- -Z unstable-options --format json | tee output.json
grep -P "^{.*}$" output.json > test-results.json