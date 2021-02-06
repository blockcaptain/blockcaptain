#!/bin/bash
set -uo pipefail

sudo blkcapteng test --container
STATUS=$?
gunzip *.log.json.gz
exit $STATUS
