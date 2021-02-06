#!/bin/bash
set -euo pipefail

sudo blkcapteng test --container
gunzip *.log.json.gz
