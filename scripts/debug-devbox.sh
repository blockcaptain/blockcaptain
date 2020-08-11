#!/bin/bash

# Copy ~/.vscode-server/extensions/vadimcn.vscode-lldb-1.5.3/lldb and this script to devbox.
# Run on devbox in lldb/bin.

exec sudo ./lldb-server platform-list --server --listen *:8000