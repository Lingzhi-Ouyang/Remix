#!/bin/bash

# Usage: generate traces and parse them.

SCRIPT_DIR=$(cd $(dirname "$0") || exit;pwd)
cd "$SCRIPT_DIR" || exit

bash generate_traces.sh

bash read_latest_traces.sh