#!/bin/bash

# build the checker and replay given traces in the implementation.
# Usage: ./buildAndReplay.sh <trace_dir>  # trace_dir is under the directory _traces_

SCRIPT_DIR=$(cd $(dirname "$0") || exit;pwd)
WORKING_DIR=$(cd "$SCRIPT_DIR"/.. || exit;pwd)

echo "## Working directory: $WORKING_DIR"

bash build.sh

if [ -n "$1" ]; then
  bash replay.sh "$1"
else
  bash replay.sh
fi
