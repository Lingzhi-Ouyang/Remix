#!/bin/bash

SCRIPT_DIR=$(cd $(dirname "$0") || exit;pwd)
WORKING_DIR=$(cd "$SCRIPT_DIR"/.. || exit;pwd)

echo "## Working directory: $WORKING_DIR"

bash build.sh

if [ -n "$1" ]; then
  bash replay.sh "$1"
else
  bash replay.sh
fi
