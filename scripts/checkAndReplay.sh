#!/bin/bash

# generate traces using TLC, parse them and replay in the implementation.

## kill current running zookeeper processes
ps -ef | grep zookeeper | grep -v grep | awk '{print $2}' | xargs kill -9

SCRIPT_DIR=$(cd $(dirname "$0") || exit;pwd)
WORKING_DIR=$(cd "$SCRIPT_DIR"/.. || exit;pwd)
echo "## Working directory: $WORKING_DIR"

echo -e "\n==========Model level=========="
cd "$WORKING_DIR"/generator || exit
bash run.sh

echo -e "\n==========Code level=========="
cd "$WORKING_DIR"/scripts || exit
TARGET_DIR=$(ls -dt ../traces/*_output | head -1 | awk -F '/' '{ print $NF }')
bash replay.sh "${TARGET_DIR}"