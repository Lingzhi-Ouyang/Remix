#!/bin/bash

SCRIPT_DIR=$(cd $(dirname "$0") || exit;pwd)

cd "$SCRIPT_DIR" || exit

echo -e "\n>> Running model checking..."
bash generate_trace.sh

RAW_DIR=$(ls -dt ../output/model_*[^output] | head -1)
echo -e "\n## Raw data directory: ${RAW_DIR}"

DIR_NAME=$(echo ${RAW_DIR} | awk -F'/' '{print $3}')
TRACE_DIR="../traces/"${DIR_NAME}"_output"

echo -e "\n>> Parsing traces..."
echo -e "\n## Trace directory: ${TRACE_DIR}\n"
bash read_trace.sh $RAW_DIR $TRACE_DIR
