#!/bin/bash

SCRIPT_DIR=$(cd $(dirname "$0") || exit;pwd)
WORKING_DIR=$(cd "$SCRIPT_DIR"/.. || exit;pwd)

# build
echo $WORKING_DIR
echo -e "\n>> Building project...\n"
cd "$WORKING_DIR"/checker && mvn clean install -DskipTests
echo "Done!"