#!/bin/bash

# Usage: generate traces using TLC.

SCRIPT_DIR=$(cd $(dirname "$0") || exit;pwd)
cd "$SCRIPT_DIR" || exit

echo -e "\n>> Gnerating model-level traces..."
python3 tlcwrapper.py Zab-simulate.ini
