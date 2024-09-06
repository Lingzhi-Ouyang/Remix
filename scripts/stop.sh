#!/bin/bash

## kill currently running zookeeper processes
PROC=$(ps -ef | grep zookeeper | grep -v grep | awk '{print $2}')
if [ -n "$PROC" ]; then
  echo "Killing stale ZooKeeper process(es) (ID):" "$PROC"
  kill "$PROC"
else
  echo "No running ZooKeeper processes to kill"
fi