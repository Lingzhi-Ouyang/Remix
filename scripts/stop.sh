#!/bin/bash

## Attention! This will kill all currently running zookeeper processes
ps -ef | grep zookeeper | grep -v grep | awk '{print $2}' | xargs kill -9