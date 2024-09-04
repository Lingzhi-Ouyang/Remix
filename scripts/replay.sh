#!/bin/bash

# replay given traces in the implementation.
# Usage: ./replay.sh <trace_dir>  # trace_dir is under the directory _traces_

## kill current running zookeeper processes
ps -ef | grep zookeeper | grep -v grep | awk '{print $2}' | xargs kill -9

SCRIPT_DIR=$(cd $(dirname "$0") || exit;pwd)
WORKING_DIR=$(cd "$SCRIPT_DIR"/.. || exit;pwd)

cd "$WORKING_DIR"/scripts || exit

echo -e "\n>> Configuring trace directory..."
TRACE_DIR="../traces/demo"

if [ -n "$1" ]; then
  TRACE_DIR="../traces/"$1
  OUTPUT_DIR="../results/"$1
fi
sed -i -e "s|^traceDir =.*|traceDir = ${TRACE_DIR}|g" zookeeper.properties
echo "## Trace directory: ${TRACE_DIR}"

echo -e "\n>> Setting result directory..."
tag=$(date "+%y-%m-%d-%H-%M-%S")
REPLAY_DIR="${OUTPUT_DIR}_replay_${tag}"
mkdir -p "${REPLAY_DIR}"
cp zk_log.properties "${REPLAY_DIR}"
echo "## Result directory: ${REPLAY_DIR}"

echo -e "\n>> Running test...\n"
JAVA_VERSION=$(java -version 2>&1 |awk -F '[".]+' 'NR==1{ print $2 }')
if [[ $JAVA_VERSION -le 8 ]]; then
  nohup java -ea -jar ../checker/zookeeper-ensemble/target/zookeeper-ensemble-jar-with-dependencies.jar zookeeper.properties ${REPLAY_DIR} > ${REPLAY_DIR}/${tag}.out 2>&1 &
else
  nohup java -ea --add-opens=java.base/java.lang=ALL-UNNAMED -jar ../checker/zookeeper-ensemble/target/zookeeper-ensemble-jar-with-dependencies.jar zookeeper.properties ${REPLAY_DIR} > ${REPLAY_DIR}/${tag}.out 2>&1 &
fi

