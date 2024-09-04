#!/bin/bash

RAW_DIR=$1
RAW_FILES=$(ls ${RAW_DIR})
TRACE_DIR=$2

INI_FILE="${RAW_DIR}/MC.ini"
SPEC_VERSION=$(grep ^target: $INI_FILE | awk -F '[./]+' 'NR==1{ print $2 }')
echo $SPEC_VERSION

starttime=`date +'%Y-%m-%d %H:%M:%S'`
start_seconds=`date +%s`
echo "Start: "$starttime
echo "----------------"

counter=0

mkdir -p ${TRACE_DIR}

for file in ${RAW_FILES}
do
  if [[ $file == trace* ]]; then
    counter=$((counter+1))
    if [[ "$counter" -gt 2 ]]; then
      python3 trace_reader.py ${RAW_DIR}/$file -o ${TRACE_DIR}/$file -f true -i 2 -v $SPEC_VERSION
      counter=0
    else
      python3 trace_reader.py ${RAW_DIR}/$file -o ${TRACE_DIR}/$file -i 2 -v $SPEC_VERSION
    fi
    if [ -f ${TRACE_DIR}/${file}*.json ]; then
      cp ${RAW_DIR}/$file ${TRACE_DIR}/
    fi
  fi
done

endtime=`date +'%Y-%m-%d %H:%M:%S'`
# end_seconds=$(date --date="$endtime" +%s);
end_seconds=`date +%s`
echo "----------------"
echo "Done: "$endtime
echo "Used time：$((end_seconds-start_seconds))s"

echo "================"

cd ${TRACE_DIR}
OUTPUT_FILE_COUNT=$(ls | wc -l)
if [[  $OUTPUT_FILE_COUNT > 0 ]]; then
    OUTPUT_TRACE_COUNT=$((OUTPUT_FILE_COUNT/3))
    echo -e "OUTPUT TRACE / total: \t$OUTPUT_TRACE_COUNT"
    echo "----------------"
    INV_VIO_COUNT=$(ls *.txt| grep '-' | wc -l)
    echo -e "Invariant violation / total: \t$INV_VIO_COUNT"
    if [[ $INV_VIO_COUNT > 0 ]]; then
        echo "---> AfterActionInvariant"
        echo -e "Invariant violation / leadership: \t$(ls *.txt | grep 'leadership' | wc -l)"
        echo -e "Invariant violation / integrity: \t$(ls *.txt | grep 'integrity' | wc -l)"
        echo -e "Invariant violation / agreement: \t$(ls *.txt | grep 'agreement' | wc -l)"
        echo -e "Invariant violation / totalOrder: \t$(ls *.txt | grep 'totalOrder' | wc -l)"
        echo -e "Invariant violation / primaryOrder: \t$(ls *.txt | grep 'localPrimaryOrder' | wc -l)"
        echo -e "Invariant violation / localPrimaryOrder: \t$(ls *.txt | grep 'localPrimaryOrder' | wc -l)"
        echo -e "Invariant violation / globalPrimaryOrder: \t$(ls *.txt | grep 'globalPrimaryOrder' | wc -l)"
        echo -e "Invariant violation / primaryIntegrity: \t$(ls *.txt | grep 'primaryIntegrity' | wc -l)"
        echo -e "Invariant violation / commitCompleteness: \t$(ls *.txt | grep 'commitCompleteness' | wc -l)"
        echo -e "Invariant violation / historyConsistency: \t$(ls *.txt | grep 'historyConsistency' | wc -l)"
        echo -e "Invariant violation / processConsistency: \t$(ls *.txt | grep 'processConsistency' | wc -l)"
        echo -e "Invariant violation / leaderLogCompleteness: \t$(ls *.txt | grep 'leaderLogCompleteness' | wc -l)"
        echo -e "Invariant violation / committedLogDurability: \t$(ls *.txt | grep 'committedLogDurability' | wc -l)"
        echo -e "Invariant violation / monotonicRead: \t$(ls *.txt | grep 'monotonicRead' | wc -l)"
        echo "---> DuringActionInvariant"
        echo -e "Invariant violation / stateConsistent: \t$(ls *.txt | grep 'stateConsistent' | wc -l)"
        echo -e "Invariant violation / proposalConsistent: \t$(ls *.txt | grep 'proposalConsistent' | wc -l)"
        echo -e "Invariant violation / commitConsistent: \t$(ls *.txt | grep 'commitConsistent' | wc -l)"
        echo -e "Invariant violation / ackConsistent: \t$(ls *.txt | grep 'ackConsistent' | wc -l)"
    fi 
fi 


