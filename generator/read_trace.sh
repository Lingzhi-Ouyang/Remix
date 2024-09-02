#!/bin/bash

MODEL_DIR=$(ls -dt ../output/model_* | head -1)
TRACE_FILES=$(ls ${MODEL_DIR})
OUTPUT_DIR="${MODEL_DIR}_output"

starttime=`date +'%Y-%m-%d %H:%M:%S'`
start_seconds=`date +%s`
echo "Start: "$starttime
echo "----------------"

counter=0

mkdir -p ${OUTPUT_DIR}

for file in ${TRACE_FILES}
do
  if [[ $file == trace* ]]; then
    counter=$((counter+1))
    if [[ "$counter" -gt 500 ]]; then
      python3 trace_reader.py ${MODEL_DIR}/$file -o ${OUTPUT_DIR}/$file -f true -i 2
      counter=0
    else
      python3 trace_reader.py ${MODEL_DIR}/$file -o ${OUTPUT_DIR}/$file -i 2
    fi
    if [ -f ${OUTPUT_DIR}/${file}*.json ]; then
      cp ${MODEL_DIR}/$file ${OUTPUT_DIR}/
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

cd ${OUTPUT_DIR}
echo -e "Invariant violation / total: \t$(ls *.txt | grep '-' | wc -l)"
echo "----------------"
echo "---> AfterActionInvariant"
echo -e "Invariant violation / processConsistency: \t$(ls *.txt | grep 'processConsistency' | wc -l)"
echo -e "Invariant violation / leaderLogCompleteness: \t$(ls *.txt | grep 'leaderLogCompleteness' | wc -l)"
echo -e "Invariant violation / committedLogDurability: \t$(ls *.txt | grep 'committedLogDurability' | wc -l)"
echo -e "Invariant violation / monotonicRead: \t$(ls *.txt | grep 'monotonicRead' | wc -l)"

echo "---> DuringActionInvariant"
echo -e "Invariant violation / proposalConsistent: \t$(ls *.txt | grep 'proposalConsistent' | wc -l)"
echo -e "Invariant violation / commitConsistent: \t$(ls *.txt | grep 'commitConsistent' | wc -l)"
echo -e "Invariant violation / messageLegal: \t$(ls *.txt | grep 'messageLegal' | wc -l)"



