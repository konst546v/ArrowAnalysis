#!/bin/bash
# script for generating measurements for typical cache sizes and a constant amount of measurements 
# size in KB, start
SIZE=16
# measurements each run
MEASURES=50
echo "start measuring"
# from 16KB to 8MB
while [ $SIZE -le 8192 ]
do
    ./build/arrow_eval $SIZE $MEASURES
    if [ $? -eq 1 ]; then 
        echo "eval failed"
        exit 1
    fi
    Rscript ./plot.r ./build/measurements_${SIZE}_${MEASURES}.json $SIZE $MEASURES
    if [ $? -eq 1 ]; then 
        echo "plot failed"
        exit 1
    fi
    echo "measured with size of ${SIZE}KB and $MEASURES runs"
    SIZE=$((SIZE*2))
done
