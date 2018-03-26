#!/usr/bin/env bash
if [ ! -z $1 ]
then
    COUNT=$1
else
    COUNT=100
fi

if [ ! -z $2 ]
then
    CHUNKSIZE=$2
else
    CHUNKSIZE=100000
fi

TZ=UTC PYSPARK_DRIVER_PYTHON=`which python` PYSPARK_PYTHON=`which python` \
    $SPARK_HOME/bin/spark-submit \
    --master "local[4]" \
    --deploy-mode client \
    --driver-memory 6g \
    --executor-memory 2g \
    --num-executors 4 \
    prepare.py --count $COUNT --chunk-size $CHUNKSIZE
