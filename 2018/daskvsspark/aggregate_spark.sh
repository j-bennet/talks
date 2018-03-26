#!/usr/bin/env bash

if [ ! -z $1 ]
then
    COUNT=$1
else
    COUNT=100
fi
TZ=UTC PYSPARK_DRIVER_PYTHON=`which python` PYSPARK_PYTHON=`which python` \
    $SPARK_HOME/bin/spark-submit \
    --master "local[4]" \
    --deploy-mode client \
    --driver-memory 6g \
    --executor-memory 2g \
    --num-executors 4 \
    aggregate_spark.py --count $COUNT
