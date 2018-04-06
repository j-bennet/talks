#!/usr/bin/env bash

if [ ! -z $1 ]
then
    COUNT=$1
else
    COUNT=100
fi

if [ ! -z $2 ]
then
    NFILES=$2
else
    NFILES=24
fi

TZ=UTC PYSPARK_DRIVER_PYTHON=`which python` PYSPARK_PYTHON=`which python` \
    $SPARK_HOME/bin/spark-submit \
    --master "local[4]" \
    --deploy-mode client \
    --driver-memory 6g \
    --executor-memory 2g \
    --num-executors 4 \
    --driver-class-path ../scala/target/scala-2.11/daskvsspark-udafs_2.11-0.0.1.jar \
    --driver-java-options "-Droot.logger=ERROR,console" \
    aggregate_spark.py --count $COUNT --nfiles $NFILES
