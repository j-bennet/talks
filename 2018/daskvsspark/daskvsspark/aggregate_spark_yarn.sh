#!/usr/bin/env bash

# Stop at any error, show all commands
set -ex

build_egg() {
    cd /home/hadoop/daskvsspark/
    python3 setup.py bdist_egg
    cp ./dist/*.egg /home/hadoop/regs/
}

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

build_egg

latest_egg=$(ls -t /home/hadoop/reqs/daskvsspark-*.egg | head -n 1)

cd /home/hadoop/daskvsspark/

TZ=UTC PYSPARK_DRIVER_PYTHON=python3 PYSPARK_PYTHON=python3 \
    spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 15g \
    --executor-memory 4g \
    --conf spark.yarn.executor.memoryOverhead=3g \
    --num-executors 4 \
    --py-files ${latest_egg} \
    --jars /home/hadoop/reqs/daskvsspark-udafs_2.11-0.0.1.jar \
    --driver-java-options "-Droot.logger=ERROR,console" \
    --input "s3://parsely-public/jbennet/daskvsspark/events/" \
    --output "s3://parsely-public/jbennet/daskvsspark/aggs_spark/" \
    aggregate_spark.py --count $COUNT --nfiles $NFILES
