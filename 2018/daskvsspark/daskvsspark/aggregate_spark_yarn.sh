#!/usr/bin/env bash

# Stop at any error
set -e

build_egg() {
    cd /home/hadoop/daskvsspark/
    python3 setup.py bdist_egg
    cp ./dist/*.egg /home/hadoop/reqs/
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

build_egg &> /dev/null

latest_egg=$(ls -t /home/hadoop/reqs/daskvsspark-*.egg | head -n 1)

cd /home/hadoop/daskvsspark/daskvsspark/

TZ=UTC PYSPARK_DRIVER_PYTHON=python3 PYSPARK_PYTHON=python3 \
    spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 8g \
    --executor-memory 3g \
    --num-executors 6 \
    --executor-cores 4 \
    --conf "spark.yarn.executor.memoryOverhead=2g" \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///home/hadoop/reqs/log4j.properties" \
    --py-files ${latest_egg} \
    --jars /home/hadoop/reqs/daskvsspark-udafs_2.11-0.0.1.jar \
    aggregate_spark.py \
    --input "s3://parsely-public/jbennet/daskvsspark/events" \
    --output "s3://parsely-public/jbennet/daskvsspark/aggs_spark" \
    --count $COUNT \
    --nfiles $NFILES
