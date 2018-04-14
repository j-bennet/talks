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

if [ ! -z $3 ]
then
    SCHEDULER=$3
else
    SCHEDULER="default"
fi

build_egg &> /dev/null

latest_egg=$(ls -t /home/hadoop/reqs/daskvsspark-*.egg | head -n 1)

cd /home/hadoop/daskvsspark/daskvsspark

PYTHONPATH=$latest_egg python3 aggregate_dask.py \
    --input "s3://parsely-public/jbennet/daskvsspark/events" \
    --output "s3://parsely-public/jbennet/daskvsspark/aggs_dask" \
    --count $COUNT \
    --nfiles $NFILES \
    --scheduler $SCHEDULER \
    --verbose \
    --yarn
