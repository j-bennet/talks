#!/usr/bin/env bash

# Stop at any error
set -e

if [ ! -z $1 ]
then
    ADDRESS=$1
else
    echo "Usage: $0 <SCHEDULER ADDRESS>"
    exit 1
fi

if [ ! -z $2 ]
then
    COUNT=$2
else
    COUNT=100
fi

if [ ! -z $3 ]
then
    NFILES=$3
else
    NFILES=24
fi


if [ ! -z $4 ]
then
    SCHEDULER=$4
else
    SCHEDULER="default"
fi

cd /home/hadoop/daskvsspark/daskvsspark

/home/hadoop/conda/envs/dvss/bin/python aggregate_dask.py \
    --input "s3://parsely-public/jbennet/daskvsspark/events" \
    --output "s3://parsely-public/jbennet/daskvsspark/aggs_dask" \
    --count $COUNT \
    --nfiles $NFILES \
    --scheduler $SCHEDULER \
    --verbose
