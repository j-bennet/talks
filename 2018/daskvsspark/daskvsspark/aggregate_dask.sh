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

if [ ! -z $3 ]
then
    SCHEDULER=$3
else
    SCHEDULER="default"
fi

python aggregate_dask.py --count $COUNT --nfiles $NFILES --scheduler $SCHEDULER
