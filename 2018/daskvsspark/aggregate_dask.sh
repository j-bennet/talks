#!/usr/bin/env bash

if [ ! -z $1 ]
then
    COUNT=$1
else
    COUNT=100
fi

if [ ! -z $2 ]
then
    SCHEDULER=$2
else
    SCHEDULER="default"
fi

python aggregate_dask.py --count $COUNT --scheduler $SCHEDULER
