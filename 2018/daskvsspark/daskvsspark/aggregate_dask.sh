#!/usr/bin/env bash

if [ ! -z $1 ]
then
    ADDRESS=$1
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

python aggregate_dask.py --count $COUNT --nfiles $NFILES --scheduler $SCHEDULER --address $ADDRESS
