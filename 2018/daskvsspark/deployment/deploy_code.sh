#!/usr/bin/env bash

cd ..

rsync -azvr \
    --include "README.md" \
    --include "requirements.txt" \
    --include "setup.py" \
    --include "/daskvsspark/" \
    --include "/daskvsspark/*.py" \
    --include "/daskvsspark/aggregate_*_yarn.sh" \
    --exclude "*" \
    ./ hadoop@dvss:/home/hadoop/daskvsspark/
