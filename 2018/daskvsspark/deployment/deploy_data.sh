#!/usr/bin/env bash

# Stop at any error, show all commands
set -ex

S3_PATH="s3://parsely-public/jbennet/daskvsspark/events/"

# copy fake data to s3
aws s3 sync ../daskvsspark/events/ ${S3_PATH}
