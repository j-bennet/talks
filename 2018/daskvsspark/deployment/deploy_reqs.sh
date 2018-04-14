#!/usr/bin/env bash

S3_PATH="s3://parsely-public/jbennet/daskvsspark/reqs/"

# copy bootstrap script to s3
aws s3 cp ../deployment/bootstrap.sh ${S3_PATH}

# copy log conf
aws s3 cp ../deployment/log4j.properties ${S3_PATH}

# copy reqs to s3
aws s3 cp ../requirements.txt ${S3_PATH}
aws s3 cp ../requirements-dask.txt ${S3_PATH}

# copy jars to s3
aws s3 cp ../scala/target/scala-2.11/daskvsspark-udafs_2.11-0.0.1.jar ${S3_PATH}
