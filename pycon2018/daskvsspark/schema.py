# -*- coding: utf-8
# schema.py
from pyspark.sql.types import *


PARTITION_FIELDS = ['year', 'month', 'day', 'hour', "customer"]

MY_SCHEMA = StructType([
    StructField('customer', StringType(), True),
    StructField('url', StringType(), True),
    StructField('referrer', StringType(), True),
    StructField('session_id', StringType(), True),
    StructField('ts', TimestampType(), True),
    # partitioning keys
    StructField('year', StringType(), nullable=False),
    StructField('month', StringType(), nullable=False),
    StructField('day', StringType(), nullable=False),
    StructField('hour', StringType(), nullable=False),
])
