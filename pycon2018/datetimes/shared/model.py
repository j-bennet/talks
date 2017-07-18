# -*- coding: utf-8
# schema.py
from pyspark.sql.types import *

MY_SCHEMA = StructType([
    StructField('name', StringType(), True),
    StructField('ts', TimestampType(), True)
])
