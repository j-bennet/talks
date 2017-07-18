# -*- coding: utf-8
# 01_create_dataframe.py
import pytz
import datetime as dt
import os
import shutil

from pprint import pprint

from shared.context import initialize, FILENAME
from shared.model import MY_SCHEMA


if __name__ == '__main__':
    sc, sqlContext = initialize()
    data = [
        ('naive', dt.datetime(2017, 2, 2)),
        ('utc', dt.datetime(2017, 2, 2, tzinfo=pytz.UTC)),
    ]
    rdd = sc.parallelize(data)
    df = sqlContext.createDataFrame(rdd, MY_SCHEMA)
    print("Dataframe before writing:")
    pprint(df.collect())
    # write parquet
    if os.path.exists(FILENAME):
        shutil.rmtree(FILENAME)
    df.write.parquet(FILENAME)
