# -*- coding: utf-8
# prepare.py

import datetime as dt
import os
import shutil

import pytz

from context import initialize, INPUT_PATH
from schema import MY_SCHEMA, PARTITION_FIELDS


def generate_data(date):
    """Create 24 hours of page view events: 3x per hour = 72 events."""
    # tuple fields:
    # customer, url, referrer, session_id, ts, year, month, day, hour
    for hour in range(24):
        yield (
            'a.com',
            'http://a.com/articles/1',
            'http://google.com/',
            'xxx',
            date.replace(hour=hour, minute=15, tzinfo=pytz.UTC),
            '{:04}'.format(date.year),
            '{:02}'.format(date.month),
            '{:02}'.format(date.day),
            '{:02}'.format(hour)
        )
        yield (
            'a.com',
            'http://a.com/articles/2',
            'http://bing.com/',
            'yyy',
            date.replace(hour=hour, minute=30, tzinfo=pytz.UTC),
            '{:04}'.format(date.year),
            '{:02}'.format(date.month),
            '{:02}'.format(date.day),
            '{:02}'.format(hour)
        )
        yield (
            'a.com',
            'http://a.com/articles/2',
            'http://facebook.com/',
            'yyy',
            date.replace(hour=hour, minute=45, tzinfo=pytz.UTC),
            '{:04}'.format(date.year),
            '{:02}'.format(date.month),
            '{:02}'.format(date.day),
            '{:02}'.format(hour)
        )


if __name__ == '__main__':
    sc, sqlContext = initialize()

    # mock some data
    data = generate_data(dt.datetime(2017, 9, 15))
    rdd = sc.parallelize(data)
    df = sqlContext.createDataFrame(rdd, MY_SCHEMA)

    # cleanup before writing
    if os.path.exists(INPUT_PATH):
        shutil.rmtree(INPUT_PATH)

    # write parquet
    (df.repartition(1)
       .write
       .parquet(INPUT_PATH, partitionBy=PARTITION_FIELDS))
