# -*- coding: utf-8
# prepare.py
import argparse
import datetime as dt
import os
import shutil
import random

import pytz

from context import initialize, INPUT_PATH
from schema import MY_SCHEMA, PARTITION_FIELDS


DATE = dt.datetime(2017, 9, 17)


def generate_row():
    """Create page view event."""
    # tuple fields:
    # customer, url, referrer, session_id, ts, year, month, day, hour
    minute = random.randint(0, 59)
    hour = random.randint(0, 23)
    article_number = random.randint(1, 20)
    referrer = random.choice(['http://google.com/', 'http://bing.com/', 'http://facebook.com/'])
    session_id = random.choice(['xxx', 'yyy'])
    return (
        'a.com',
        'http://a.com/articles/{}'.format(article_number),
        referrer,
        session_id,
        DATE.replace(hour=hour, minute=minute, tzinfo=pytz.UTC),
        '{:04}'.format(DATE.year),
        '{:02}'.format(DATE.month),
        '{:02}'.format(DATE.day),
        '{:02}'.format(hour)
    )


def get_partitions(count):
    if count <= 1000:
        return 1
    elif count <= 1000000:
        return 4
    else:
        return count / 1000000


def generate_rows(sc, count):
    """Generate data."""
    random.seed(count)
    partitions = get_partitions(count)
    partition_size = count / partitions
    print('Generating {} partition(s) with {} records each...'.format(partitions, partition_size))
    data = (sc.parallelize([], partitions)
              .mapPartitions(lambda i: (generate_row() for _ in xrange(partition_size))))
    return data


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100)
    args = parser.parse_args()

    sc, sqlContext = initialize()

    # mock some data
    started = dt.datetime.now()
    print('Generating data...')
    data = generate_rows(sc, args.count)
    df = sqlContext.createDataFrame(data, MY_SCHEMA)

    print('Generated {} records in {}.'.format(args.count, dt.datetime.now() - started))

    write_path = INPUT_PATH.format(event_count=args.count)
    # cleanup before writing
    if os.path.exists(write_path):
        shutil.rmtree(write_path)

    # write parquet
    started = dt.datetime.now()
    print('Writing {} records with {} partitions...'.format(args.count, df.rdd.getNumPartitions()))
    (df.write
       .parquet(write_path, partitionBy=PARTITION_FIELDS, compression='gzip'))
    print('Wrote {} records in {}.'.format(args.count, dt.datetime.now() - started))
