# -*- coding: utf-8
# prepare.py
import argparse
import datetime as dt
import math
import itertools
import os
import random
import sys

import pytz

from daskvsspark.context import initialize, INPUT_PATH
from daskvsspark.schema import MY_SCHEMA, PARTITION_FIELDS


DATE = dt.datetime(2017, 9, 17)


def generate_row(total_articles, session_ids):
    """Create page view event."""
    # tuple fields:
    # customer, url, referrer, session_id, ts, year, month, day, hour
    minute = random.randint(0, 59)
    hour = random.randint(0, 23)
    article_number = random.randint(1, total_articles)
    referrer = random.choice(['http://google.com/', 'http://bing.com/', 'http://facebook.com/'])
    session_id = random.choice(session_ids)
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


def nfiles(records, records_per_file):
    parts_per_hour = max(1, records / records_per_file / 24)
    total_files = parts_per_hour * 24
    return parts_per_hour, total_files


def generate_rows(sc, records, records_per_file):
    """Generate data."""
    random.seed(records)
    parts_per_hour, total_files = nfiles(records, records_per_file)
    part_size = records / parts_per_hour
    actual_records_per_file = records / total_files
    print('Generating {} files(s) ({:,} per hour) with {:,} ({:,} actual) records each...'.format(
        total_files,
        parts_per_hour,
        records_per_file,
        actual_records_per_file))

    total_articles = math.ceil(math.pow(records, 1.0/3))
    session_ids = [''.join(t) for t in list(itertools.permutations(list('abcdefg'), 3))]
    data = (sc.parallelize([], parts_per_hour)
              .mapPartitions(lambda rs: (generate_row(total_articles, session_ids)
                                         for _ in range(part_size))))
    return data


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--chunk-size", type=int, default=100000)
    myargs = parser.parse_args()

    parts_per_hour, total_files = nfiles(myargs.count, myargs.chunk_size)
    write_path = INPUT_PATH.format(event_count=myargs.count, nfiles=total_files)

    # cleanup before writing
    if os.path.exists(write_path):
        print('Path exists: {}. Exiting.'.format(write_path))
        sys.exit(0)

    sc, sqlContext = initialize()

    # mock some data
    started = dt.datetime.now()
    print('Generating data...')
    data = generate_rows(sc, myargs.count, myargs.chunk_size)
    df = sqlContext.createDataFrame(data, MY_SCHEMA)

    print('Generated {:,} records with {:,} files per hour in {}.'.format(
        myargs.count, parts_per_hour, dt.datetime.now() - started))

    # write parquet
    started = dt.datetime.now()
    print('Writing {:,} records...'.format(myargs.count))
    (df.write
       .parquet(write_path, partitionBy=PARTITION_FIELDS, compression='gzip'))
    print('Wrote {:,} records in {}.'.format(myargs.count, dt.datetime.now() - started))
