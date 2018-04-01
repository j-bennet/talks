# -*- coding: utf-8
# aggregate_dask.py
import argparse
import datetime as dt
import os
import shutil

import dask.dataframe as dd
import simplejson as json

import dask
from dask.distributed import Client

from common import *

INPUT_MASK = './events/{event_count}-{nfiles}/year=*/month=*/day=*/hour=*/*/part*.parquet'
OUTPUT_MASK = './aggs_dask/{event_count}-{nfiles}/*.json'


def read_data(read_path):
    """Reads the original Parquet data.
    :returns: DataFrame
    """
    df = dd.read_parquet(read_path).drop('hour', axis=1)
    return df


def counter_chunk(ser):
    sdf = ser.value_counts().to_frame('counts').reset_index()
    sdf = sdf.set_index(sdf.referrer)
    res = sdf.counts.to_dict().items()
    return res


def counter_agg(chunks):
    total = Counter()
    for chunk in chunks:
        if not isinstance(chunk[0], tuple):
            chunk = [chunk]
        current = Counter(dict(chunk))
        total = total + current
    return total.items()


def group_data(df):
    """Aggregate the DataFrame and return the grouped DataFrame.

    :param df: DataFrame
    :returns: DataFrame
    """
    # round timestamps down to an hour
    df['ts'] = df['ts'].dt.floor('1H')

    # group on customer, timestamp (rounded) and url
    gb = df.groupby(['customer', 'url', 'ts'])

    counter = dd.Aggregation(
        'counter',
        lambda s: counter_chunk(s),
        lambda s: s.apply(counter_agg),
    )

    count_unique = dd.Aggregation(
        'count_unique',
        lambda s: s.nunique(),
        lambda s: s.nunique()
    )

    ag = gb.agg({
        'session_id': [count_unique, 'count'],
        'referrer': counter}
    )

    ag = ag.reset_index()

    # get rid of multilevel columns
    ag.columns = ['customer', 'url', 'ts', 'referrers', 'visitors', 'page_views']
    ag = ag.repartition(npartitions=df.npartitions)

    return ag


def transform_one(series):
    """Takes a Series object representing a grouped DataFrame row,
    and returns a dict ready to be stored as JSON.

    :returns: pd.Series
    """
    data = series.to_dict()
    if not data:
        return pd.Series([], name='data')
    page_views = data.pop('page_views')
    visitors = data.pop('visitors')
    data.update({
        '_id': format_id(data['customer'], data['url'], data['ts']),
        'ts': data['ts'].strftime('%Y-%m-%dT%H:%M:%S'),
        'metrics': format_metrics(page_views, visitors),
        'referrers': dict(data['referrers'])
    })
    return pd.Series([data], name='data')


def transform_data(ag):
    """Accepts a Dask DataFrame and returns a Dask Bag, where each record is
    a string, and the contents of the string is a JSON representation of the
    document to be written.

    :param ag: DataFrame
    :returns: DataFrame with one column "data" containing a dict.
    """
    tr = ag.apply(transform_one, axis=1, meta={'data': str})
    tr = tr.repartition(npartitions=ag.npartitions)
    return tr


def save_json(tr, path):
    """Write records as json."""
    root_dir = os.path.dirname(path)

    # cleanup before writing
    if os.path.exists(root_dir):
        shutil.rmtree(root_dir)

    # create root directory
    if not os.path.exists(root_dir):
        os.makedirs(root_dir)

    (tr.to_bag()
       .map(lambda t: t[0])
       .map(json.dumps)
       .to_textfiles(path))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--count', type=int, default=100)
    parser.add_argument('--nfiles', type=int, default=24)
    parser.add_argument('--wait', action='store_true', default=False)
    parser.add_argument('--scheduler', choices=['thread', 'process', 'default', 'single'])
    args = parser.parse_args()

    read_path = INPUT_MASK.format(event_count=args.count, nfiles=args.nfiles)
    write_path = OUTPUT_MASK.format(event_count=args.count, nfiles=args.nfiles)

    set_display_options()
    started = dt.datetime.utcnow()
    if args.scheduler != 'default':
        print('Scheduler: {}.'.format(args.scheduler))
        getters = {'process': dask.multiprocessing.get,
                   'thread': dask.threaded.get,
                   'single': dask.get}
        dask.set_options(get=getters[args.scheduler])

    try:
        client = Client(processes=False)
        df = read_data(read_path)
        aggregated = group_data(df)
        prepared = transform_data(aggregated)
        save_json(prepared, write_path)
        elapsed = dt.datetime.utcnow() - started
        parts_per_hour = args.nfiles / 24
        print('{:,} records, {} files ({} per hour): done in {}.'.format(
            args.count, args.nfiles, parts_per_hour, elapsed))
        if args.wait:
            raw_input('Press any key')
    except:
        elapsed = dt.datetime.utcnow() - started
        print('Failed in {}.'.format(elapsed))
        raise
