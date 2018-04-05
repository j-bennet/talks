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

from daskvsspark.common import *

INPUT_MASK = './events/{event_count}-{nfiles}/year=*/month=*/day=*/hour=*/*/part*.parquet'
OUTPUT_MASK = './aggs_dask/{event_count}-{nfiles}/*.json'


def read_data(read_path):
    """Reads the original Parquet data.
    :returns: DataFrame
    """
    df = dd.read_parquet(read_path).drop('hour', axis=1)
    return df


def counter_chunk(ser):
    """Return all values in series."""
    return ser.values


def counter_agg(chunks):
    """Make a counter of values and return it as dict items."""
    total = Counter()
    for chunk in chunks:
        if isinstance(chunk[0], tuple):
            current = Counter(dict(chunk))
        else:
            current = Counter(chunk)
        total = total + current
    return list(total.items())


def nunique_chunk(ser):
    """Get all unique values in series."""
    return ser.unique()


def nunique_agg(chunks):
    """Return number of unique values in all chunks."""
    total = pd.Series()
    for chunk in chunks:
        current = pd.Series(chunk)
        total = total.append(current)
        total = total.drop_duplicates()
    res = total.nunique()
    return res


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
        lambda s: s.apply(counter_chunk),
        lambda s: s.apply(counter_agg),
    )

    count_unique = dd.Aggregation(
        'count_unique',
        lambda s: s.apply(nunique_chunk),
        lambda s: s.apply(nunique_agg)
    )

    ag = gb.agg({
        'session_id': [count_unique, 'count'],
        'referrer': counter}
    )

    ag = ag.reset_index()

    # get rid of multilevel columns
    ag.columns = ['customer', 'url', 'ts', 'visitors', 'page_views', 'referrers']
    ag = ag.repartition(npartitions=df.npartitions)

    return ag


def transform_one(ser):
    """Takes a Series object representing a grouped DataFrame row,
    and returns a dict ready to be stored as JSON.

    :returns: pd.Series
    """
    data = ser.to_dict()
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
    parser.add_argument('--scheduler', choices=['thread', 'process', 'default', 'single'],
                        default='default')
    parser.add_argument('--verbose', action='store_true', default=False)
    parser.add_argument('--address', help='Scheduler address')
    myargs = parser.parse_args()

    read_path = INPUT_MASK.format(event_count=myargs.count, nfiles=myargs.nfiles)
    write_path = OUTPUT_MASK.format(event_count=myargs.count, nfiles=myargs.nfiles)

    set_display_options()
    started = dt.datetime.utcnow()
    if myargs.scheduler != 'default':
        print('Scheduler: {}.'.format(myargs.scheduler))
        getters = {'process': dask.multiprocessing.get,
                   'thread': dask.threaded.get,
                   'single': dask.get}
        dask.set_options(get=getters[myargs.scheduler])

    try:
        # explicit address is a workaround for "Worker failed to start":
        # scheduler and worker have to be started in console.
        # see https://github.com/dask/distributed/issues/1825
        client = (Client(address=myargs.address, silence_logs=False) if myargs.verbose
                  else Client(address=myargs.address))
        df = read_data(read_path)
        aggregated = group_data(df)
        prepared = transform_data(aggregated)
        save_json(prepared, write_path)
        elapsed = dt.datetime.utcnow() - started
        parts_per_hour = myargs.nfiles / 24
        print('{:,} records, {} files ({} per hour): done in {}.'.format(
            myargs.count, myargs.nfiles, parts_per_hour, elapsed))
        if myargs.wait:
            input('Press any key')
    except:
        elapsed = dt.datetime.utcnow() - started
        print('Failed in {}.'.format(elapsed))
        raise
