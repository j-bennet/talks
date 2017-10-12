# -*- coding: utf-8
# aggregate_dask.py
import glob
import itertools as it
import os
import shutil

import dask.dataframe as dd
import fastparquet as fp
import pandas as pd
import simplejson as json
from dask import delayed
from dask.distributed import Client

from common import *

INPUT_MASK = './events/*/*/*/*/*/part*.parquet'
OUTPUT_MASK = './aggs_dask/*.json'

pd.set_option('display.expand_frame_repr', False)


def read_data():
    """Reads the original Parquet data.
    :returns: DataFrame
    """
    file_names = glob.glob(INPUT_MASK)
    pf = fp.ParquetFile(file_names, root='./events')
    pf.cats = {'customer': pf.cats['customer']}
    dfs = (delayed(pf.read_row_group_file)(rg, pf.columns, pf.cats) for rg in pf.row_groups)
    df = dd.from_delayed(dfs)
    return df


def group_data(df):
    """Aggregate the DataFrame and return the grouped DataFrame.

    :param df: DataFrame
    :returns: DataFrame
    """
    # round timestamps down to an hour
    df['ts'] = df['ts'].dt.floor('1H')

    # group on customer, timestamp (rounded) and url
    gb = df.groupby(['customer', 'url', 'ts'])

    collect_list = dd.Aggregation(
        'collect_list',
        lambda s: s.apply(list),
        lambda s: s.apply(lambda chunks: list(it.chain.from_iterable(chunks))),
    )

    get_length = dd.Aggregation(
        'get_length',
        lambda s: s.apply(len),
        lambda s: s.apply(len),
    )

    ag = gb.agg({
        'session_id': {'visitors': 'count', 'page_views': get_length},
        'referrer': {'referrers': collect_list}}
    )

    ag = ag.reset_index()
    return ag


def transform_one(series):
    """Takes a Series object representing a grouped DataFrame row,
    and returns a dict ready to be stored as JSON.

    :returns: pd.Series
    """
    empty = pd.Series([], name='data')
    data = series.to_dict()
    if not data:
        return empty
    page_views = data.pop(('session_id', 'page_views'))
    referrers = data.pop(('referrer', 'referrers'))
    visitors = data.pop(('session_id', 'visitors'))
    customer = data.pop(('customer', ''))
    url = data.pop(('url', ''))
    ts = data.pop(('ts', ''))
    data.update({
        '_id': format_id(customer, url, ts),
        'customer': customer,
        'url': url,
        'ts': ts.strftime('%Y-%m-%dT%H:%M:%S'),
        'metrics': format_metrics(visitors, page_views),
        'referrers': format_referrers(referrers)
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
    client = Client()
    df = read_data()
    aggregated = group_data(df)
    prepared = transform_data(aggregated)
    save_json(prepared, OUTPUT_MASK)
