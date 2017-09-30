# -*- coding: utf-8
# aggregate_dask.py
import glob
import os
import shutil

import dask.dataframe as dd
import fastparquet as fp
import pandas as pd
import simplejson as json
from dask import delayed
from dask.dataframe import utils

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
    :return: DataFrame
    """
    agg_info = {
        'page_views': 'i4',
        'visitors': 'i4',
        'referrers': 'object',
    }
    agg_meta = utils.make_meta(agg_info)

    # round timestamps down to an hour
    df['ts'] = df['ts'].map(lambda x: x.replace(minute=0, second=0, microsecond=0))

    # group on customer, timestamp (rounded) and url
    gb = df.groupby(['customer', 'url', 'ts'])

    ag = gb.apply(lambda d: pd.DataFrame({
        'page_views': len(d),
        'visitors': d.session_id.count(),
        'referrers': [d.referrer.tolist()]
    }), meta=agg_meta)

    return ag


def transform_one(series):
    """Takes a Series object representing a grouped dataframe row,
    and returns a string (serialized JSON).

    :return: dict
    """
    data = series.to_dict()
    (customer, url, ts, _) = data.pop('index')
    page_views = data.pop('page_views')
    referrers = data.pop('referrers')
    visitors = data.pop('visitors')
    data.update({
        '_id': format_id(customer, url, ts),
        'customer': customer,
        'url': url,
        'ts': ts.strftime('%Y-%m-%dT%H:%M:%S'),
        'metrics': format_metrics(visitors, page_views),
        'referrers': format_referrers(referrers)
    })
    serialized = json.dumps(data)
    return pd.DataFrame({'data': [serialized]})


def transform_data(df):
    """Accepts a Dask DataFrame and returns a Dask Bag, where each record is
    a string, and the contents of the string is a JSON representation of the
    document to be written.

    :param df: DataFrame
    :return: Bag
    """
    # I want index to be a part of dataframe, so it is passed
    # to the next .apply call.
    df['index'] = df.index
    return df.apply(transform_one, axis=1, meta={'data': str}).to_bag()


def save_json(df, path):
    """Write records as json."""
    root_dir = os.path.dirname(path)
    # cleanup before writing
    if os.path.exists(root_dir):
        shutil.rmtree(root_dir)
    # create root directory
    if not os.path.exists(root_dir):
        os.makedirs(root_dir)
    df.repartition(npartitions=1).to_textfiles(path)


if __name__ == '__main__':
    df = read_data()
    aggregated = group_data(df)
    prepared = transform_data(aggregated)
    save_json(prepared, OUTPUT_MASK)
