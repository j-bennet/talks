# -*- coding: utf-8
import pandas as pd
import dask.dataframe as dd
import fastparquet as fp
import glob
from pprint import pprint
from dask import delayed


pd.set_option('display.expand_frame_repr', False)


def transform_one(series):
    """Takes a Series object representing a grouped dataframe row,
    and returns a string (serialized JSON).

    :return: dict
    """
    data = series.to_dict()
    if not data:
        return pd.Series([], name='data')
    (customer, url, ts, _) = data.pop('index')
    page_views = data.pop('views')
    visitors = data.pop('visitors')
    data.update({
        'customer': customer,
        'url': url,
        'ts': ts.strftime('%Y-%m-%dT%H:%M:%S'),
        'metrics': {'views': page_views, 'visitors': visitors}
    })
    return pd.Series([data], name='data')


if __name__ == '__main__':
    file_names = glob.glob('./events/*/*/*/*/*/part*.parquet')
    pf = fp.ParquetFile(file_names, root='./events')
    pf.cats = {'customer': pf.cats['customer']}
    dfs = (delayed(pf.read_row_group_file)(rg, pf.columns, pf.cats) for rg in pf.row_groups)
    df = dd.from_delayed(dfs)

    # round datetimes down to an hour
    df['ts'] = df['ts'].dt.floor('1H')

    # group on customer, timestamp (rounded) and url
    gb = df.groupby(['customer', 'url', 'ts'])

    df = gb.apply(lambda d: pd.DataFrame({
            'views': len(d),
            'visitors': d.session_id.count(),
            'referrers': [d.referrer.tolist()]}),
        meta={'views': int, 'visitors': int, 'referrers': int})

    # I want index to be a part of dataframe, so it is passed
    # to the next .apply call.
    df['index'] = df.index

    df = df.apply(transform_one, axis=1, meta={'data': str}).to_bag()

    pprint(df.compute())
