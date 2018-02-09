# -*- coding: utf-8
import pandas as pd
import dask.dataframe as dd
from pprint import pprint


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
    pdf = pd.DataFrame.from_records([
        ("http://a.com/articles/1", "http://google.com/", "xxx", "2017-09-15 00:10:00", "a.com"),
        ("http://a.com/articles/2", "http://bing.com/", "yyy", "2017-09-15 00:20:00", "a.com"),
        ("http://a.com/articles/2", "http://facebook.com/", "yyy", "2017-09-15 00:30:00", "a.com"),
        ("http://a.com/articles/1", "http://google.com/", "xxx", "2017-09-15 01:10:00", "a.com"),
        ("http://a.com/articles/2", "http://bing.com/", "yyy", "2017-09-15 01:20:00", "a.com")
    ],
    columns=['url', 'referrer', 'session_id', 'ts', 'customer'])

    pdf['ts'] = pd.to_datetime(pdf['ts'])

    df = dd.from_pandas(pdf, 2)

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
