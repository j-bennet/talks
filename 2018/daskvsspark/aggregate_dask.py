# -*- coding: utf-8
# aggregate_dask.py
import glob
import pandas as pd
import dask.dataframe as dd

FILE_MASK = './events/*/*/*/*/*/*.*.parquet'


if __name__ == '__main__':
    file_names = glob.glob(FILE_MASK)

    """
    ipdb> df.columns
    Index([u'url', u'referrer', u'session_id', u'ts', u'customer', u'hour'], dtype='object')
    
    ipdb> df.head(npartitions=24)
                           url              referrer session_id                  ts customer hour
    0  http://a.com/articles/1    http://google.com/        xxx 2017-09-15 00:00:00    a.com    0
    1  http://a.com/articles/2      http://bing.com/        yyy 2017-09-15 00:00:00    a.com    0
    2  http://a.com/articles/2  http://facebook.com/        yyy 2017-09-15 00:00:00    a.com    0
    0  http://a.com/articles/1    http://google.com/        xxx 2017-09-15 01:00:00    a.com    1
    1  http://a.com/articles/2      http://bing.com/        yyy 2017-09-15 01:00:00    a.com    1
    """
    df = dd.read_parquet(file_names, index=False)

    # round imestamps down to an hour
    df['ts'] = df['ts'].map(lambda x: x.replace(minute=0, second=0, microsecond=0))

    # group on timestamp (rounded) and url
    grouped = df.groupby(['ts', 'url'])

    # calculate page views (count rows in each group)
    page_views = grouped.size()

    # collect a list of referrer strings per group
    referrers = grouped['referrer'].apply(list, meta=('referrers', 'f8'))

    # count unique visitors (session ids)
    visitors = grouped['session_id'].count()

    # I want all of these in one dataframe now.
    # An equivalent of:

    """
    select 
        customer,
        url,
        ts,
        count(*) as page_views,
        count(distinct(session_id)) as visitors,
        collect_list(referrer) as referrers
    from df
    group by
        customer,
        url,
        ts
    """

    import ipdb; ipdb.set_trace()
