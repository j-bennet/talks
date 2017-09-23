# -*- coding: utf-8
# aggregate_dask.py
import glob
import copy
import pandas as pd
import dask.dataframe as dd

FILE_MASK = './events/*/*/*/*/*/*.*.parquet'
INPUT_FIELDS = ['url', 'referrer', 'session_id', 'ts', 'customer']

pd.set_option('display.expand_frame_repr', False)


if __name__ == '__main__':
    file_names = glob.glob(FILE_MASK)

    """
    ipdb> df.columns
    Index([u'url', u'referrer', u'session_id', u'ts', u'customer'], dtype='object')
    
    ipdb> df.head(npartitions=24)
                           url              referrer session_id                  ts customer
    0  http://a.com/articles/1    http://google.com/        xxx 2017-09-15 00:00:00    a.com
    1  http://a.com/articles/2      http://bing.com/        yyy 2017-09-15 00:00:00    a.com
    2  http://a.com/articles/2  http://facebook.com/        yyy 2017-09-15 00:00:00    a.com
    0  http://a.com/articles/1    http://google.com/        xxx 2017-09-15 01:00:00    a.com
    1  http://a.com/articles/2      http://bing.com/        yyy 2017-09-15 01:00:00    a.com
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
    def add_dicts(d1, d2):
        ks = set(d1.keys() + d2.keys())
        return {
            k: d1.get(k, 0) + d2.get(k, 0)
            for k in ks
        }

    def add_dict_value(d, k):
        d = copy.copy(d)
        d[k] = d.get(k, 0) + 1
        return d

    def row_key(row):
        url, _, _, ts, customer = row
        return customer, url, ts

    def binop(agg, row):
        url, ref, session_id, ts, customer = row
        _, _, _, pvs, vis, refs = agg
        refs = add_dict_value(refs, ref)
        agg_vis = set(vis)
        agg_vis.add(session_id)
        return customer, url, ts, pvs + 1, agg_vis, refs

    def combine(agg1, agg2):
        _, _, _, pvs1, vis1, refs1 = agg1
        customer, url, ts, pvs2, vis2, refs2 = agg2
        refs = add_dicts(refs1, refs2)
        return customer, url, ts, pvs1 + pvs2, vis1.union(vis2), refs

    initial = (None, None, None, 0, set(), {})

    bg = df[INPUT_FIELDS].to_bag()
    gg = bg.foldby(row_key,
                   binop=binop,
                   initial=initial,
                   combine=combine,
                   combine_initial=initial)

    res = dict(gg)

    import ipdb; ipdb.set_trace()
