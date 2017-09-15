# -*- coding: utf-8
# aggregate.py
import os
import shutil
from collections import Counter

from pyspark.sql.types import StringType, IntegerType, MapType
from pyspark.sql.functions import udf, lit

from context import initialize, INPUT_PATH, OUTPUT_PATH

if os.environ.get('TZ', '') != 'UTC':
    raise Exception('Please set TZ=UTC to run this.')


def aggregate(df):
    """Group data by customer, url, and 1 hour bucket."""
    df.createOrReplaceTempView("df")
    agg = sqlContext.sql("""
    select 
        customer,
        url,
        window(ts, '1 hour').start as ts,
        count(*) as page_views,
        count(distinct(session_id)) as visitors,
        collect_list(referrer) as referrers
    from df
    group by
        customer,
        url,
        window(ts, '1 hour').start
    """)
    return agg


def make_id(customer, url, ts):
    """Create a unique id for the aggregated record."""
    return "{}|{}|{:%Y-%m-%dT%H:%M:%S}".format(url, customer, ts)


def make_metrics(visitors, page_views):
    """Create a dict of metrics."""
    return {
        "page_views": page_views,
        "visitors": visitors
    }


def make_referrers(referrers):
    """Create a dict of referrer counts."""
    counter = Counter(referrers)
    return dict(counter)


def transform(df):
    """Transform data into complex json representation."""
    udf_id = udf(
        lambda *args: make_id(*args),
        StringType())

    udf_metrics = udf(
        lambda *args: make_metrics(*args),
        MapType(StringType(), IntegerType()))

    udf_refs = udf(
        lambda *args: make_referrers(*args),
        MapType(StringType(), IntegerType()))

    df = (df.withColumn('_id', udf_id('customer', 'url', 'ts'))
            .withColumn('_index', lit('events'))
            .withColumn('freq', lit('1hour'))
            .withColumn('metrics', udf_metrics('visitors', 'page_views'))
            .withColumn('referrers', udf_refs('referrers'))
            .drop('page_views')
            .drop('visitors')
            .drop('referrers'))
    return df


def save_json(df, path):
    """Write aggregate rows as json."""
    # cleanup before writing
    if os.path.exists(path):
        shutil.rmtree(path)
    df.coalesce(1).write.json(path)


if __name__ == '__main__':
    sc, sqlContext = initialize()
    df = sqlContext.read.parquet(INPUT_PATH)
    agg = aggregate(df)
    prep = transform(agg)
    save_json(prep, OUTPUT_PATH)
