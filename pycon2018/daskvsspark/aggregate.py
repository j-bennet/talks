# -*- coding: utf-8
# aggregate.py
import os
import shutil
from collections import Counter

from pyspark.sql.types import StringType, IntegerType, MapType

from context import initialize, INPUT_PATH, OUTPUT_PATH

if os.environ.get('TZ', '') != 'UTC':
    raise Exception('Please set TZ=UTC to run this.')


def format_id(customer, url, ts):
    """Create a unique id for the aggregated record."""
    return "{}|{}|{:%Y-%m-%dT%H:%M:%S}".format(url, customer, ts)


def format_metrics(visitors, page_views):
    """Create a dict of metrics."""
    return {
        "page_views": page_views,
        "visitors": visitors
    }


def format_referrers(referrers):
    """Create a dict of referrer counts."""
    counter = Counter(referrers)
    return dict(counter)


def load_sql_user_functions(sqlContext):
    """Load our custom UDAFs into a sql context."""
    sqlContext.udf.register('format_id',
                            format_id,
                            StringType())
    sqlContext.udf.register('format_referrers',
                            format_referrers,
                            MapType(StringType(), IntegerType()))
    sqlContext.udf.register('format_referrers',
                            format_referrers,
                            MapType(StringType(), IntegerType()))


def aggregate(df):
    """Group data by customer, url, and 1 hour bucket."""
    df.createOrReplaceTempView("df")
    agg = sqlContext.sql("""
    select 
      format_id(customer, url, ts) as _id,
      customer,
      url,
      ts,
      format_metrics(page_views, visitors) as metrics,
      format_referrers(referrers) as referrers
    from (
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
    )
    """)
    return agg


def save_json(df, path):
    """Write aggregate rows as json."""
    # cleanup before writing
    if os.path.exists(path):
        shutil.rmtree(path)
    df.coalesce(1).write.json(path)


if __name__ == '__main__':
    sc, sqlContext = initialize()
    load_sql_user_functions(sqlContext)
    df = sqlContext.read.parquet(INPUT_PATH)
    agg = aggregate(df)
    save_json(agg, OUTPUT_PATH)
