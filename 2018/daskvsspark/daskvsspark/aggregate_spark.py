# -*- coding: utf-8
# aggregate_spark.py
import argparse
import os
import datetime as dt

from pyspark.sql.types import StringType, IntegerType, MapType

from daskvsspark.context import initialize, INPUT_ROOT, OUTPUT_ROOT, PATH_TEMPLATE
from daskvsspark.common import *

if os.environ.get('TZ', '') != 'UTC':
    raise Exception('Please set TZ=UTC to run this.')


def load_sql_user_functions(sc, sqlContext):
    """Load our custom UDAFs into a sql context."""
    sqlContext.udf.register('format_id',
                            format_id,
                            StringType())
    sqlContext.udf.register('format_metrics',
                            format_metrics,
                            MapType(StringType(), IntegerType()))

    # custom aggregation function. Needs a jar provided in runner script.
    agg_counter = sc._jvm.com.jbennet.daskvsspark.udafs.AggregateCounter()
    sqlContext.sparkSession._jsparkSession.udf().register('count_values', agg_counter)


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
      referrers
    from (
        select 
            customer,
            url,
            window(ts, '1 hour').start as ts,
            count(*) as page_views,
            count(distinct(session_id)) as visitors,
            count_values(referrer) as referrers
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
    df.write.mode('overwrite').json(path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--nfiles", type=int, default=24)
    parser.add_argument("--wait", action='store_true', default=False)
    parser.add_argument('--input', default=INPUT_ROOT)
    parser.add_argument('--output', default=OUTPUT_ROOT)
    myargs = parser.parse_args()

    read_path = PATH_TEMPLATE.format(root=myargs.input, event_count=myargs.count,
                                     nfiles=myargs.nfiles)
    write_path = PATH_TEMPLATE.format(root=myargs.output, event_count=myargs.count,
                                      nfiles=myargs.nfiles)
    target_partitions = myargs.nfiles

    started = dt.datetime.utcnow()

    sc, sqlContext = initialize(target_partitions=target_partitions)
    load_sql_user_functions(sc, sqlContext)

    df = sqlContext.read.parquet(read_path)
    agg = aggregate(df)
    save_json(agg, write_path)
    elapsed = dt.datetime.utcnow() - started

    parts_per_hour = int(myargs.nfiles / 24)
    print('{:,} records, {} files ({} per hour): done in {}.'.format(
        myargs.count, myargs.nfiles, parts_per_hour, elapsed))
    if myargs.wait:
        input('Press any key')
