What is this?
=============

An example of data aggregation in Spark and in Dask.

How do I use it?
================

To run this locally, you need an Apache Spark distribution
(let's say it's in `$HOME/bin/`). Then, after setting some
environment variables:

```
  export SPARK_HOME="$HOME/bin/spark-2.1.1-bin-hadoop2.7"
  export PYTHONPATH="$SPARK_HOME/python/lib/pyspark.zip:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH"
```

you can run a Spark script as simply as:

```
python main.py
```

The above is good enough for testing. In real life, you'd use ``spark-submit``:

```
PYSPARK_DRIVER_PYTHON=`which python` PYSPARK_PYTHON=`which python` \
    spark-submit \
    --master "local[4]" \
    --deploy-mode client \
    main.py
```

Generate input data
-------------------

A script is included to mock some input data. It writes Parquet to `./events/` directory.
To run it with Spark:

```
prepare.sh
```

By default, it'll generate 100 input records and assume 100k records per partition (one parquet
file). You can provide a different number:

```
prepare.sh [total-records] [records-per-partition]
```

The data is partitioned on disk by year, month, day, customer and hour. The script generates 1 day
of data. This means that at least 24 files (partitions) will be created, because we can't create
less than one partition per hour. The script will write parquet to
``./events/[number-of-records]-[number-of-partitions]``

Make sure that the spark-submit settings in ``prepare.sh`` (``driver-memory``,
``executor-memory``, ``num-executors``) will work for you.

Aggregate with Spark
---------------------

Run this:

```
aggregate_spark.sh [number-of-records] [number-of-partitions]
```

This will read the data from ``./events`` and write the aggregates as JSON
to ``./aggs_spark/[number-of-records]-[number-of-partitions]``.

Aggregate with Dask
-------------------

Run this:

```
python aggregate_dask.py [number-of-records] [number-of-partitions]
```

This will read the data from `./events` and write the aggregates as JSON
to ``./aggs_dask/[number-of-records]-[number-of-partitions]``.

Inspect the data
----------------

A script is included to pretty-print generated json records. For example,
this:

```
python show.py ./aggs_dask/100-24 3
```

will pretty-print 3 json records from ``./aggs_dask/100-24`` directory.
