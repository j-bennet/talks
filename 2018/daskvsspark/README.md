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

you can run any Spark script as simply as:

```
python xxx.py
```

Generate input data
-------------------

A script is included to mock some input data. It writes Parquet to `./events.py`.
To run it with Spark:

```
python prepare.py
```

Aggregate with Spark
---------------------

Run this:

```
TZ=UTC python aggregate_spark.py
```

This will read the data from `./events` and write the aggregates as JSON
to `./aggs_spark/`.

Aggregate with Dask
-------------------

Run this:

```
python aggregate_dask.py
```

This will read the data from `./events` and write the aggregates as JSON
to `./aggs_dask/`.

Inspect the data
----------------

A script is included to pretty-print generated json records. For example,
this:

```
python show.py ./aggs_dask 3
```

will pretty-print 3 json records from `./aggs_dask` directory.