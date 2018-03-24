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

A script is included to mock some input data. It writes Parquet to `./events.py`.
To run it with Spark:

```
prepare.sh
```

By default, it'll generate 100 input records. You can provide a different number:

```
prepare.sh 10000
```

but make sure that all the spark-submit settings in ``prepare.sh`` (``driver-memory``,
``executor-memory``, ``num-executors``) will work for you.

Aggregate with Spark
---------------------

Run this:

```
aggregate_spark.sh
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