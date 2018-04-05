# Scala UDAFs for Pyspark

## Huh?

Spark has lots and lots of wonderful aggregations! But sometimes, standard
aggregations (`min`, `max`, `avg` etc.) are not enough. For example, what if
I have a table like this:

|          url          | referrer          |
|:---------------------:|:-----------------:|
| http://a.com/article1 | http://google.com |
| http://a.com/article2 | http://google.com |
| http://a.com/article2 | http://yahoo.com  |


and I want to group things by `url`, count `referrers` in each group, and put
those counts in a dict:

```sql
select url, count_values(referrer) as referrers
from df
group by url
```

like this:

|          url          |                    referrers                    |
|:---------------------:|:-----------------------------------------------:|
| http://a.com/article1 | {"http://google.com": 1}                        |
| http://a.com/article2 | {"http://google.com": 1, "http://yahoo.com": 2} |

There's one little problem. PySpark doesn't support UDAFs written in Python:

https://issues.apache.org/jira/browse/SPARK-10915

## So I'm screwed?

Not quite. It is possible to write a UDAF in Scala and call it from Python.

## You lost me at Scala.

It's not so bad. Besides, I already wrote it. See the code in "udafs.scala".

## How do I build this?

```
$ cd scala
$ sbt compile
$ sbt package
```

If you have problems finding dependencies when you compile, try deleting the `~/.ivy2/` cache.

## How do I use this?

Note: this will only work with Spark 2.1.0 and up.

* Start with the jar on the classpath.
* Get an instance of class using `sc._jvm` object.
* Register it as a UDF to use in Spark SQL.
* Or wrap it in a Python function to use in aggregations.

## Show me.

```
--- daskvsspark/scala $ ipyspark --driver-class-path target/scala-2.11/daskvsspark-udafs_2.11-0.0.1.jar
Using Python version 3.6.5 (default, Apr  2 2018 14:34:27)
SparkSession available as 'spark'.

In [1]: df = sqlContext.createDataFrame([('url1', 'ref1'), ('url2', 'ref1'), ('url2', 'ref2')], ['url', 'referrer'])

In [2]: agg_counter = sc._jvm.com.jbennet.daskvsspark.udafs.AggregateCounter()

In [4]: sqlContext.sparkSession._jsparkSession.udf().register('count_values', agg_counter)
Out[4]: JavaObject id=o45

In [5]: df.createOrReplaceTempView('df')

In [6]: sqlContext.sql('select url, count_values(referrer) as referrers from df group by url').show()
+----+--------------------+
| url|           referrers|
+----+--------------------+
|url1|         [ref1 -> 1]|
|url2|[ref1 -> 1, ref2 ...|
+----+--------------------+
```

or:

```
In [7]: from pyspark.sql.column import Column, _to_java_column, _to_seq

In [11]: def count_values(col):
    ...:     counter = sc._jvm.com.jbennet.daskvsspark.udafs.AggregateCounter().apply
    ...:     return Column(counter(_to_seq(sc, [col], _to_java_column)))
    ...:
    ...:

In [12]: df.groupBy("url").agg(count_values("referrer").alias("referrer")).show()
+----+--------------------+
| url|            referrer|
+----+--------------------+
|url1|         [ref1 -> 1]|
|url2|[ref1 -> 1, ref2 ...|
+----+--------------------+
```

You're welcome.