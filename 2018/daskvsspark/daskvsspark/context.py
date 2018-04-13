# -*- coding: utf-8
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SQLContext

# template path. Event_count will be replaced by a number.
PATH_TEMPLATE = '{root}/{event_count}-{nfiles}'
INPUT_ROOT = './events'
OUTPUT_ROOT = "./aggs_spark"


def initialize(target_partitions=None):
    """Returns SparkContext and SQLContext."""
    conf = SparkConf()
    extra_settings = {
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        'spark.executor.extraJavaOptions': '-XX:+UseG1GC'
    }
    if target_partitions:
        extra_settings['spark.default.parallelism'] = target_partitions

    conf.setAll(extra_settings.items())
    environment = {'PYTHON_EGG_CACHE': '/tmp/python-eggs'}
    sc = SparkContext(conf=conf, environment=environment)

    sqlContext = SQLContext(sc)
    if target_partitions:
        sqlContext.setConf('spark.sql.shuffle.partitions', target_partitions)

    jvm_logger = sc._jvm.org.apache.log4j
    jvm_logger.LogManager.getLogger("org").setLevel(jvm_logger.Level.ERROR)
    jvm_logger.LogManager.getLogger("akka").setLevel(jvm_logger.Level.ERROR)
    return sc, sqlContext
