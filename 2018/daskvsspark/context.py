# -*- coding: utf-8
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SQLContext

# template path. Event_count will be replaced by a number.
INPUT_PATH = "./events/{nfiles}.{event_count}/"
OUTPUT_PATH = "./aggs_spark/{nfiles}.{event_count}/"


def initialize():
    """Returns SparkContext and SQLContext."""
    conf = SparkConf()
    extra_settings = {
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        'spark.executor.extraJavaOptions': '-XX:+UseG1GC'
    }
    conf.setAll(extra_settings.items())
    environment = {'PYTHON_EGG_CACHE': '/tmp/python-eggs'}
    sc = SparkContext(conf=conf, environment=environment)
    sqlContext = SQLContext(sc)
    jvm_logger = sc._jvm.org.apache.log4j
    jvm_logger.LogManager.getLogger("org").setLevel(jvm_logger.Level.ERROR)
    jvm_logger.LogManager.getLogger("akka").setLevel(jvm_logger.Level.ERROR)
    return sc, sqlContext
