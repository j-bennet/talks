# -*- coding: utf-8
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SQLContext


INPUT_PATH = "./events/"
OUTPUT_PATH = "./aggs/"


def initialize():
    """Returns SparkContext and SQLContext."""
    conf = SparkConf()
    environment = {'PYTHON_EGG_CACHE': '/tmp/python-eggs'}
    sc = SparkContext(conf=conf, environment=environment)
    sqlContext = SQLContext(sc)
    jvm_logger = sc._jvm.org.apache.log4j
    jvm_logger.LogManager.getLogger("org").setLevel(jvm_logger.Level.ERROR)
    jvm_logger.LogManager.getLogger("akka").setLevel(jvm_logger.Level.ERROR)
    return sc, sqlContext
