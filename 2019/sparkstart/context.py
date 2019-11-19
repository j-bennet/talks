from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SQLContext


def get_spark_context():
    conf = SparkConf()
    extra_settings = {
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.executor.extraJavaOptions": "-XX:+UseG1GC",
        "spark.default.parallelism": 200,
    }
    conf.setAll(extra_settings.items())
    environment = {"PYTHON_EGG_CACHE": "/tmp/python-eggs"}
    sc = SparkContext(conf=conf, environment=environment)
    return sc
