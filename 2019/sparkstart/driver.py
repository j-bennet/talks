from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


def get_spark_context():
    pass


def read_input_data(sc, sqlContext, **kwargs):
    pass


def transform_or_aggregate(sc, sqlContext, df, **kwargs):
    pass


def save_output_data(df_out, **kwargs):
    pass


if __name__ == "__main__":
    with get_spark_context() as sc:
        sqlContext = SQLContext(sc)
        df_in = read_input_data(sc, sqlContext)
        df_out = transform_or_aggregate(sc, sqlContext, df)
        save_output_data(df_out)
