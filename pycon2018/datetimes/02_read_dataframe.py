# -*- coding: utf-8
# 02_read_dataframe.py
from pprint import pprint
from shared.context import initialize, FILENAME


if __name__ == '__main__':
    sc, sqlContext = initialize()
    df = sqlContext.read.parquet(FILENAME)
    print("Dataframe after writing:")
    pprint(df.collect())
