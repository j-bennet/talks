import dask.dataframe as dd
from dask.distributed import Client


if __name__ == '__main__':
    client = Client()
    df = dd.read_parquet('s3://path/here')
    print(df.head(3))
