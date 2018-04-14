import dask.dataframe as dd


if __name__ == '__main__':
    read_path = 's3://parsely-public/jbennet/daskvsspark/events/100-24/*/*/*/*/*/*.parquet'
    df = dd.read_parquet(read_path)
    print(df.head(3))
