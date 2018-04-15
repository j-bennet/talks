import sys
import dask.dataframe as dd
from dask.distributed import Client


if __name__ == '__main__':
    address = sys.argv[1]
    print('Connecting to address: {}.'.format(address))
    client = Client(address=address)
    df = dd.read_parquet('s3://parsely-public/jbennet/daskvsspark/events/100-24/*/*/*/*/*/*.parquet')
    print('-' * 20)
    print(df.head(3))
