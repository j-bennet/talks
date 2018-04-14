import dask.dataframe as dd
from dask.distributed import Client
from dask_yarn import DaskYARNCluster


if __name__ == '__main__':
    cluster = DaskYARNCluster(env='/home/hadoop/conda/envs/dvss')
    cluster.start(n_workers=3, memory=500, cpus=5, checks=True)
    client = Client(address=cluster)
    print('-' * 20)
    print(cluster.knit.conf)
    read_path = 's3://parsely-public/jbennet/daskvsspark/events/100-24/*/*/*/*/*/*.parquet'
    df = dd.read_parquet(read_path)
    print('-' * 20)
    print(df.head(3))
