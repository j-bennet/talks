from time import sleep
import dask_yarn


cluster = dask_yarn.DaskYARNCluster(env='/home/hadoop/reqs/dvss.zip', lang='en_US.UTF-8')
cluster.start(n_workers=3, memory=5*1024, cpus=5)
try:
    while True:
        print('-' * 20)
        print('Cluster scheduler: {}.'.format(cluster.scheduler_address))
        print('Bokeh: {}'.format(cluster.local_cluster.scheduler.services['bokeh']))
        sleep(20)
except KeyboardInterrupt:
    print('Interrupted, exiting.')

print('-' * 20)
cluster.knit.print_logs()
print('Cluster is done.')
