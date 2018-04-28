import argparse
import dask_yarn
from time import sleep


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--verbose', dest='verbose', action='store_true', help='Print logs on exit',
                        default=False)
    parser.add_argument('nworkers', help='Number of workers', type=int, default=4)
    parser.add_argument('ncores', help='Number of worker cores (threads)', type=int, default=3)
    parser.add_argument('memory', help='Worker memory (MiB)', type=int, default=5*1024)
    myargs = parser.parse_args()

    cluster = dask_yarn.DaskYARNCluster(env='/home/hadoop/reqs/dvss.zip', lang='en_US.UTF-8')
    cluster.start(n_workers=myargs.nworkers, memory=myargs.memory, cpus=myargs.ncores)
    try:
        while True:
            print('-' * 20)
            print('Cluster scheduler: {}.'.format(cluster.scheduler_address))
            bk = cluster.local_cluster.scheduler.services['bokeh'].server
            print('Bokeh: http://{}:{}'.format(bk.address, bk.port))
            sleep(20)
    except KeyboardInterrupt:
        print('Interrupted, exiting.')

    print('-' * 20)
    if myargs.verbose:
        cluster.knit.print_logs()
    print('Cluster is done.')
