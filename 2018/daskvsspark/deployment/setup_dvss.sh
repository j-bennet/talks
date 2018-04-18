#!/usr/bin/env bash
# This should run on master first, and then on workers.

# Are we running on a master node?
cat /var/lib/info/instance.json | grep '"isMaster": true'
IS_MASTER=$?

set -e

build_egg() {
    echo "Building the egg"
    cd /home/hadoop/daskvsspark/
    python3 setup.py bdist_egg
    cp ./dist/*.egg /home/hadoop/reqs/
    echo "Uploading the egg to s3"
    aws s3 cp ~/reqs/daskvsspark-0.1-py3.6.egg s3://parsely-public/jbennet/daskvsspark/reqs/
}

package_env() {
    #echo "Installing daskvsspark into master's venv"
    #cd /home/hadoop/daskvsspark
    #~/conda/envs/dvss/bin/python setup.py install -q

    if [[ -f ~/reqs/dvss.zip ]]; then
        rm ~/reqs/dvss.zip
    fi

    echo "Zipping up venv"
    cd ~/conda/envs
    zip -qr dvss.zip dvss
    mv dvss.zip ~/reqs/
    echo "Uploading zip to s3"
    aws s3 cp ~/reqs/dvss.zip s3://parsely-public/jbennet/daskvsspark/reqs/
}

download_env() {
    echo "Downloading venv and egg"
    aws s3 cp s3://parsely-public/jbennet/daskvsspark/reqs/daskvsspark-0.1-py3.6.egg /home/hadoop/reqs/
    aws s3 cp s3://parsely-public/jbennet/daskvsspark/reqs/dvss.zip /home/hadoop/reqs/
}

if [[ $IS_MASTER eq 0 ]]; then
    build_egg
    package_env
else
    download_env
fi