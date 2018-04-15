#!/usr/bin/env bash

set -e

install_conda() {
    echo "Downloading conda"
    wget --quiet https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh
    chmod +x ~/miniconda.sh

    echo "Installing conda"
    ~/miniconda.sh -b -p ~/conda

    export PATH="/home/hadoop/conda/bin:$PATH"

    echo "Updating conda"
    conda update --yes conda
    conda info -a
}

create_conda_env() {
    echo "Creatinv venv dvss"
    conda create -n dvss --copy -y -q python=3
    echo "Installing requirements into venv"
    conda install -n dvss --copy -y -c conda-forge --file ~/reqs/requirements.txt --file ~/reqs/requirements-dask.txt
}

package_conda_env() {
    echo "Installing daskvsspark into venv"
    cd /home/hadoop/daskvsspark
    ~/conda/envs/dvss/bin/python setup.py install -q

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

if [[ ! -d /home/hadoop/conda ]]; then
    install_conda
fi

if [[ ! -f /home/hadoop/conda/envs/dvss ]]; then
    create_conda_env
fi

package_conda_env