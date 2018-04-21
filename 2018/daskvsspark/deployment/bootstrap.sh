#!/usr/bin/env bash

# Are we running on a master node?
cat /var/lib/info/instance.json | grep '"isMaster": true'
IS_MASTER=$?

unset PYTHON_INSTALL_LAYOUT
export LC_ALL=C.UTF-8
export LANG=C.UTF-8

# Stop at any error, show all commands
set -ex


install_python_36() {
    # Ensure Python 3.6 is installed
    if [[ ! -x /usr/local/bin/python3.6 ]]; then
        echo "Python 3.6 not installed, installing"
        # Compilers and related tools:
        sudo yum groupinstall -y "development tools"
        # Libraries needed during compilation to enable all features of Python:
        sudo yum install -y zlib-devel bzip2-devel openssl-devel ncurses-devel sqlite-devel readline-devel tk-devel gdbm-devel db4-devel libpcap-devel xz-devel expat-devel
        # Download and install Python 3.6.1
        wget https://s3.amazonaws.com/parsely-public/chef-pkgs/python_3.6.1_x86_64.rpm
        sudo rpm -ivh python_3.6.1_x86_64.rpm
        # Make sure we have pip
        sudo /usr/local/bin/python3 -m ensurepip --upgrade
    fi

    echo "Note: Python 3.6 will be available as python3, not python"
    echo "Be sure to set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON in Configurations"
}

update_packages() {
    # Do this again just in case
    unset PYTHON_INSTALL_LAYOUT

    PIP="sudo /usr/local/bin/pip3"

    cd /home/hadoop/

    # this includes a jar also
    aws s3 cp --recursive s3://parsely-public/jbennet/daskvsspark/reqs/ ./reqs
    chmod +x ./reqs/*.sh

    # needed to install python-snappy
    sudo yum install -y snappy-devel

    $PIP install -U pip
    $PIP install -r ./reqs/requirements.txt
}

install_conda() {
    if [[ ! -d /home/hadoop/conda ]]; then
        echo "Downloading conda"
        wget --quiet https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh
        chmod +x ~/miniconda.sh

        echo "Installing conda"
        ~/miniconda.sh -b -p ~/conda

        export PATH="/home/hadoop/conda/bin:$PATH"
        echo 'export PATH="/home/hadoop/conda/bin:$PATH"' >> ~/.bashrc

        echo "Updating conda"
        conda update --yes conda
        conda info -a
    fi
}

create_conda_env() {
    if [[ ! -f /home/hadoop/conda/envs/dvss ]]; then
        echo "Creatinv venv dvss"
        conda create -n dvss --copy -y -q python=3
        echo "Installing requirements into venv"
        conda install -n dvss --copy -y -c conda-forge --file ~/reqs/requirements.txt --file ~/reqs/requirements-dask.txt
    fi
}


install_python_36
update_packages
install_conda
create_conda_env
