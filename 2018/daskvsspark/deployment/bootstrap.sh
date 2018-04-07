#!/usr/bin/env bash

# Stop at any error, show all commands
set -ex

# Are we running on a master node?
cat /var/lib/info/instance.json | grep '"isMaster": true'
IS_MASTER=$?
unset PYTHON_INSTALL_LAYOUT


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

    PIP="python3 -m pip"

    cd /home/hadoop/
    # this includes a jar also
    aws s3 cp --recursive s3://parsely-public/jbennet/daskvsspark/reqs/ ./reqs
    $PIP install -U pip
    $PIP install -r ./artifacts/requirements.txt
}

install_python_36
update_packages
