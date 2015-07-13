#!/bin/bash -xue

function install_deb {
    sudo aptitude update
    sudo aptitude install -y python-dev libffi-dev build-essential
}

function install_centos {
    # GCC installed automatically in debian because recommended packages
    # Doing that manually in centos.
    sudo yum -y install python-devel libffi-devel epel-release gcc wget
}

function is_centos {
    [[ -f /etc/centos-release ]]
}

function is_deb {
    [[ -f /etc/debian_version ]]
}

function install {
    if is_deb; then
        install_deb
    elif is_centos; then
        install_centos
    else
        echo "Unknown distribution"
        exit 1
    fi

    wget https://bootstrap.pypa.io/ez_setup.py -O - | sudo python -
    sudo easy_install pip
    # I can't have Tox 1.9.1 to work with external repository
    # (scality-sproxyd-client)
    sudo pip install "tox<=1.9.0"
}

install
