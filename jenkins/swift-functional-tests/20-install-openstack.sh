#!/bin/bash -xue


function common {
    git clone -b ${DEVSTACK_BRANCH} https://github.com/openstack-dev/devstack.git
    cp devstack/samples/local.conf devstack/local.conf
    cat >> devstack/local.conf <<EOF
disable_all_services
enable_service key mysql s-proxy s-object s-container s-account
SCREEN_LOGDIR="\${DEST}/logs"
EOF
    cp jenkins/${JOB_NAME%%/*}/extras.d/55-swift-sproxyd.sh devstack/extras.d/55-swift-sproxyd.sh
    if [[ $DEVSTACK_BRANCH == "stable/icehouse" ]]; then
        # Workaound depencies version conflict :
        # last python-glanceclient (0.17 at the time of writing), wich gets installed by default,
        # requires keystoneclient >= 1.0.0
        # whereas keystone requires keystoneclient <= 0.11.2
        # python-glanceclient >= 0.13.1 is required by openstackclient 0.4.1
        sudo pip install python-glanceclient==0.13.1
    fi
    ./devstack/stack.sh
}

function ubuntu_common {
    wget https://bootstrap.pypa.io/ez_setup.py -O - | sudo python;
    sudo easy_install pip
    if [[ $DEVSTACK_BRANCH == 'master' ]]; then
        sudo aptitude install -y make
    elif [[ $DEVSTACK_BRANCH == "stable/icehouse" ]]; then
        sudo aptitude install -y gcc python-dev
    fi
}

function ubuntu14_specifics {
    # Workaround pip upgrading six without completely removing the old one
    # which then cause an error.
    sudo easy_install -U six
}

function ubuntu12_specifics {
    # Workaround pip upgrading cmd2 without completely removing the old one
    # which then cause an error.
    sudo easy_install -U cmd2
}

function centos_specifics {
    sudo yum install -y wget
    wget https://bootstrap.pypa.io/ez_setup.py -O - | sudo python;
    sudo easy_install -U six
    sudo yum install -y python-pip
    if [[ $DEVSTACK_BRANCH == "stable/icehouse" ]]; then
        # Required to get 'cryptography' python package compiled during its installation through pip.
        sudo yum install -y gcc python-devel libffi-devel openssl-devel
        # Required by keystoneclient
        sudo yum install -y MySQL-python
        # devstack uses the ip command.
        export PATH=$PATH:/sbin/
    fi
}

function main {
    source jenkins/openstack-ci-scripts/jenkins/distro-utils.sh
    if is_ubuntu; then
        ubuntu_common
        if [[ $os_CODENAME == "precise" ]]; then
            ubuntu12_specifics
        elif [[ $os_CODENAME == "trusty" ]]; then
            ubuntu14_specifics
        fi
    elif is_centos; then
        centos_specifics
    fi
    common
}

main
