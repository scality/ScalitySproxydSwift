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
    ./devstack/stack.sh
}

function ubuntu14_specifics {
    # Workaround pip upgrading six without completely removing the old one
    # which then cause an error.
    wget https://bootstrap.pypa.io/ez_setup.py -O - | sudo python;
    sudo easy_install -U six
}

function ubuntu12_specifics {
    # Workaround pip upgrading cmd2 without completely removing the old one
    # which then cause an error.
    wget https://bootstrap.pypa.io/ez_setup.py -O - | sudo python;
    sudo easy_install -U cmd2
}

function centos_specifics {
    sudo yum install -y wget
    wget https://bootstrap.pypa.io/ez_setup.py -O - | sudo python;
    sudo easy_install -U six
}

function main {
    source jenkins/openstack-ci-scripts/jenkins/distro-utils.sh
    if is_ubuntu; then
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
