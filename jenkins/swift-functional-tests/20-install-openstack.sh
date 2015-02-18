#!/bin/bash -xue

function get_release_number {
    local release_string=`lsb_release -r -s`
    local release_number=${release_string:0:2}
    echo $release_number
}

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

function main {
    release_number=$(get_release_number)
    if [ "$release_number" -eq "12" ]; then
        ubuntu12_specifics
    elif [ "$release_number" -eq "14" ]; then
        ubuntu14_specifics
    fi
    common
}

main