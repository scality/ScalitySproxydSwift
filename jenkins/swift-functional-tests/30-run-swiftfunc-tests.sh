#!/bin/bash -xue
source jenkins/openstack-ci-scripts/jenkins/distro-utils.sh
if is_centos; then
    sudo pip install -U nose
fi
SWIFT_DIR=/opt/stack/swift
sudo pip install -r $SWIFT_DIR/test-requirements.txt
set +e
nosetests -v -w $SWIFT_DIR/test/functional --exe --with-xunit --xunit-file=${WORKSPACE}/swift-func-tests.xml
set -e

