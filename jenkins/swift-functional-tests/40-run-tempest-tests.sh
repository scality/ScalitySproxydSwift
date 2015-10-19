#!/bin/bash -xue

EXCLUDES='test_get_object_after_expiry_time|test_get_object_at_expiry_time|test_container_sync_middleware'

TEMPEST_DIR=/opt/stack/tempest
sudo pip install -r $TEMPEST_DIR/requirements.txt

source jenkins/openstack-ci-scripts/jenkins/distro-utils.sh
if is_ubuntu; then
    if [[ "$(lsb_release -c -s)" == "trusty" ]]; then
        sudo pip install -U oslo.config
    fi
fi

set +e
nosetests -v -w $TEMPEST_DIR/tempest/api/object_storage --exe --exclude=${EXCLUDES} --with-xunit --xunit-file=${WORKSPACE}/tempest-tests.xml
set -e
