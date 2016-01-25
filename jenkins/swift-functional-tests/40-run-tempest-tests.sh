#!/bin/bash -xue

cd /opt/stack/tempest

# Once https://review.openstack.org/#/c/242076/ is merged we should also enable
# Tempest scenarios
set +e
tox -e all -- 'object_storage(?!.*with_expect_continue)(?!.*ContainerSyncMiddlewareTest.test_container_synchronization)'
set -e

testr last --subunit | subunit2junitxml -o ${WORKSPACE}/tempest-tests.xml
