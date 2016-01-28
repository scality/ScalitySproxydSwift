#!/bin/bash -xue

cd /opt/stack/tempest

set +e
tox -e all -- 'object_storage(?!.*with_expect_continue)(?!.*ContainerSyncMiddlewareTest.test_container_synchronization)'
testr last --subunit | subunit2junitxml -o ${WORKSPACE}/tempest-tests.xml
set -e
