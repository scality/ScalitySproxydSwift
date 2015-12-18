#!/bin/bash -xue

cd /opt/stack/swift

set +e
if [[ ${DEVSTACK_BRANCH} == "stable/juno" ]] || [[ ${DEVSTACK_BRANCH} == "stable/kilo" ]] || [[ ${DEVSTACK_BRANCH} == "stable/liberty" ]]; then
    tox -e func -- test/functional --with-xunit --xunit-file=${WORKSPACE}/swift-func-tests.xml
else
    tox -e func
fi

if [[ ${DEVSTACK_BRANCH} == "stable/mitaka" ]] || [[ ${DEVSTACK_BRANCH} == "master" ]]; then
    testr last --subunit | subunit2junitxml -o ${WORKSPACE}/swift-func-tests.xml
fi
set -e
