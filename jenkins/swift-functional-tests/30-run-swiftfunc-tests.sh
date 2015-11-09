#!/bin/bash -xue

cd /opt/stack/swift

set +e
tox -e func -- test/functional --with-xunit --xunit-file=${WORKSPACE}/swift-func-tests.xml
set -e
