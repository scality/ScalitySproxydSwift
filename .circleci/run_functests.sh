#!/bin/bash

set -e
set -x

restart_swift()
{
    swift-init --run-dir=/opt/stack/data/swift/run all stop

    # Manually kill processes listening on Swift ports
    # to be sure that Swift can restart
    fuser -k -n tcp 8080 || true
    fuser -k -n tcp 6611 || true
    fuser -k -n tcp 6612 || true
    fuser -k -n tcp 6613 || true

    swift-init --run-dir=/opt/stack/data/swift/run all start || true
}

main()
{
    pushd /opt/stack/swift

    # Workaround to avoid the ImportError: No module named google_compute_engine
    rm -f /etc/boto.cfg

    # Create the tests results folder
    mkdir -p /tmp/ScalitySproxydSwift/func-tests-results

    export UPPER_CONSTRAINTS_FILE="/opt/stack/requirements/upper-constraints.txt"
    # Run functional tests on master branch
    tox -v -epy27 ./test/functional -- --with-xunit

    # Collect tests results
    mv nosetests.xml /tmp/ScalitySproxydSwift/func-tests-results/nosetests-master.xml

    # And then on each stable/* branch
    git fetch
    for branch in $(git branch -a | grep '/stable/')
    do
        branch_basename=$(basename $branch)
        git checkout stable/$branch_basename
        python setup.py install
        restart_swift
        rm -rf ./.tox
        tox -v -epy27 ./test/functional -- --with-xunit

        # Collect results
        mv nosetests.xml /tmp/ScalitySproxydSwift/func-tests-results/nosetests-$branch_basename.xml
    done

    # Collect logfiles
    cp /var/log/syslog /tmp/ScalitySproxydSwift/func-tests-results/
    cp /tmp/swift_logfile /tmp/ScalitySproxydSwift/func-tests-results/
    chown ${CIRCLECI_USER}:${CIRCLECI_USER} -R /tmp/ScalitySproxydSwift/func-tests-results/

    popd
}

main
