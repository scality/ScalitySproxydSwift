#!/bin/bash -xue

SWIFT_DIR=/opt/stack/swift
sudo pip install -r $SWIFT_DIR/test-requirements.txt
set +e
nosetests -w $SWIFT_DIR/test/functional --exe --with-xunit --xunit-file=${WORKSPACE}/nosetests.xml
set -e
echo "Entering WORKSPACE."
cd $WORKSPACE
mkdir jenkins-logs
echo "Creating jenkins-log directory."
cp -R /opt/stack/logs/* jenkins-logs/
if [[ -f "/var/log/messages" ]]; then
    sudo cp /var/log/messages jenkins-logs/messages
fi
if [[ -f "/var/log/syslog" ]]; then
    sudo cp /var/log/syslog jenkins-logs/syslog
fi
sudo chown jenkins jenkins-logs/*
exit 0;
