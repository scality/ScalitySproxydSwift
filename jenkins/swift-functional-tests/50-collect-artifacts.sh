#!/bin/bash -xue

echo "Entering WORKSPACE."
cd $WORKSPACE

echo "Creating jenkins-log directory."
mkdir jenkins-logs
cp -R /opt/stack/logs/* jenkins-logs/

if [[ -f "/var/log/messages" ]]; then
    sudo cp /var/log/messages jenkins-logs/messages
fi

if [[ -f "/var/log/syslog" ]]; then
    sudo cp /var/log/syslog jenkins-logs/syslog
fi

sudo chown jenkins jenkins-logs/*
