#!/bin/bash -xue

sudo add-apt-repository --yes ppa:fkrull/deadsnakes
sudo apt-get update
sudo apt-get install --yes python2.6 python2.6-dev

sudo aptitude install -y python-dev libffi-dev python-pip
sudo pip install tox
