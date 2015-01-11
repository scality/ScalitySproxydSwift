#!/bin/bash -xue

XUNIT_FILE=nosetests.xml

# Cleanup
rm -f $XUNIT_FILE
coverage erase

# Run
export NOSE_WITH_COVERAGE=1
export NOSE_COVER_BRANCHES=1
export NOSE_COVER_PACKAGE=swift_scality_backend

export NOSE_WITH_XUNIT=1
export NOSE_XUNIT_FILE=$XUNIT_FILE

tox -e cover

# Report
coverage xml
coverage html
