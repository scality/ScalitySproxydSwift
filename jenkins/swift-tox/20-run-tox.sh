#!/bin/bash -xue

XUNIT_FILE=nosetests.xml

rm -f $XUNIT_FILE

export NOSE_WITH_XUNIT=1
export NOSE_XUNIT_FILE=$XUNIT_FILE

# We need at least one test, otherwise Jenkins' JUnit reporter gets cranky
if test "x${TOXENV}" == "xpep8"; then
        cat > $XUNIT_FILE << EOF
<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="nosetests" tests="1" errors="0" failures="0" skip="0">
    <testcase classname="test" name="test_noop" time="0.000" />
</testsuite>
EOF
fi

tox
