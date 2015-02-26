#!/bin/bash -xue

XUNIT_FILE=nosetests.xml

rm -f $XUNIT_FILE

export NOSE_WITH_XUNIT=1
export NOSE_XUNIT_FILE=$XUNIT_FILE

set +e
tox
TOXRC=$?
set -e

if test $TOXRC -ne 0; then
        echo "tox run unsuccessful, build unstable"
fi


# We need at least one test, otherwise Jenkins' JUnit reporter gets cranky
if test "x${TOXENV}" == "xpep8"; then
        if test $TOXRC -eq 0; then
                cat > $XUNIT_FILE << EOF
<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="tox-pep8" tests="1" errors="0" failures="0" skip="0">
    <testcase classname="pep8" name="run" time="0.000" />
</testsuite>
EOF
        else
                cat > $XUNIT_FILE << EOF
<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="tox-pep8" tests="1" errors="0" failures="1" skip="0">
    <testcase classname="pep8" name="run" time="0.000">
        <failure type="exceptions.AssertionError" message="PEP8 failed">
            See console log
        </failure>
    </testcase>
</testsuite>
EOF
        fi
fi
