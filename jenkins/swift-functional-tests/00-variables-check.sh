#!/bin/bash -xue

test -n "${DEVSTACK_BRANCH:-}" || (echo "DEVSTACK_BRANCH should be defined." && return 1);
