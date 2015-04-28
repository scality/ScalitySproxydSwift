#!/bin/bash -xue

test -n "${KEEPALIVE:-}" || (echo "KEEPALIVE should be defined." && return 1);
test -n "${DEVSTACK_BRANCH:-}" || (echo "DEVSTACK_BRANCH should be defined." && return 1);