#!/bin/bash -xue

SUP_ADMIN_LOGIN="myName"
SUP_ADMIN_PASS="myPass"
INTERNAL_MGMT_LOGIN="super"
INTERNAL_MGMT_PASS="adminPass"

HOST_IP=$(/sbin/ip addr show dev eth0 | sed -nr 's/.*inet ([0-9.]+).*/\1/p');

source jenkins/openstack-ci-scripts/jenkins/ring-install.sh

initialize
add_source
install_base_scality_node
install_supervisor
install_ringsh
build_ring
show_ring_status
install_sproxyd
test_sproxyd
