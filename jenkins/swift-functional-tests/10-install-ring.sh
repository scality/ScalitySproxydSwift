#!/bin/bash -xue


function amend_apache_conf {
    local option=$1
    local filepath
    for filepath in "/etc/apache2/apache2.conf" "/etc/httpd/conf/httpd.conf"; do
        if  [[ -f $filepath ]]; then
            sudo sed -i'.sedbck' "s/KeepAlive O.*/KeepAlive ${option}/" $filepath
            return 0
        fi
    done
    echo "Could not find the path to the apache configuration file."
    return 1
}

function restart_apache {
    local filepath
    for filepath in "/etc/init.d/apache2" "/etc/init.d/httpd"; do
        if  [[ -f $filepath ]]; then
            sudo $filepath restart
            return 0
        fi
    done
    echo "Could not find the apache init script."
    return 1
}

function set_keepalive {
    local option=$1
    amend_apache_conf $option
    restart_apache
}


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
set_keepalive $KEEPALIVE
test_sproxyd
