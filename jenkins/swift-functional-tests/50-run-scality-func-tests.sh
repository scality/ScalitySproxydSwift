#!/bin/bash -xue

function ln_object_ring {
    for i in {2..6}; do
	local obj_link=/etc/swift/object-${i}.ring.gz
	if ! [[ -e $obj_link ]]; then
	    ln -s /etc/swift/object.ring.gz $obj_link
	fi
    done
}

# FIXME : this is already defined 10-install-ring.sh
function get_AllowEncodedSlashes {
    if is_ubuntu; then
	echo "$UBUNTU_AllowEncodedSlashes"
    elif is_centos; then
	echo "$CENTOS_AllowEncodedSlashes"
    else
	echo "Unkown distribution"
	return 1
    fi
}

function initialize_env {
    set +u
    source devstack/openrc demo
    OS_DEMO_USERNAME=$OS_USERNAME
    OS_DEMO_PASSWORD=$OS_PASSWORD
    OS_DEMO_TENANT_NAME=$OS_TENANT_NAME
    source devstack/openrc admin
    set -u
    OS_ADMIN_USERNAME=$OS_USERNAME
    OS_ADMIN_PASSWORD=$OS_PASSWORD
    OS_ADMIN_TENANT_NAME=$OS_TENANT_NAME

    source jenkins/openstack-ci-scripts/jenkins/distro-utils.sh
    AllowEncodedSlashes=$(get_AllowEncodedSlashes)
    # FIXME : this is already defined 10-install-ring.sh
    HOST_IP=$(/sbin/ip addr show dev eth0 | sed -nr 's/.*inet ([0-9.]+).*/\1/p');
    SPROXYD_NUMBER=2
}

function grant_permissions {
    local user=$(whoami)
    local directory
    if  [[ -d /etc/httpd/conf.d ]]; then
	directory="/etc/httpd/conf.d"
    elif  [[ -d /etc/apache2/sites-enabled ]]; then
	directory=/etc/apache2/sites-enabled
    else
	echo "Unkown distribution"
	return 1
    fi

    sudo chown ${user} $directory
    sudo chown ${user} /etc
}

function create_document_root {
    let upper_bound=$SPROXYD_NUMBER+2
    for ((i=2; i<upper_bound; i++)); do
	local directory=/var/www/sproxyd-${i}
	if ! [[ -d ${directory} ]]; then
	    sudo mkdir $directory
	fi
    done
}

if [[ $DEVSTACK_BRANCH != 'stable/icehouse' ]]; then
    # FIXME : this functional test suite rely completely on the storage policies support which is starting with juno
    # Tests in test/func/test_common.py should not rely on the storage policy support so that they can be run against icehouse
    ln_object_ring
    initialize_env
    grant_permissions
    create_document_root
    sudo pip install pytest pytest-timeout subprocess32

    set +e
    py.test -v --timeout=60 --os-auth-url=$OS_AUTH_URL --os-identity-api-version=$OS_IDENTITY_API_VERSION --os-demo-username=$OS_DEMO_USERNAME --os-demo-password=$OS_DEMO_PASSWORD --os-demo-tenantname=$OS_DEMO_TENANT_NAME --os-admin-username=$OS_ADMIN_USERNAME --os-admin-password=$OS_ADMIN_PASSWORD --os-admin-tenantname=$OS_ADMIN_TENANT_NAME --host-ip=$HOST_IP --allow-encoded-slashes=$AllowEncodedSlashes --sproxyd-numbers=$SPROXYD_NUMBER --junit-xml=${WORKSPACE}/scality-func-tests.xml test/func
    set -e
fi




