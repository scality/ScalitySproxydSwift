#!/bin/bash -xue

source jenkins/openstack-ci-scripts/jenkins/distro-utils.sh
install_packages git

git clone -b ${DEVSTACK_BRANCH} https://github.com/openstack-dev/devstack.git

cat > devstack/local.conf <<-EOF
	[[local|localrc]]
	DATABASE_PASSWORD=testtest; RABBIT_PASSWORD=testtest; SERVICE_TOKEN=testtest; SERVICE_PASSWORD=testtest; ADMIN_PASSWORD=testtest; SWIFT_HASH=011688b44136573e209e; SCREEN_LOGDIR=\${DEST}/logs
	disable_all_services; enable_service key mysql s-proxy s-object s-container s-account tempest
	# 167.88.149.196 is a physical server in the Scality OpenStack Lab. It hosts a copy
	# of github.com/scality/devstack-plugin-scality to avoid Github's rate limiting.
	enable_plugin scality git://167.88.149.196/devstack-plugin-scality
	SCALITY_SPROXYD_ENDPOINTS=http://127.0.0.1:81/proxy/bpchord
	[[post-config|\${SWIFT_CONF_DIR}/proxy-server.conf]]
	[filter:versioned_writes]
	allow_versioned_writes = true
	[[post-config|\${SWIFT_CONF_DIR}/object-server/1.conf]]
	[app:object-server]
	use = egg:swift_scality_backend#sproxyd_object
	sproxyd_host = 127.0.0.1:81
	sproxyd_path = /proxy/bpchord
EOF

if [[ ${DEVSTACK_BRANCH} == "stable/kilo" ]] && is_centos; then
    export RHEL7_RDO_REPO_RPM=http://mirror.centos.org/centos/7/cloud/x86_64/openstack-kilo/centos-release-openstack-kilo-2.el7.noarch.rpm
fi

cat > devstack/extras.d/60-scality-swift-diskfile.sh <<-EOF
	if [[ "\$1" == "stack" && "\$2" == "install" ]] && is_service_enabled swift; then
	    sudo pip install https://github.com/scality/scality-sproxyd-client/archive/master.tar.gz
	    sudo pip install .
	fi
EOF

if is_ubuntu && [[ $os_CODENAME == "precise" ]]; then
    # https://review.openstack.org/#/c/246973/ removed support for Ubuntu Precise
    export FORCE=yes
fi

# For some reason on Centos 7, there's a segfault in libpython-dev which
# crashes DevStack. In that case, try again.
./devstack/stack.sh || ./devstack/stack.sh

sudo pip install junitxml
