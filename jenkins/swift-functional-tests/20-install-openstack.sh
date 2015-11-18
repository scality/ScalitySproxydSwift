#!/bin/bash -xue

source jenkins/openstack-ci-scripts/jenkins/distro-utils.sh
install_packages git

git clone -b ${DEVSTACK_BRANCH} https://github.com/openstack-dev/devstack.git

cat > devstack/local.conf <<-EOF
	[[local|localrc]]
	DATABASE_PASSWORD=testtest; RABBIT_PASSWORD=testtest; SERVICE_TOKEN=testtest; SERVICE_PASSWORD=testtest; ADMIN_PASSWORD=testtest; SWIFT_HASH=011688b44136573e209e; SCREEN_LOGDIR=\${DEST}/logs
	disable_all_services; enable_service key mysql s-proxy s-object s-container s-account tempest
	[[post-config|\${SWIFT_CONF_DIR}/proxy-server.conf]]
	[filter:versioned_writes]
	allow_versioned_writes = true
	[[post-config|\${SWIFT_CONF_DIR}/object-server/1.conf]]
	[app:object-server]
	use = egg:swift_scality_backend#sproxyd_object
	sproxyd_host = 127.0.0.1:81
	sproxyd_path = /proxy/bpchord
EOF

# stable/juno is broken. Changes in DevStack and in Swift are required but
# upstreaming the fixes is not practical.
if [[ ${DEVSTACK_BRANCH} == "stable/juno" ]]; then
	cat > devstack/extras.d/10-fix-scality.sh <<-EOF
		if [[ "\$1" == "stack" && "\$2" == "install" ]]; then
		    pip_install_gr python-novaclient; pip_install_gr python-cinderclient; pip_install_gr python-glanceclient; pip_install_gr python-neutronclient
		    if is_service_enabled swift; then
		        cd /opt/stack/swift
		        sed -i 's/ignore-errors = True/ignore_errors = True/' .coveragerc
		        grep -q python-keystoneclient test-requirements.txt || echo 'python-keystoneclient>=0.10.0,<1.2.0' >> test-requirements.txt
		        grep -q 'passenv = SWIFT_' tox.ini || sed -i '/commands = nosetests {posargs:test\/unit}/ a\passenv = SWIFT_* *_proxy' tox.ini
		        cd -
		    fi
		fi
	EOF
fi

cat > devstack/extras.d/60-scality-swift-diskfile.sh <<-EOF
	if [[ "\$1" == "stack" && "\$2" == "install" ]] && is_service_enabled swift; then
	    sudo pip install .
	fi
EOF

# For some reason on Centos 7, there's a segfault in libpython-dev which
# crashes DevStack. In that case, try again.
./devstack/stack.sh || ./devstack/stack.sh

sudo pip install junitxml
