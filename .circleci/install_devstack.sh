#!/bin/bash

set -e

add_scality_apt_repo()
{
    add-apt-repository multiverse
    mkdir -p /etc/apt/sources.list.d
    echo "deb [ arch=amd64 ] https://${SCAL_USERNAME}:${SCAL_PASSWORD}@${SCAL_URL} trusty scality/ring scality/thirdparty" \
	 > /etc/apt/sources.list.d/scality.list
    apt-get update
}

install_liberasurecode_from_source()
{
    pushd ~
    git clone https://github.com/openstack/liberasurecode.git
    pushd liberasurecode
    ./autogen.sh
    ./configure
    make
    make install
    popd
    popd
}

install_pyeclib_from_source()
{
    pushd ~
    git clone https://github.com/openstack/pyeclib.git
    pushd pyeclib
    pip install -U bindep -r test-requirements.txt
    python setup.py install
    echo '/usr/local/lib' >> /etc/ld.so.conf
    ldconfig
    popd
    popd
}

install_required_packages()
{
    DEBIAN_FRONTEND=noninteractive apt-get install --yes \
		   git \
		   gcc \
		   build-essential \
		   autoconf \
		   automake \
		   libtool \
		   zlib1g-dev \
		   python2.7 \
		   python-dev \
		   python-pip \
		   systemd \
		   apt-transport-https \
		   ca-certificates \
		   lsb-release \
		   software-properties-common \
		   bridge-utils \
		   iptables \
		   rsyslog \
		   sudo \
		   xfsprogs \
		   memcached

    pip install tox
}

install_required_deps()
{
    install_required_packages
    install_liberasurecode_from_source
    install_pyeclib_from_source
}

create_sudoer_user_scality()
{
    useradd -m -d /home/scality -s /bin/bash scality
    mkdir -p /etc/sudoers.d/
    echo 'scality ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers.d/scality
}

install_unit_file()
{
    cp /tmp/ScalitySproxydSwift/.circleci/$1.service /etc/systemd/system/
    chmod 644 /etc/systemd/system/$1.service
    chown root:root /etc/systemd/system/$1.service
    systemctl enable $1.service
}

install_systemd_unit_files()
{
    install_unit_file apache2
    install_unit_file mysql
    install_unit_file memcached

    systemctl daemon-reload
}

install_devstack()
{
    cp /tmp/ScalitySproxydSwift/.circleci/local.conf /tmp/
    chown scality:scality /tmp/local.conf

    # devstack/stack.sh needs to be executed as non-root user
    # We install devstack from Ocata since it is the last stable release to support ubuntu 14
    # which is the latest version offered by CircleCI at the moment
    pushd /tmp
    sudo -H -u scality bash <<EOF
set -x
set -e

git clone -b stable/ocata https://git.openstack.org/openstack-dev/devstack
mv /tmp/local.conf /tmp/devstack/
pushd /tmp/devstack
./stack.sh
popd
EOF
    popd
}

pip_reinstall_specific_deps()
{
    pip uninstall $1 -y
    if [ $3 -eq 1 ]
    then
	apt-get remove python-$1 --yes
    fi
    pip install $1==$2
}

install_swift_master()
{
    pushd /opt/stack/swift
    git fetch && git checkout master
    python setup.py install
    touch /tmp/swift_logfile
    chown scality:scality /tmp/swift_logfile
    popd
}

install_swift_scality_backend()
{
    rm -rf /usr/local/lib/python2.7/dist-packages/swift_scality_backend*
    pushd /tmp/ScalitySproxydSwift
    git checkout ${CIRCLE_BRANCH}
    python setup.py install
    popd
}

install_local_sproxyd()
{
    apt-get install --yes --allow-unauthenticated \
	    python3-requests \
	    scality-sproxyd \
	    scality-sproxyd-apache2

    cp /tmp/ScalitySproxydSwift/.circleci/sproxyd.conf /etc/

    # Copy the apache sproxyd config file with an extra specific directive to
    # prevent apache from truncating or rejecting the metadata above a certain
    # default size, considered too small by Swift
    cp /tmp/ScalitySproxydSwift/.circleci/scality-sd.conf /etc/apache2/sites-enabled/

    # We create a 6 gigabytes file so that sproxyd can store its data in it.
    # 6G was chosen so that it is fairly above the Swift file size limit of 5G,
    # which is tested in the functional tests suite
    truncate -s 6G /sproxyd-file

    # Install an XFS filesystem so that we can store metadata with a size > 4096
    mkfs.xfs /sproxyd-file

    # Create a mountpoint
    mkdir -p /var/tmp/local-sproxyd-file

    # Mount it as a loop device to simulate a disk
    mount -o loop /sproxyd-file /var/tmp/local-sproxyd-file/
}

main()
{
    add_scality_apt_repo

    set -x

    install_required_deps

    create_sudoer_user_scality

    install_systemd_unit_files

    install_devstack

    pip_reinstall_specific_deps urllib3 1.24.1 1
    pip_reinstall_specific_deps requests 2.14.2 0
    pip_reinstall_specific_deps idna 2.5 0

    swift-init --run-dir=/opt/stack/data/swift/run all stop

    install_swift_master
    install_swift_scality_backend

    # Install and launch local sproxyd
    install_local_sproxyd

    if pgrep -x "sproxyd" > /dev/null
    then
	killall -KILL sproxyd
    fi
    systemctl restart apache2
    /usr/bin/sproxyd -c /etc/sproxyd.conf

    # Kill processes listening on Swift ports
    fuser -k -n tcp 8080
    fuser -k -n tcp 6611
    fuser -k -n tcp 6612
    fuser -k -n tcp 6613

    # Start Swift
    swift-init --run-dir=/opt/stack/data/swift/run all start || true
}

main
