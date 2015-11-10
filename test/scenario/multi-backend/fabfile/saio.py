import os
import os.path

import fabric.contrib.files

from fabric.api import put, sudo

from utils import build_object_ring, render


def disk_setup(swift_user):
    # Setup a loopdevice to act as disk for swift
    sudo('mkdir -p /srv')
    sudo('truncate -s 1GB /srv/swift-disk')
    sudo('mkfs.xfs /srv/swift-disk')
    fabric.contrib.files.append(
        filename='/etc/fstab',
        text='/srv/swift-disk /mnt/sdb1 xfs loop,noatime 0 0',
        use_sudo=True
        )

    sudo('mkdir /mnt/sdb1')
    sudo('mount /mnt/sdb1')

    # Prepare directory structure for 4 swift nodes, with two "partitions" each
    node_mkdir = 'mkdir -p /mnt/sdb1/{0:d}/node/sdb{1:d}'
    num_nodes = 4
    for i in range(1, num_nodes + 1):
        sudo(node_mkdir.format(i, i))
        sudo(node_mkdir.format(i, i + num_nodes))
        sudo('ln -s /mnt/sdb1/{0:d} /srv/{1:d}'.format(i, i))
        sudo('mkdir /var/cache/swift{0:d}'.format(i))

    sudo('chown -R {0:s}: /mnt/sdb1'.format(swift_user))
    sudo('mkdir /var/run/swift')
    sudo('chown {0:s}: /var/run/swift /var/cache/swift*'.format(swift_user))
    render(
        directory='assets/saio/phase1/etc',
        filenames=['rc.local'],
        local_path_prefix='assets/saio/phase1',
        content={'user': swift_user},
        )
    sudo('chmod 755 /etc/rc.local')
    sudo('chown root: /etc/rc.local')


def install(swift_user):
    sudo('pip install '
         'git+https://github.com/openstack/python-swiftclient.git@2.6.0')
    sudo('pip install git+https://github.com/openstack/swift.git@2.5.0')

    content = { 'user': swift_user, 'group': swift_user }
    for path, _, filenames in os.walk('assets/saio/phase1/etc/swift'):
        render(path, filenames, 'assets/saio/phase1', content)

    sudo('chown -R {0:s}: /etc/swift'.format(swift_user))


def build_rings(swift_user):
    # Account ring
    build_object_ring(
        swift_user=swift_user,
        name='account.builder',
        devices=[
            'r1z1-127.0.0.1:6012/sdb1',
            'r1z2-127.0.0.1:6022/sdb2',
            'r1z3-127.0.0.1:6032/sdb3',
            'r1z4-127.0.0.1:6042/sdb4',
            ],
        )

    # Container ring
    build_object_ring(
        swift_user=swift_user,
        name='container.builder',
        devices=[
            'r1z1-127.0.0.1:6011/sdb1',
            'r1z2-127.0.0.1:6021/sdb2',
            'r1z3-127.0.0.1:6031/sdb3',
            'r1z4-127.0.0.1:6041/sdb4',
            ],
        )

    # Object ring
    build_object_ring(
        swift_user=swift_user,
        name='object.builder',
        devices=[
            'r1z1-127.0.0.1:6010/sdb1',
            'r1z1-127.0.0.1:6010/sdb5',
            'r1z2-127.0.0.1:6020/sdb2',
            'r1z2-127.0.0.1:6020/sdb6',
            'r1z3-127.0.0.1:6030/sdb3',
            'r1z3-127.0.0.1:6030/sdb7',
            'r1z4-127.0.0.1:6040/sdb4',
            'r1z4-127.0.0.1:6040/sdb8',
            ],
        )


def setup_rsync(swift_user):
    render(
        directory='assets/saio/phase1/etc',
        filenames=['rsyncd.conf'],
        local_path_prefix='assets/saio/phase1',
        content={ 'user': swift_user, 'group': swift_user },
        )
    fabric.contrib.files.sed(
        filename='/etc/default/rsync',
        before='RSYNC_ENABLE=false',
        after='RSYNC_ENABLE=true',
        use_sudo=True,
        )

    sudo('sudo service rsync restart')


def install_scality_swift():
    sudo('pip install git+https://github.com/scality/scality-sproxyd-client.git')
    sudo('pip install git+https://github.com/scality/ScalitySproxydSwift.git')


def start(swift_user):
    sudo('swift-init main start', user=swift_user)


def stop(swift_user):
    sudo('swift-init main stop', user=swift_user)
