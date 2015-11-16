import os
import urlparse

import saio
import utils

from fabric.api import env, run, sudo, task
from fabric.context_managers import cd, hide, prefix, settings


@task
def swift(swift_user):
    """
    Bootstrap a SAIO installation.

    :param swift_user: the user to run swift as
    :type swift_user: string
    """
    packages = [
        'curl', 'gcc', 'git-core', 'memcached', 'rsync', 'sqlite3',
        'xfsprogs', 'git-core', 'libffi-dev', 'python-coverage', 'python-dev',
        'python-simplejson', 'python-xattr', 'python-eventlet',
        'python-greenlet', 'python-pastedeploy', 'python-netifaces',
        'python-pip', 'python-dnspython',
        ]

    utils.apt_get(packages)

    # Add swift user
    sudo('useradd -r -s /usr/bin/nologin -d /srv -U {0:s}'.format(swift_user))

    saio.disk_setup(swift_user)
    saio.install(swift_user)
    saio.setup_rsync(swift_user)
    saio.build_rings(swift_user)
    saio.start(swift_user)


@task
def ring():
    """
    Bootstrap Scality RING (environment variable `SCAL_PASS` must be exported).

    The environment variable `SCAL_PASS` is expected to hold username:password
    for fetching scality packages.
    """
    install_env = {
        'SUP_ADMIN_LOGIN': 'supadmin',
        'SUP_ADMIN_PASS': 'supadmin',
        'INTERNAL_MGMT_LOGIN': 'admin',
        'INTERNAL_MGMT_PASS': 'admin',
        'HOST_IP': env.host,
        'SCAL_PASS': os.environ['SCAL_PASS'],
        'AllowEncodedSlashes': 'NoDecode',
        }
    export_vars = ('{0:s}={1:s}'.format(k, v) for k, v in install_env.items())
    export_cmd = 'export {0:s}'.format(' '.join(export_vars))

    utils.apt_get(['git-core'])
    run('git clone https://github.com/scality/openstack-ci-scripts.git')

    # Hide aborts to not leak any repository passwords to console on failure.
    with cd('openstack-ci-scripts/jenkins'), prefix(export_cmd):
        with prefix('source ring-install.sh'), settings(hide('aborts')):
            run('add_source')
            run('install_base_scality_node', pty=False)  # avoid setup screen
            run('install_supervisor')
            run('install_ringsh')
            run('build_ring')
            run('install_sproxyd')
            run('test_sproxyd')


@task
def scality_storage_policy(swift_user, sproxyd_endpoint):
    """
    Install a storage policy backed by Scality RING.

    :param swift_user: the user swift is running as
    :type swift_user: string
    :param sproxyd_endpoint: the sproxyd endpoint backing the storage policy
    :type sproxyd_endpoint: string
    """
    # Validate sproxyd endpoint connectivity.
    curl_cmd = run('curl --fail --connect-timeout 15 {0:s}/.conf'.format(
                   sproxyd_endpoint), warn_only=True)

    if curl_cmd.failed:
        raise Exception("Unable to establish a connection to sproxyd at "
                        "'{0:s}.'".format(sproxyd_endpoint))

    # Install dependencies.
    saio.install_scality_swift()

    # Add scality RING storage policy.
    endpoint_parts = urlparse.urlparse(sproxyd_endpoint)
    content = {
        'sproxyd_endpoint': sproxyd_endpoint,
        'sproxyd_netloc': endpoint_parts.netloc,
        'sproxyd_path': endpoint_parts.path,
        'user': swift_user,
        }
    for path, _, filenames in os.walk('assets/saio/phase2/etc/swift'):
        utils.render(path, filenames, 'assets/saio/phase2', content)
    sudo('chown -R {0:s} /etc/swift'.format(swift_user))

    # Setup an additional object ring.
    sudo('mkdir -p /srv/scality/placeholder')
    sudo('chown -R {0:s}: /srv/scality'.format(swift_user))
    sudo('mkdir -p /var/cache/scality')
    sudo('chown {0:s}: /var/cache/scality'.format(swift_user))

    utils.render(
        directory='assets/saio/phase2/etc',
        filenames=['rc.local'],
        local_path_prefix='assets/saio/phase2',
        content={'user': swift_user},
        )
    sudo('chmod 755 /etc/rc.local')
    sudo('chown root: /etc/rc.local')

    utils.build_object_ring(
        swift_user=swift_user,
        name='object-1.builder',
        devices=[
            'r1z1-127.0.0.1:6050/placeholder',
            ],
        replicas=1,
        )

    saio.stop(swift_user)
    saio.start(swift_user)
