import os.path

import fabric.contrib.files

from fabric.api import cd, sudo


def render(directory, filenames, local_path_prefix, content):
    """
    Render a list of templates in a certain directory and upload them.

    :param directory: local directory holding templates
    :type directory: string
    :param filenames: files to render from local directory
    :type filenames: list of strings
    :param local_path_prefix: prefix to strip from the directory path to
        obtain remote path
    :type local_path_prefix: string
    :param content: content for string interpolation on templates
    :type content: dict
    """
    remote_directory = directory[len(local_path_prefix):]
    sudo('mkdir -p {0:s}'.format(remote_directory))
    for filename in filenames:
        local_path = os.path.join(directory, filename)
        fabric.contrib.files.upload_template(
            filename=local_path,
            destination=os.path.join(remote_directory, filename),
            context=content,
            use_sudo=True,
            )


def build_object_ring(swift_user, name, devices, part_power=10, replicas=3):
    """
    Build a swift object ring by invocation of `swift-ring-builder`.

    :param swift_user: the user running swift
    :type swift_user: string
    :param name: name of the ring file to create, eg. `account.builder`
    :type name: string
    :param devices: list of fully qualified device names
        z<zone>-<ip>:<port>/<device>_<meta>
    :type devices: list of strings
    :param part_power: 2^<part_power> of partitions in the ring (optional)
    :type part_power: int
    :param replicas: replication factor
    :type replicas: int
    """
    # swift-ring-builder <file> create <part_power> <replicas> <min_part_hours>
    create_cmd = "swift-ring-builder {0:s} create {1:d} {2:d} 1"
    # swift-ring-builder <file> add z<zone>-<ip>:<port>/<dev>_<meta> <weight>
    add_cmd = "swift-ring-builder {0:s} add {1:s} 1"
    with cd('/etc/swift'):
        sudo(create_cmd.format(name, part_power, replicas), user=swift_user)

        for device in devices:
            sudo(add_cmd.format(name, device), user=swift_user)

        sudo('swift-ring-builder {0:s} rebalance'.format(name), user=swift_user)


def apt_get(packages):
    """
    Install packages through apt-get.

    :param packages: list of packages for installation
    :type packages: list of strings
    """
    sudo('apt-get update')
    sudo('apt-get install -y {0:s}'.format(' '.join(packages)))
