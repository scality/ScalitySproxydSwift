import hashlib
import json
import io
import os

import swiftclient.client

from fabric.api import task
from swiftclient.exceptions import ClientException

AUTH_ENDPOINT = 'http://{host:s}:8080/auth/v1.0'


def hash_dataset(directory, manifest_path=None):
    """
    SHA1 hash the content of each file in the given directory.

    :param directory: containing directory of files to compute hashes of
    :type directory: string
    :param manifest_path: path to where a manifest of all files and
        corresponding SHA1 hash will be written
    :type manifest_path: string
    :return: dictionary mapping filename to a hash of its content
    """
    file_hashes = {}
    for filename in os.listdir(directory):
        path = os.path.join(directory, filename)
        if os.path.isfile(path) and os.access(path, os.R_OK):
            with io.open(path, 'rb') as f:
                file_hashes[path] = hashlib.sha1(f.read()).hexdigest()
        else:
            print("Skipped '{0:s}'".format(path))

    manifest = manifest_path or os.path.join(directory, 'manifest')
    with io.open(manifest, 'wb') as manifest_file:
        json.dump(file_hashes, manifest_file, indent=2)

    return file_hashes


def summarize(failures):
    """
    Print a summary of any failures to stdout.

    :param failues: mapping of objects with bad integrity to an error string
    :type failures: dict
    """
    if failures:
        print("Objects with bad integrity encountered:")
        for obj, reason in failures.items():
            print("- {obj:<32s} {reason:s}".format(obj=obj, reason=reason))
    else:
        print("All objects checked out OK")


def create_container(swift_connection, name, storage_policy=None):
    """
    Create a container with an optional storage policy.

    :param swift_connection: connection to Swift
    :type swift_connection: :py:class:`swiftclient.client.Connection`
    :param name: container name
    :type name: string
    :param storage_policy: container storage policy (optional)
    :type storage_policy: string
    :return: policy of the craeted container
    """
    headers = {'X-Storage-Policy': storage_policy} if storage_policy else {}
    swift_connection.put_container(name, headers)
    return swift_connection.head_container(name).get('x-storage-policy')


def upload(swift_connection, container, files):
    """
    Upload a set of files to a container.

    :param swift_connection: connection to Swift
    :type swift_connection: :py:class:`swiftclient.client.Connection`
    :param container: target container for upload
    :type container: string
    :param files: paths to files for upload
    :type files: list of strings
    """
    for filename in files:
        with io.open(filename, 'rb') as f:
            swift_connection.put_object(container, filename, f)


def integrity_check(swift_connection, container, file_hashes):
    """
    Download and verify the integrity of a set of files from a bucket.

    :param swift_connection: connection to Swift
    :type swift_connection: :py:class:`swiftclient.client.Connection`
    :param container: container for integrity check
    :type container: string
    :param file_hashes: dictionary of mapping filename to a SHA1 hash
        of its content
    :type file_hashes: dict
    :return: dict of object names with bad integrity
    """
    failures = {}
    for filename, expected_sha1 in file_hashes.items():
        print("Checking integrity of '{0:s}'...".format(filename))
        try:
            headers, content = swift_connection.get_object(container, filename)
            actual_sha1 = hashlib.sha1(content).hexdigest()
            if expected_sha1 != actual_sha1:
                failures[filename] = (
                    "SHA1 mismatch: {expected:s} != {actual:s}".format(
                        expected=expected_sha1,
                        actual=actual_sha1,
                        )
                    )

        except ClientException as e:
            failures[filename] = (
                "Failed to retrieve object '{obj:s}': {exception:s}".format(
                    obj=filename,
                    exception=e,
                    )
                )

    return failures


@task
def container_integrity(swift_host, container, directory, storage_policy=None,
                        manifest_path=None, user='test:tester', key='testing'):
    """
    Upload files to a container and validate integrity.

    :param swift_host: hostname or IP of SAIO installation
    :type swift_host: string
    :param container: target container for upload and integrity check
    :type container: string
    :param directory: path to directory holding dataset for upload, and
        integrity check
    :type directory: string
    :param storage_policy: storage policy to apply on created container
    :type storage_policy: string
    :param manifest_path: path to where a manifest of all checked files will
        be written
    :type manifest_path: string
    :param user: username for authentication
    :type user: string
    :param key: password to authenticate with
    :type key: string
    """
    file_hashes = hash_dataset(directory, manifest_path)
    swift_connection = swiftclient.client.Connection(
        authurl=AUTH_ENDPOINT.format(host=swift_host),
        user=user,
        key=key,
        )
    try:
        policy = create_container(swift_connection, container, storage_policy)
        print("'{0:s}' created with policy '{1:s}'".format(container, policy))

        upload(swift_connection, container, file_hashes.keys())
        failures = integrity_check(swift_connection, container, file_hashes)
        summarize(failures)
    finally:
        swift_connection.close()


@task
def container_by_manifest(swift_host, container, manifest_path,
                          user='test:tester', key='testing'):
    """
    Validate the integrity of the files in a container from a manifest.

    :param swift_host: hostname or IP of SAIO installation
    :type swift_host: string
    :param container: container to inspect
    :type container: string
    :param manifest_path: path to the manifest file
    :type manifest_path: string
    :param user: username for authentication
    :type user: string
    :param key: password to authenticate with
    :type key: string
    """
    with io.open(manifest_path, 'rb') as manifest_file:
        manifest = json.load(manifest_file)

    swift_connection = swiftclient.client.Connection(
        authurl=AUTH_ENDPOINT.format(host=swift_host),
        user=user,
        key=key,
        )
    try:
        failures = integrity_check(swift_connection, container, manifest)
        summarize(failures)
    finally:
        swift_connection.close()
