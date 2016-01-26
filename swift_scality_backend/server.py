# Copyright (c) 2010-2013 OpenStack, LLC.
# Copyright (c) 2014 Scality
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Scality Sproxyd Object Server for Swift """

import errno

import swift.common.bufferedhttp
import swift.common.exceptions
import swift.common.http
import swift.obj.server

import scality_sproxyd_client.sproxyd_client

import swift_scality_backend.diskfile
import swift_scality_backend.http_utils
import swift_scality_backend.policy_configuration
import swift_scality_backend.utils

POLICY_STUB = object()


class ScalityDiskFileRouter(object):
    """
    Replacement for Swift's DiskFileRouter object.
    Always returns Scality's DiskFileManager implementation.

    Copied from SwiftOnFile::obj/server.py
    """
    def __init__(self, *args, **kwargs):
        self.manager_cls = swift_scality_backend.diskfile.DiskFileManager(
            *args, **kwargs)

    def __getitem__(self, policy):
        return self.manager_cls


class ObjectController(swift.obj.server.ObjectController):
    """Implements the WSGI application for the Scality Object Server."""

    def __init__(self, *args, **kwargs):
        self._clients = {}
        self._conn_timeout = None
        self._read_timeout = None
        self._diskfile_mgr = None
        self._policy_configuration = None
        self._policy_0_urls = None
        self._location_preferences = None

        super(ObjectController, self).__init__(*args, **kwargs)

    def setup(self, conf):
        """Class setup

        :param conf: WSGI configuration parameter
        """
        # Replaces Swift's DiskFileRouter object reference with ours.
        self._diskfile_router = ScalityDiskFileRouter(conf, self.logger)

        # New style configuration expects
        # sproxyd_endpoints = http://sproxyd1:port/path1, http://sproxyd2:port/path2
        # in object-server.conf
        if conf.get('sproxyd_endpoints'):
            self._policy_0_urls = swift_scality_backend.utils.split_list(
                conf['sproxyd_endpoints'])
        # But we still support specifying sproxyd_host and sproxyd_path
        else:
            sproxyd_path = conf.get('sproxyd_path', '/proxy/chord').strip('/')
            self._policy_0_urls = [
                'http://%s/%s/' % (h, sproxyd_path)
                for h in swift_scality_backend.utils.split_list(conf['sproxyd_host'])]

        def float_or_none(value):
            return float(value) if value is not None else None

        self._conn_timeout = float_or_none(conf.get('sproxyd_conn_timeout'))
        self._read_timeout = float_or_none(conf.get('sproxyd_proxy_timeout'))

        self._diskfile_mgr = swift_scality_backend.diskfile.DiskFileManager(conf, self.logger)

        sp_path = \
            swift_scality_backend.policy_configuration.DEFAULT_CONFIGURATION_PATH
        self.logger.info('Reading storage policy configuration from %r', sp_path)
        try:
            with open(sp_path, 'r') as fd:
                self.logger.info('Parsing storage policy configuration')
                self._policy_configuration = \
                    swift_scality_backend.policy_configuration.Configuration.from_stream(fd)
        except IOError as exc:
            if exc.errno == errno.ENOENT:
                self.logger.info(
                    'No storage policy configuration found at %r', sp_path)
                self._policy_configuration = None
            else:
                self.logger.exception(
                    'Failure while reading storage policy configuration '
                    'from %r', sp_path)
                raise

        location_preferences = conf.get('scality_location_preferences')
        if location_preferences is None:
            self._location_preferences = None
        else:
            self._location_preferences = \
                swift_scality_backend.utils.split_list(location_preferences)

        self.logger.info('=== Begin swift_scality_backend configuration ===')
        self.logger.info(repr({
            'policy_0_urls': self._policy_0_urls,
            'conn_timeout': self._conn_timeout,
            'read_timeout': self._read_timeout,
            'policy_configuration': self._policy_configuration,
            'location_preferences': self._location_preferences,
        }))
        self.logger.info('=== End swift_scality_backend configuration ===')

    def _get_client_for_policy(self, policy_idx):
        '''Retrieve or create an Sproxyd client for a given storage policy

        :param policy_idx: Policy identifier
        :type policy_idx: `int`
        :return: Sproxyd client which can be used for requests in the given
                 policy
        :rtype: `scality_sproxyd_client.sproxyd_client.SproxydClient`

        :raise RuntimeError: No policies configured
        '''

        # Static arguments to pass to the `SproxydClient` constructor.
        # Note we can only add `conn_timeout` and `read_timeout` when configured
        # (i.e. not pass `None` to the constructor) because of the default
        # values in the constructor arguments.
        sproxyd_client_kwargs = {
            'logger': self.logger,
        }
        if self._conn_timeout is not None:
            sproxyd_client_kwargs['conn_timeout'] = self._conn_timeout
        if self._read_timeout is not None:
            sproxyd_client_kwargs['read_timeout'] = self._read_timeout

        # Set up a new client collection when we didn't create one before,
        # otherwise just return the old one
        if policy_idx not in self._clients:
            collection = None

            # 'Default' (old-style) configuration (no custom storage policy)
            if policy_idx == 0:
                endpoints = self._policy_0_urls

                client = scality_sproxyd_client.sproxyd_client.SproxydClient(
                    endpoints, **sproxyd_client_kwargs)

                collection = swift_scality_backend.http_utils.ClientCollection(
                    read_clients=[client], write_clients=[client])
            else:
                if not self._policy_configuration:
                    raise RuntimeError(
                        'No storage policy configuration found, but request '
                        'for policy %r' % policy_idx)

                policy = self._policy_configuration.get_policy(policy_idx)

                # Clients in a write set of a policy are assumed to be readable
                # as well (although potentially with a lower precedence). To
                # ensure we only create a single `SproxydClient` for a given set
                # of endpoints (to reduce the number of connections to those
                # endpoints, and not to duplicate the failure detector requests
                # sent to it), this map caches `SproxydClient`s based on their
                # set of endpoints for re-use.
                clients = {}

                # Iterate over all *read* endpoints and construct
                # `SproxydClient` instances accordingly, or re-use an existing
                # one.
                read_clients = []
                for endpoints in policy.lookup(
                        policy.READ, location_hints=self._location_preferences):
                    if endpoints in clients:
                        # This should only happen for very funky configurations
                        read_clients.append(clients[endpoints])
                    else:
                        client = scality_sproxyd_client.sproxyd_client.SproxydClient(
                            (endpoint.url for endpoint in endpoints),
                            **sproxyd_client_kwargs)
                        clients[endpoints] = client

                        read_clients.append(client)

                # Iterate over all *write* endpoints and construct
                # `SproxydClient` instances accordingly, or re-use an existing
                # one.
                write_clients = []
                for endpoints in policy.lookup(
                        policy.WRITE, location_hints=self._location_preferences):
                    if endpoints in clients:
                        write_clients.append(clients[endpoints])
                    else:
                        client = scality_sproxyd_client.sproxyd_client.SproxydClient(
                            (endpoint.url for endpoint in endpoints),
                            self._conn_timeout, self._read_timeout, self.logger)
                        clients[endpoints] = client

                        write_clients.append(client)

                collection = swift_scality_backend.http_utils.ClientCollection(
                    read_clients=read_clients, write_clients=write_clients)

            # Cache the collection for this policy
            self._clients[policy_idx] = collection

            self.logger.info(
                '=== Begin swift_scality_backend configuration for '
                'storage policy %r ===' % policy_idx)
            self.logger.info(repr(collection))
            self.logger.info(
                '=== End swift_scality_backend configuration for '
                'storage policy %r ===' % policy_idx)

        return self._clients[policy_idx]

    def get_diskfile(self, device, partition, account, container, obj,
                     policy=POLICY_STUB, **kwargs):
        """
        Utility method for instantiating a DiskFile object supporting a
        given REST API.
        """

        if 'policy_idx' in kwargs:
            if policy is POLICY_STUB:
                policy = kwargs['policy_idx']
            else:
                raise ValueError('Both `policy_idx` and `policy` provided')

        # When `policy_idx` is not set (e.g. running Swift 1.13), the fallback
        # policy 0 should be used.
        if policy is POLICY_STUB:
            policy = 0

        # In Swift 2.3 policies are no longer defined by an integer, but by a
        # value of type `swift.common.storage_policy.BaseStoragePolicy`, which
        # has an `idx` attribute.
        # We rely on the integer only, for now.
        policy = getattr(policy, 'idx', policy)

        client_collection = self._get_client_for_policy(policy)

        return self._diskfile_mgr.get_diskfile(
            client_collection, account, container, obj)

    def REPLICATE(*_args, **_kwargs):
        """Handle REPLICATE requests for the Swift Object Server.

        This is used by the object replicator to get hashes for directories.
        """
        pass


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI object server apps."""
    conf = global_conf.copy()
    conf.update(local_conf)
    return ObjectController(conf)
