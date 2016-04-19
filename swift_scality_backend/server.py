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

import os

import eventlet

import swift.common.bufferedhttp
import swift.common.exceptions
import swift.common.http
from swift import gettext_ as _
import swift.obj.server

import swift_scality_backend.diskfile


POLICY_STUB = object()


class ObjectController(swift.obj.server.ObjectController):
    """Implements the WSGI application for the Scality Object Server."""

    def setup(self, conf):
        """Class setup

        :param conf: WSGI configuration parameter
        """
        self._filesystem = swift_scality_backend.diskfile.SproxydFileSystem(conf, self.logger)

    def get_diskfile(self, device, partition, account, container, obj,
                     policy=POLICY_STUB, **kwargs):
        """Utility method for instantiating a DiskFile object supporting a
        given REST API.
        """
        return self._filesystem.get_diskfile(account, container, obj, **kwargs)

    def async_update(self, op, account, container, obj, host, partition,
                     contdevice, headers_out, objdevice,
                     policy=POLICY_STUB,
                     logger_thread_locals=None):
        """Sends or saves an async update.

        :param op: operation performed (ex: 'PUT', or 'DELETE')
        :param account: account name for the object
        :param container: container name for the object
        :param obj: object name
        :param host: host that the container is on
        :param partition: partition that the container is on
        :param contdevice: device name that the container is on
        :param headers_out: dictionary of headers to send in the container
                            request
        :param objdevice: device name that the object is in
        :param policy: the associated BaseStoragePolicy instance OR the
                       associated storage policy index (depends on the Swift
                       version)
        :param logger_thread_locals: The thread local values to be set on the
                                     self.logger to retain transaction
                                     logging information.
        """
        if logger_thread_locals:
            self.logger.thread_locals = logger_thread_locals
        headers_out['user-agent'] = 'obj-server %s' % os.getpid()
        full_path = '/%s/%s/%s' % (account, container, obj)
        if all([host, partition, contdevice]):
            try:
                with swift.common.exceptions.ConnectionTimeout(self.conn_timeout):
                    ip, port = host.rsplit(':', 1)
                    conn = swift.common.bufferedhttp.http_connect(ip, port,
                                                                  contdevice, partition, op,
                                                                  full_path, headers_out)
                with eventlet.Timeout(self.node_timeout):
                    response = conn.getresponse()
                    response.read()
                    if swift.common.http.is_success(response.status):
                        return
                    else:
                        self.logger.error(_(
                            'ERROR Container update failed: %(status)d '
                            'response from %(ip)s:%(port)s/%(dev)s'),
                            {'status': response.status, 'ip': ip, 'port': port,
                             'dev': contdevice})
            except Exception:
                self.logger.exception(_(
                    'ERROR container update failed with '
                    '%(ip)s:%(port)s/%(dev)s'),
                    {'ip': ip, 'port': port, 'dev': contdevice})
        # FIXME: For now don't handle async updates

    def REPLICATE(ctrl=None, *_args, **_kwargs):
        """Handle REPLICATE requests for the Swift Object Server.

        This is used by the object replicator to get hashes for directories.
        """
        pass


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI object server apps."""
    conf = global_conf.copy()
    conf.update(local_conf)
    return ObjectController(conf)
