# Copyright (c) 2015 Scality
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

import base64
import contextlib
import errno
import gzip
import httplib
import json
import os
import os.path
import pickle
import shutil
import tempfile
import time
import unittest

import eventlet

import swift_scality_backend.server

import swift.account.server
import swift.common.ring
import swift.common.utils
import swift.container.server
import swift.proxy.server

# Pre storage-policy compatibility
try:
    import swift.common.storage_policy
    HAS_STORAGE_POLICY = True
except ImportError:
    HAS_STORAGE_POLICY = False


class TestRangeHeaders(unittest.TestCase):
    """Test outgoing range headers to sproxyd from the object server on a partial
    get of an object. See https://github.com/scality/ScalitySproxydSwift/issues/121
    for more details.
    """

    def setUp(self):
        if HAS_STORAGE_POLICY:
            # Clear out any previously set storage policies
            swift.common.storage_policy.reload_storage_policies()

        # Test app servers and associated sockets
        self.sockets = []
        self.servers = []

        # Bind proxy, account, container, and object server to a free port
        bind_address = ('127.0.0.1', 0)

        # Directory holding account, container, and object rings
        self.swift_dir = tempfile.mkdtemp()

        # Setup mocked sproxyd server
        sproxyd_socket = eventlet.listen(bind_address)
        self.sockets.append(sproxyd_socket)
        sproxyd = eventlet.spawn(eventlet.wsgi.server, sproxyd_socket,
                                 self.sproxyd_mock)
        self.servers.append(sproxyd)

        self.sproxyd_path = '/proxy/chord'
        conf = {
            'swift_dir': self.swift_dir,
            'devices': self.swift_dir,
            'mount_check': 'false',
            # Scality sproxyd config options
            'sproxyd_host': '%s:%d' % sproxyd_socket.getsockname(),
            'sproxyd_path': self.sproxyd_path,
        }

        # Hashes for the ring hashing algorithm when determining data placement
        swift.common.utils.HASH_PATH_SUFFIX = 'foo'
        swift.common.utils.HASH_PATH_PREFIX = 'bar'

        # Start swift proxy with auxiliary components.
        controllers = (
            ('account', swift.account.server.AccountController(conf)),
            ('container', swift.container.server.ContainerController(conf)),
            ('object', swift_scality_backend.server.ObjectController(conf)),
        )
        for server_name, app in controllers:
            self.setup_ring(server_name, app, bind_address)

        # Setup swift proxy
        proxy_socket = eventlet.listen(bind_address)
        self.sockets.append(proxy_socket)
        proxy = swift.proxy.server.Application(conf)
        proxy_server = eventlet.spawn(eventlet.wsgi.server, proxy_socket, proxy)
        self.servers.append(proxy_server)

        # Create test account
        account = 'a'
        partition, acc_nodes = proxy.account_ring.get_nodes(account)
        acc_node = acc_nodes.pop()
        acc_path = '/%s/%d/%s' % (acc_node['device'], partition, account)
        acc_headers = {
            'X-Timestamp': swift.common.utils.normalize_timestamp(time.time()),
        }
        acc_conn = httplib.HTTPConnection(acc_node['ip'], acc_node['port'])
        acc_conn.request('PUT', acc_path, headers=acc_headers)
        if acc_conn.getresponse().status != 201:
            raise Exception("Unable to setup test account")

        # Create test container
        self.proxy_host, self.proxy_port = proxy_socket.getsockname()
        proxy_conn = httplib.HTTPConnection(self.proxy_host, self.proxy_port)
        proxy_conn.request('PUT', '/v1/a/c/')
        if proxy_conn.getresponse().status != 201:
            raise Exception("Unable to setup test container")

    def tearDown(self):
        # Reset monkey patched hash paths
        swift.common.utils.HASH_PATH_SUFFIX = ''
        swift.common.utils.HASH_PATH_PREFIX = ''

        # Stop wsgi servers
        for wsgi_server in self.servers:
            wsgi_server.kill()

        for socket in self.sockets:
            socket.close()

        # Clean-up object rings
        shutil.rmtree(self.swift_dir, ignore_errors=1)

    def _get(self, path, headers):
        """Send a HTTP GET request to the swift test proxy.

        :param path: Request path
        :type path: str
        :param headers: Request headers
        :return: A tuple with response status, headers, and body.
        """
        proxy_conn = httplib.HTTPConnection(self.proxy_host, self.proxy_port)
        proxy_conn.request('GET', path, headers=headers)
        r = proxy_conn.getresponse()

        return r.status, dict(r.getheaders()), r.read()

    def test_range_from_byte(self):
        self.response = '_' * 100

        # Get last 95 bytes
        headers = {'Range': 'bytes=5-'}
        status, response_headers, _ = self._get('/v1/a/c/o', headers)
        self.assertEqual(status, 206)
        self.assertEqual(self.sproxyd_request_headers['RANGE'], 'bytes=5-99')
        self.assertEqual(response_headers['content-range'], 'bytes 5-99/100')
        self.assertEqual(response_headers['content-length'], '95')

    def test_range_last_bytes(self):
        self.response = '_' * 100

        # Get last 5 bytes
        headers = {'Range': 'bytes=-5'}
        status, response_headers, _ = self._get('/v1/a/c/o', headers)
        self.assertEqual(status, 206)
        self.assertEqual(self.sproxyd_request_headers['RANGE'], 'bytes=95-99')
        self.assertEqual(response_headers['content-range'], 'bytes 95-99/100')
        self.assertEqual(response_headers['content-length'], '5')

    def test_range_explicit_first_bytes(self):
        self.response = '_' * 100

        # Get first 5 bytes
        headers = {'Range': 'bytes=0-4'}
        status, response_headers, _ = self._get('/v1/a/c/o', headers)
        self.assertEqual(status, 206)
        self.assertEqual(self.sproxyd_request_headers['RANGE'], 'bytes=0-4')
        self.assertEqual(response_headers['content-range'], 'bytes 0-4/100')
        self.assertEqual(response_headers['content-length'], '5')

    def test_range_explicit_intermediate(self):
        self.response = '_' * 100

        headers = {'Range': 'bytes=20-40'}
        status, response_headers, _ = self._get('/v1/a/c/o', headers)
        self.assertEqual(status, 206)
        self.assertEqual(self.sproxyd_request_headers['RANGE'], 'bytes=20-40')
        self.assertEqual(response_headers['content-range'], 'bytes 20-40/100')
        self.assertEqual(response_headers['content-length'], '21')

    def test_range_explicit_end(self):
        self.response = '_' * 100

        # Get last 5 bytes
        headers = {'Range': 'bytes=95-99'}
        status, response_headers, _ = self._get('/v1/a/c/o', headers)
        self.assertEqual(status, 206)
        self.assertEqual(self.sproxyd_request_headers['RANGE'], 'bytes=95-99')
        self.assertEqual(response_headers['content-range'], 'bytes 95-99/100')
        self.assertEqual(response_headers['content-length'], '5')

    def test_range_exceed_length(self):
        self.response = '_' * 100

        # Exceed object length by one
        headers = {'Range': 'bytes=95-100'}
        status, response_headers, _ = self._get('/v1/a/c/o', headers)
        self.assertEqual(status, 206)
        self.assertEqual(self.sproxyd_request_headers['RANGE'], 'bytes=95-99')
        self.assertEqual(response_headers['content-range'], 'bytes 95-99/100')
        self.assertEqual(response_headers['content-length'], '5')

    def test_range_first_byte(self):
        self.response = '_' * 100

        # Get first byte
        headers = {'Range': 'bytes=0-0'}
        status, response_headers, _ = self._get('/v1/a/c/o', headers)
        self.assertEqual(status, 206)
        self.assertEqual(self.sproxyd_request_headers['RANGE'], 'bytes=0-0')
        self.assertEqual(response_headers['content-range'], 'bytes 0-0/100')
        self.assertEqual(response_headers['content-length'], '1')

    def setup_ring(self, server_name, app, bind_address):
        """Setup a single replica ring."""
        # Device definition
        server_socket = eventlet.listen(bind_address)
        self.sockets.append(server_socket)
        server_ip, server_port = server_socket.getsockname()
        dev = {
            'id': 0,
            'zone': 0,
            'device': 'sda1',
            'ip': server_ip,
            'port': server_port,
        }

        # Prepare ring directory
        try:
            os.makedirs(os.path.join(self.swift_dir, dev['device']))
        except OSError as e:
            # Ignore if directory already exists
            if e.errno != errno.EEXIST:
                raise

        # Compose ring metadata
        replica2part2dev = [[0, 0, 0, 0]]
        part_shift = 30
        ring = swift.common.ring.RingData(replica2part2dev, [dev], part_shift)
        path = '%s/%s.ring.gz' % (self.swift_dir, server_name)
        with contextlib.closing(gzip.GzipFile(path, 'wb')) as f:
            pickle.dump(ring, f)

        wsgi_server = eventlet.spawn(eventlet.wsgi.server, server_socket, app)
        self.servers.append(wsgi_server)

    def sproxyd_mock(self, env, start_response):
        method = env['REQUEST_METHOD']
        path = env['PATH_INFO']

        if method == 'GET' and path == '%s/.conf' % self.sproxyd_path:
            # Return configuration parameter expected by failure detector
            response = json.dumps({
                'by_path_enabled': True,
            }, indent=2)
        else:
            response = self.response

        # Expected metadata (xattr) headers by swift
        metadata = {
            'X-Timestamp': swift.common.utils.normalize_timestamp(time.time()),
            'Content-Length': len(response),
            'ETag': 'some_hash',
        }

        headers = {
            'Content-Type': 'application/json',
            'X-Scal-Usermd': base64.b64encode(pickle.dumps(metadata)),
            'Content-Length': len(response),
        }

        self.sproxyd_request_headers = dict(
            (k[len('HTTP_'):], v)
            for k, v in env.iteritems()
            if k.startswith('HTTP_')
        )

        start_response('200 OK', headers.items())
        return [response]
