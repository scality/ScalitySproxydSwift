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

"""Tests for Sproxyd Client"""

import base64
import itertools
import pickle
import socket
import unittest
import urllib
import weakref

import eventlet
import mock
import urllib3
import urllib3.exceptions

from swift_scality_backend.sproxyd_client import SproxydClient
from swift_scality_backend.exceptions import SproxydConfException, \
    SproxydHTTPException
from . import utils


class TestSproxydClient(unittest.TestCase):
    """Tests for swift_scality_backend.sproxyd_client.SproxydClient"""

    def test_init_with_default_timeout_values(self):
        sfs = SproxydClient({}, mock.Mock())
        self.assertEqual(10, sfs.conn_timeout)
        self.assertEqual(3, sfs.proxy_timeout)

    def test_init_with_custom_timeout_values(self):
        conf = {'sproxyd_conn_timeout': 42.1, 'sproxyd_proxy_timeout': 4242.1}
        sfs = SproxydClient(conf, mock.Mock())
        self.assertEqual(42.1, sfs.conn_timeout)
        self.assertEqual(4242.1, sfs.proxy_timeout)

    def test_init_base_path_has_slashes(self):
        conf = {'sproxyd_path': 'missing_slashes'}
        sfs = SproxydClient(conf, mock.Mock())
        self.assertEqual('/missing_slashes/', sfs.base_path)

    def test_init_sproxyd_hosts(self):
        # Mind the white spaces
        conf = {'sproxyd_host': ' host1:81 , host2:82 '}
        sfs = SproxydClient(conf, mock.Mock())
        expected_sproxyd_hosts_set = set([('host1', 81), ('host2', 82)])
        self.assertEqual(expected_sproxyd_hosts_set, sfs.sproxyd_hosts_set)

    def test_init_monitoring_threads(self):
        conf = {'sproxyd_host': 'host1:81,host2:82'}
        sfs = SproxydClient(conf, mock.Mock())
        self.assertEqual(2, len(sfs.healthcheck_threads))

    @mock.patch('urllib3.PoolManager.request',
                side_effect=SproxydConfException(""))
    def test_ping_with_bad_sproxyd_conf(self, request_mock):
        mock_logger = mock.Mock()
        sfs = SproxydClient({}, mock_logger)
        ping_result = sfs.ping('http://ignored')

        self.assertFalse(ping_result)
        self.assertTrue(mock_logger.warning.called)
        (msg, _, exc), _ = mock_logger.warning.call_args
        self.assertTrue(type(exc) is SproxydConfException)
        self.assertTrue("is invalid:" in msg)

    @mock.patch('urllib3.PoolManager.request', side_effect=Exception)
    def test_ping_with_unexpected_exc(self, urlopen_mock):
        mock_logger = mock.Mock()
        sfs = SproxydClient({}, mock_logger)
        ping_result = sfs.ping('http://ignored')

        self.assertFalse(ping_result)
        self.assertTrue(mock_logger.exception.called)
        (msg, _), _ = mock_logger.exception.call_args
        self.assertTrue("Unexpected" in msg)

    def test_on_sproxyd_up(self):
        sfs = SproxydClient({}, mock.Mock())
        host, port = 'host1', 81
        sfs.on_sproxyd_up(host, port)
        self.assertTrue((host, port) in sfs.sproxyd_hosts_set)
        self.assertTrue((host, port) in itertools.islice(sfs.sproxyd_hosts, 2))

    @mock.patch.object(SproxydClient, 'ping', return_value=True)
    # We need this mock because otherwise the failure detector would
    # remove localhost:81 from the list of valid sproxyd hosts
    def test_on_sproxyd_down(self, mock_ping=None):
        sfs = SproxydClient({}, mock.Mock())
        # This has to match the default value of `sproxyd_host`
        sfs.on_sproxyd_down('localhost', 81)
        self.assertFalse(('localhost', 81) in sfs.sproxyd_hosts_set)
        self.assertEqual([], list(sfs.sproxyd_hosts))

    @mock.patch('socket.socket.connect', side_effect=socket.timeout)
    def test_do_http_connection_timeout(self, mock_http_connect):
        timeout = 0.01
        sfs = SproxydClient({'sproxyd_conn_timeout': timeout}, mock.Mock())

        regex = r'^.*connect timeout=%s.*$' % timeout
        utils.assertRaisesRegexp(urllib3.exceptions.ConnectTimeoutError, regex,
                                 sfs._do_http, 'me', {}, 'HTTP_METH', '/')

    def test_do_http_timeout(self):
        server1 = eventlet.listen(('127.0.0.1', 0))
        (ip, port) = server1.getsockname()

        def run_server1(sock):
            (client, addr) = sock.accept()
            eventlet.sleep(0.1)

        t = eventlet.spawn(run_server1, server1)
        timeout = 0.01
        sfs = SproxydClient({'sproxyd_host': '%s:%d' % (ip, port),
                            'sproxyd_proxy_timeout': timeout},
                            mock.Mock())

        regex = r'^.*read timeout=%s.*$' % timeout
        utils.assertRaisesRegexp(urllib3.exceptions.ReadTimeoutError, regex,
                                 sfs._do_http, 'me', {}, 'HTTP_METH', '/')
        t.kill()

    def test_do_http_unexpected_http_status(self):
        mock_response = mock.Mock()
        mock_response.status = 500
        mock_response.read.return_value = 'error'

        sfs = SproxydClient({}, mock.Mock())
        msg = r'^caller1: %s .*' % mock_response.read.return_value
        with mock.patch('urllib3.HTTPConnectionPool.request', return_value=mock_response):
            utils.assertRaisesRegexp(SproxydHTTPException, msg, sfs._do_http,
                                     'caller1', {}, 'HTTP_METH', '/')

    @mock.patch('urllib3.HTTPConnectionPool.request',
                return_value=urllib3.response.HTTPResponse(status=200))
    def test_do_http(self, mock_http):
        mock_handler = mock.Mock()

        sfs = SproxydClient({}, mock.Mock())
        method = 'HTTP_METH'
        # Note the white space, to test proper URL encoding
        path = 'pa th'
        headers = {'k': 'v'}
        sfs._do_http('caller1', {200: mock_handler}, method, path, headers)

        mock_http.assert_called_once_with(method,
                                          sfs.base_path + urllib.quote(path),
                                          headers=headers,
                                          preload_content=False)
        mock_handler.assert_called_once_with(mock_http.return_value)

    def test_do_http_drains_connection(self):
        sfs = SproxydClient({}, mock.Mock())
        mock_response = mock.Mock()
        mock_response.status = 200
        mock_response.read.side_effect = ['blah', 'blah', '']

        handlers = {200: lambda response: None}
        with mock.patch('urllib3.HTTPConnectionPool.request', return_value=mock_response):
            sfs._do_http('caller1', handlers, 'METHOD', '/')

        self.assertEqual(3, mock_response.read.call_count)

    @mock.patch('urllib3.HTTPConnectionPool.request')
    def test_get_meta_on_200(self, mock_http):
        headers = {'x-scal-usermd': base64.b64encode(pickle.dumps('fake'))}
        mock_http.return_value = urllib3.response.HTTPResponse(status=200,
                                                               headers=headers)

        sfs = SproxydClient({}, mock.Mock())
        metadata = sfs.get_meta('object_name_1')

        mock_http.assert_called_once_with('HEAD',
                                          sfs.base_path + 'object_name_1',
                                          headers=None, preload_content=False)
        self.assertEqual('fake', metadata)

    @mock.patch('urllib3.HTTPConnectionPool.request',
                return_value=urllib3.response.HTTPResponse(status=404))
    def test_get_meta_on_404(self, mock_http):
        sfs = SproxydClient({}, mock.Mock())

        self.assertTrue(sfs.get_meta('object_name_1') is None)

    @mock.patch('urllib3.HTTPConnectionPool.request',
                return_value=urllib3.response.HTTPResponse(status=200))
    def test_put_meta(self, mock_http):
        sfs = SproxydClient({}, mock.Mock())
        sfs.put_meta('object_name_1', 'fake')

        self.assertEqual(1, mock_http.call_count)
        (method, path), kwargs = mock_http.call_args
        self.assertEqual('PUT', method)
        self.assertTrue('object_name_1' in path)
        headers = kwargs['headers']
        self.assertTrue('x-scal-cmd' in headers)
        self.assertEqual('update-usermd', headers['x-scal-cmd'])
        self.assertTrue('x-scal-usermd' in headers)
        self.assertTrue(len(headers['x-scal-usermd']) > 0)

    def test_put_meta_with_no_metadata(self):
        sfs = SproxydClient({}, mock.Mock())

        utils.assertRaisesRegexp(SproxydHTTPException, 'no usermd',
                                 sfs.put_meta, 'object_name_1', None)

    @mock.patch('urllib3.HTTPConnectionPool.request',
                return_value=urllib3.response.HTTPResponse(status=200))
    def test_del_object(self, mock_http):
        sfs = SproxydClient({}, mock.Mock())
        sfs.del_object('object_name_1')

        mock_http.assert_called_once_with('DELETE',
                                          sfs.base_path + 'object_name_1',
                                          headers=None, preload_content=False)

    def test_get_object(self):
        server = eventlet.listen(('127.0.0.1', 0))
        (ip, port) = server.getsockname()

        content = 'Hello, World!'

        def hello_world(env, start_response):
            start_response('200 OK', [('Content-Type', 'text/plain')])
            return [content]

        t = eventlet.spawn(eventlet.wsgi.server, server, hello_world)

        sfs = SproxydClient({'sproxyd_host': '%s:%d' % (ip, port)},
                            mock.Mock())
        obj = sfs.get_object('ignored')

        self.assertEqual(content, obj.next())
        # Assert that `obj` is an Iterable
        self.assertRaises(StopIteration, obj.next)
        t.kill()

    @mock.patch('eventlet.spawn')
    def test_del_instance(self, mock_spawn):
        sfs = SproxydClient({}, mock.Mock())

        # Reset mock to clear some references to bound methods
        # Otherwise reference count can never go to 0
        mock_spawn.reset_mock()

        ref = weakref.ref(sfs)
        del sfs
        if ref() is not None:
            self.skipTest("GC didn't collect our object yet")

        mock_spawn().kill.assert_called_once_with()
