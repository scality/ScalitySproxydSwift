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
import urlparse
import weakref

import eventlet
import eventlet.wsgi
import mock
import urllib3
import urllib3.exceptions

from scality_sproxyd_client.sproxyd_client import SproxydClient
from scality_sproxyd_client.exceptions import SproxydConfException, \
    SproxydHTTPException
from . import utils
from .utils import make_sproxyd_client


class TestSproxydClient(unittest.TestCase):
    """Tests for scality_sproxyd_client.sproxyd_client.SproxydClient"""

    @mock.patch('eventlet.spawn')
    def test_init_with_default_timeout_values(self, _):
        sfs = make_sproxyd_client()
        self.assertEqual(10, sfs.conn_timeout)
        self.assertEqual(3, sfs._proxy_timeout)

    @mock.patch('eventlet.spawn')
    def test_init_with_custom_timeout_values(self, _):
        conf = {'conn_timeout': 42.1, 'proxy_timeout': 4242.1}
        sfs = make_sproxyd_client(**conf)
        self.assertEqual(42.1, sfs.conn_timeout)
        self.assertEqual(4242.1, sfs._proxy_timeout)

    @mock.patch('eventlet.spawn')
    def test_init_sproxyd_hosts(self, _):
        # Mind the white spaces
        conf = {
            'endpoints': [
                urlparse.urlparse('http://host1:81'),
                urlparse.urlparse('http://host2:81'),
            ],
        }
        sfs = make_sproxyd_client(**conf)
        expected_endpoints = frozenset([
            urlparse.urlparse('http://host1:81'),
            urlparse.urlparse('http://host2:81'),
        ])

        self.assertEqual(expected_endpoints, sfs.endpoints)

    @mock.patch('eventlet.spawn')
    def test_init_monitoring_threads(self, _):
        conf = {
            'endpoints': [
                urlparse.urlparse('http://host1:81'),
                urlparse.urlparse('http://host2:82'),
            ],
        }
        sfs = make_sproxyd_client(**conf)
        self.assertEqual(2, len(sfs._healthcheck_threads))

    @mock.patch('eventlet.spawn')
    @mock.patch('urllib3.PoolManager.request',
                side_effect=SproxydConfException(""))
    def test_ping_with_bad_sproxyd_conf(self, request_mock, _):
        mock_logger = mock.Mock()
        sfs = make_sproxyd_client(logger=mock_logger)
        ping_result = sfs._ping('http://ignored')

        self.assertFalse(ping_result)
        self.assertTrue(mock_logger.warning.called)
        (msg, _, exc), _ = mock_logger.warning.call_args
        self.assertTrue(type(exc) is SproxydConfException)
        self.assertTrue("is invalid:" in msg)

    @mock.patch('eventlet.spawn')
    @mock.patch('urllib3.PoolManager.request', side_effect=Exception)
    def test_ping_with_unexpected_exc(self, urlopen_mock, _):
        mock_logger = mock.Mock()
        sfs = make_sproxyd_client(logger=mock_logger)
        ping_result = sfs._ping('http://ignored')

        self.assertFalse(ping_result)
        self.assertTrue(mock_logger.exception.called)
        (msg, _), _ = mock_logger.exception.call_args
        self.assertTrue("Unexpected" in msg)

    @mock.patch('eventlet.spawn')
    def test_on_sproxyd_up(self, _):
        sfs = make_sproxyd_client()
        endpoint = urlparse.urlparse('http://host1:81')
        sfs._on_sproxyd_up(endpoint)
        self.assertTrue(endpoint in sfs._alive)
        self.assertTrue(endpoint in itertools.islice(sfs._cycle, 2))

    @mock.patch('eventlet.spawn')
    @mock.patch.object(SproxydClient, '_ping', return_value=True)
    # We need this mock because otherwise the failure detector would
    # remove localhost:81 from the list of valid sproxyd hosts
    def test_on_sproxyd_down(self, mock_ping, _):
        sfs = make_sproxyd_client()
        # This has to match the default value of `sproxyd_host`
        endpoint = urlparse.urlparse('http://localhost:81/proxy/chord/')
        sfs._on_sproxyd_down(endpoint)
        self.assertFalse(endpoint in sfs._alive)
        self.assertEqual([], list(sfs._alive))

    @mock.patch('eventlet.spawn')
    @mock.patch('socket.socket.connect', side_effect=socket.timeout)
    def test_do_http_connection_timeout(self, mock_http_connect, _):
        timeout = 0.01
        sfs = make_sproxyd_client(conn_timeout=timeout)

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
        with mock.patch('eventlet.spawn'):
            conf = {
                'endpoints': [
                    urlparse.urlparse('http://%s:%d' % (ip, port)),
                ],
                'proxy_timeout': timeout,
            }
            sfs = make_sproxyd_client(**conf)

        regex = r'^.*read timeout=%s.*$' % timeout
        utils.assertRaisesRegexp(urllib3.exceptions.ReadTimeoutError, regex,
                                 sfs._do_http, 'me', {}, 'HTTP_METH', '/')
        t.kill()

    @mock.patch('eventlet.spawn')
    def test_do_http_unexpected_http_status(self, _):
        mock_response = mock.Mock()
        mock_response.status = 500
        mock_response.read.return_value = 'error'

        sfs = make_sproxyd_client()
        msg = r'^caller1: %s .*' % mock_response.read.return_value
        with mock.patch('urllib3.HTTPConnectionPool.request', return_value=mock_response):
            utils.assertRaisesRegexp(SproxydHTTPException, msg, sfs._do_http,
                                     'caller1', {}, 'HTTP_METH', '/')

    @mock.patch('eventlet.spawn')
    @mock.patch('urllib3.HTTPConnectionPool.request',
                return_value=urllib3.response.HTTPResponse(status=200))
    def test_do_http(self, mock_http, _):
        mock_handler = mock.Mock()

        sfs = make_sproxyd_client()
        method = 'HTTP_METH'
        # Note the white space, to test proper URL encoding
        path = 'pa th'
        headers = {'k': 'v'}
        sfs._do_http('caller1', {200: mock_handler}, method, path, headers)

        mock_http.assert_called_once_with(method,
                                          '/proxy/chord/' + urllib.quote(path),
                                          headers=headers,
                                          preload_content=False)
        mock_handler.assert_called_once_with(mock_http.return_value)

    @mock.patch('eventlet.spawn')
    def test_do_http_drains_connection(self, _):
        sfs = make_sproxyd_client()
        mock_response = mock.Mock()
        mock_response.status = 200
        mock_response.read.side_effect = ['blah', 'blah', '']

        handlers = {200: lambda response: None}
        with mock.patch('urllib3.HTTPConnectionPool.request', return_value=mock_response):
            sfs._do_http('caller1', handlers, 'METHOD', '/')

        self.assertEqual(3, mock_response.read.call_count)

    @mock.patch('eventlet.spawn')
    @mock.patch('urllib3.HTTPConnectionPool.request')
    def test_get_meta_on_200(self, mock_http, _):
        headers = {'x-scal-usermd': base64.b64encode(pickle.dumps('fake'))}
        mock_http.return_value = urllib3.response.HTTPResponse(status=200,
                                                               headers=headers)

        sfs = make_sproxyd_client(
            endpoints=[urlparse.urlparse('http://host/base/')])
        metadata = sfs.get_meta('object_name_1')

        mock_http.assert_called_once_with('HEAD',
                                          '/base/object_name_1',
                                          headers=None, preload_content=False)
        self.assertEqual('fake', metadata)

    @mock.patch('eventlet.spawn')
    @mock.patch('urllib3.HTTPConnectionPool.request',
                return_value=urllib3.response.HTTPResponse(status=404))
    def test_get_meta_on_404(self, mock_http, _):
        sfs = make_sproxyd_client()

        self.assertTrue(sfs.get_meta('object_name_1') is None)

    @mock.patch('eventlet.spawn')
    @mock.patch('urllib3.HTTPConnectionPool.request',
                return_value=urllib3.response.HTTPResponse(status=200))
    def test_put_meta(self, mock_http, _):
        sfs = make_sproxyd_client()
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

    @mock.patch('eventlet.spawn')
    def test_put_meta_with_no_metadata(self, _):
        sfs = make_sproxyd_client()

        utils.assertRaisesRegexp(SproxydHTTPException, 'no usermd',
                                 sfs.put_meta, 'object_name_1', None)

    @mock.patch('eventlet.spawn')
    @mock.patch('urllib3.HTTPConnectionPool.request',
                return_value=urllib3.response.HTTPResponse(status=200))
    def test_del_object(self, mock_http, _):
        sfs = make_sproxyd_client()
        sfs.del_object('object_name_1')

        mock_http.assert_called_once_with('DELETE',
                                          '/proxy/chord/object_name_1',
                                          headers=None, preload_content=False)

    def test_get_object(self):
        server = eventlet.listen(('127.0.0.1', 0))
        (ip, port) = server.getsockname()

        content = 'Hello, World!'

        def hello_world(env, start_response):
            start_response('200 OK', [('Content-Type', 'text/plain')])
            return [content]

        t = eventlet.spawn(eventlet.wsgi.server, server, hello_world)

        with mock.patch('eventlet.spawn'):
            endpoint = urlparse.urlparse('http://%s:%d' % (ip, port))
            sfs = make_sproxyd_client(endpoints=[endpoint])

        obj = sfs.get_object('ignored')

        self.assertEqual(content, obj.next())
        # Assert that `obj` is an Iterable
        self.assertRaises(StopIteration, obj.next)
        t.kill()

    @mock.patch('eventlet.spawn')
    def test_del_instance(self, mock_spawn):
        sfs = make_sproxyd_client()

        # Reset mock to clear some references to bound methods
        # Otherwise reference count can never go to 0
        mock_spawn.reset_mock()

        ref = weakref.ref(sfs)
        del sfs
        if ref() is not None:
            self.skipTest("GC didn't collect our object yet")

        mock_spawn().kill.assert_called_once_with()

    @mock.patch('eventlet.spawn')
    def test_has_alive_endpoints(self, _):
        sfs = make_sproxyd_client()

        self.assertTrue(sfs.has_alive_endpoints)

        sfs._on_sproxyd_down(
            urlparse.urlparse('http://localhost:81/proxy/chord/'))

        self.assertFalse(sfs.has_alive_endpoints)
