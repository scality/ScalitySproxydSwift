# Copyright (c) 2014, 2015 Scality
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

"""Tests for swift_scality_backend.diskfile"""

import base64
import itertools
import httplib
import pickle
import StringIO
import unittest
import urllib
import weakref

import eventlet
import mock
import swift.common.exceptions
import swift.common.utils

from swift_scality_backend.diskfile import SproxydFileSystem, DiskFileWriter, \
    DiskFileReader, DiskFile
from swift_scality_backend.exceptions import SproxydConfException, \
    SproxydHTTPException
import utils


NEW_SPLICE = 'new_splice'
OLD_SPLICE = 'old_splice'
NO_SPLICE_AT_ALL = 'no_splice_at_all'
try:
    import swift.common.splice
    SPLICE = NEW_SPLICE
except ImportError:
    if hasattr(swift.common.utils, 'system_has_splice'):
        SPLICE = OLD_SPLICE
    else:
        SPLICE = NO_SPLICE_AT_ALL


class FakeHTTPResp(httplib.HTTPResponse):

    def __init__(self, status=200):
        self.status = status
        self.reason = 'because'
        usermd = base64.b64encode(pickle.dumps('fake'))
        headers = StringIO.StringIO('x-scal-usermd: %s' % usermd)
        self.msg = httplib.HTTPMessage(headers)

    def read(self):
        return 'My mock msg'


class FakeHTTPConn(object):

    def __init__(self, resp_status=200):
        self.resp_status = resp_status
        self._buffer = StringIO.StringIO()

    def getresponse(self):
        return FakeHTTPResp(self.resp_status)

    def send(self, data):
        self._buffer.write(data)

    def close(self):
        pass


class TestSproxydFileSystem(unittest.TestCase):
    """Tests for swift_scality_backend.diskfile.SproxydFileSystem"""

    def test_init_with_default_timeout_values(self):
        sfs = SproxydFileSystem({}, mock.Mock())
        self.assertEqual(10, sfs.conn_timeout)
        self.assertEqual(3, sfs.proxy_timeout)

    def test_init_with_custom_timeout_values(self):
        conf = {'sproxyd_conn_timeout': 42.1, 'sproxyd_proxy_timeout': 4242.1}
        sfs = SproxydFileSystem(conf, mock.Mock())
        self.assertEqual(42.1, sfs.conn_timeout)
        self.assertEqual(4242.1, sfs.proxy_timeout)

    def test_init_with_default_splice(self):
        sfs = SproxydFileSystem({}, mock.Mock())
        self.assertFalse(sfs.use_splice)

    def test_init_with_splice_no(self):
        sfs = SproxydFileSystem({'splice': 'no'}, mock.Mock())
        self.assertFalse(sfs.use_splice)

    def test_init_base_path_has_slashes(self):
        conf = {'sproxyd_path': 'missing_slashes'}
        sfs = SproxydFileSystem(conf, mock.Mock())
        self.assertEqual('/missing_slashes/', sfs.base_path)

    def test_init_sproxyd_hosts(self):
        # Mind the white spaces
        conf = {'sproxyd_host': ' host1:81 , host2:82 '}
        sfs = SproxydFileSystem(conf, mock.Mock())
        expected_sproxyd_hosts_set = set([('host1', 81), ('host2', 82)])
        self.assertEqual(expected_sproxyd_hosts_set, sfs.sproxyd_hosts_set)

    def test_init_monitoring_threads(self):
        conf = {'sproxyd_host': 'host1:81,host2:82'}
        sfs = SproxydFileSystem(conf, mock.Mock())
        self.assertEqual(2, len(sfs.healthcheck_threads))

    def _test_init_splice_unavailable(self):
        sfs = SproxydFileSystem({'splice': 'no'}, mock.Mock())
        self.assertFalse(sfs.use_splice, "Splice not wanted by conf and not " +
                         "available from system: use_splice should be False")

        mock_logger = mock.Mock()
        sfs = SproxydFileSystem({'splice': 'yes'}, mock_logger)
        self.assertFalse(sfs.use_splice, "Splice wanted by conf but not " +
                         "available from system: use_splice should be False")
        self.assertTrue(mock_logger.warn.called)

    def _test_init_splice_available(self):
        sfs = SproxydFileSystem({'splice': 'yes'}, mock.Mock())
        self.assertTrue(sfs.use_splice, "Splice wanted by conf and " +
                        "available from system: use_splice should be True")

        sfs = SproxydFileSystem({'splice': 'no'}, mock.Mock())
        self.assertFalse(sfs.use_splice, "Splice not wanted by conf though " +
                         "available from system: use_splice should be False")

    @utils.skipIf(SPLICE != NEW_SPLICE, 'Need new `splice` support')
    @mock.patch('swift.common.splice.splice')
    def test_init_new_splice_unavailable(self, mock_splice):
        type(mock_splice).available = mock.PropertyMock(return_value=False)
        self._test_init_splice_unavailable()

    @utils.skipIf(SPLICE != NEW_SPLICE, 'Need new `splice` support')
    @mock.patch('swift.common.splice.splice')
    def test_init_new_splice_is_available(self, mock_splice):
        type(mock_splice).available = mock.PropertyMock(return_value=True)
        self._test_init_splice_available()

    @utils.skipIf(SPLICE != OLD_SPLICE, 'Need old `splice` support')
    @mock.patch.object(swift.common.utils, 'system_has_splice',
                       return_value=True)
    def test_init_old_splice_is_available(self, mock_splice):
        self._test_init_splice_available()

    @utils.skipIf(SPLICE != OLD_SPLICE, 'Need old `splice` support')
    @mock.patch.object(swift.common.utils, 'system_has_splice',
                       return_value=False)
    def test_init_old_splice_unavailable(self, mock_splice):
        self._test_init_splice_unavailable()

    @utils.skipIf(SPLICE != NO_SPLICE_AT_ALL, 'This Swift knows `splice`')
    def test_init_no_splice_at_all(self):
        self._test_init_splice_unavailable()

    @mock.patch.object(urllib, 'urlopen')
    @mock.patch('swift_scality_backend.utils.is_sproxyd_conf_valid',
                side_effect=SproxydConfException(""))
    def test_ping_with_bad_sproxyd_conf(self, conf_checker_mock, urlopen_mock):
        mock_logger = mock.Mock()
        sfs = SproxydFileSystem({}, mock_logger)
        ping_result = sfs.ping('http://ignored')

        self.assertFalse(ping_result)
        self.assertTrue(mock_logger.warning.called)
        (msg, _, exc), _ = mock_logger.warning.call_args
        self.assertTrue(type(exc) is SproxydConfException)
        self.assertTrue("is invalid:" in msg)

    @mock.patch.object(urllib, 'urlopen', side_effect=Exception)
    def test_ping_with_unexpected_exc(self, urlopen_mock):
        mock_logger = mock.Mock()
        sfs = SproxydFileSystem({}, mock_logger)
        ping_result = sfs.ping('http://ignored')

        self.assertFalse(ping_result)
        self.assertTrue(mock_logger.exception.called)
        (msg, _), _ = mock_logger.exception.call_args
        self.assertTrue("Unexpected" in msg)

    def test_on_sproxyd_up(self):
        sfs = SproxydFileSystem({}, mock.Mock())
        host, port = 'host1', 81
        sfs.on_sproxyd_up(host, port)
        self.assertTrue((host, port) in sfs.sproxyd_hosts_set)
        self.assertTrue((host, port) in itertools.islice(sfs.sproxyd_hosts, 2))

    @mock.patch.object(SproxydFileSystem, 'ping', return_value=True)
    # We need this mock because otherwise the failure detector would
    # remove localhost:81 from the list of valid sproxyd hosts
    def test_on_sproxyd_down(self, mock_ping=None):
        sfs = SproxydFileSystem({}, mock.Mock())
        # This has to match the default value of `sproxyd_host`
        sfs.on_sproxyd_down('localhost', 81)
        self.assertFalse(('localhost', 81) in sfs.sproxyd_hosts_set)
        self.assertEqual([], list(sfs.sproxyd_hosts))

    @mock.patch('swift.common.bufferedhttp.http_connect_raw',
                side_effect=lambda *args: eventlet.sleep(0.02))
    def test_do_http_connection_timeout(self, mock_http_connect):
        sfs = SproxydFileSystem({'sproxyd_conn_timeout': 0.01}, mock.Mock())
        self.assertRaises(swift.common.exceptions.ConnectionTimeout,
                          sfs._do_http, 'me', {}, 'HTTP_METH', '/')

    def test_do_http_timeout(self):
        class HTTPConn(object):

            def getresponse(self):
                eventlet.sleep(0.02)

            def close(self):
                pass

        sfs = SproxydFileSystem({'sproxyd_proxy_timeout': 0.01}, mock.Mock())

        with mock.patch('swift.common.bufferedhttp.http_connect_raw',
                        return_value=HTTPConn()):
            self.assertRaises(eventlet.Timeout, sfs._do_http, 'me',
                              {}, 'HTTP_METH', '/')

    @mock.patch('swift.common.bufferedhttp.http_connect_raw',
                return_value=FakeHTTPConn())
    def test_do_http_unexpected_http_status(self, mock_http):
        sfs = SproxydFileSystem({}, mock.Mock())

        msg = r'caller1: %s .*' % mock_http.return_value.getresponse().read()
        utils.assertRaisesRegexp(SproxydHTTPException, msg, sfs._do_http,
                                 'caller1', {}, 'HTTP_METH', '/')

    @mock.patch('swift.common.bufferedhttp.http_connect_raw',
                return_value=FakeHTTPConn())
    def test_do_http(self, mock_http):
        mock_handler = mock.Mock()

        sfs = SproxydFileSystem({}, mock.Mock())
        method = 'HTTP_METH'
        # Note the white space, to test proper URL encoding
        path = 'pa th'
        headers = {'k': 'v'}
        sfs._do_http('caller1', {200: mock_handler}, method, path, headers)

        mock_http.assert_called_with(mock.ANY, mock.ANY, method,
                                     sfs.base_path + urllib.quote(path),
                                     headers)
        call_args = mock_handler.call_args
        self.assertTrue(call_args is not None)
        expected_arg = mock_http.return_value.getresponse()
        self.assertTrue(isinstance(call_args[0][0], type(expected_arg)))

    @mock.patch('swift.common.bufferedhttp.http_connect_raw',
                return_value=FakeHTTPConn())
    def test_get_meta(self, mock_http):
        sfs = SproxydFileSystem({}, mock.Mock())
        metadata = sfs.get_meta('object_name_1')

        self.assertEqual(1, mock_http.call_count)
        mock_http.assert_called_with(mock.ANY, mock.ANY, 'HEAD',
                                     sfs.base_path + 'object_name_1',
                                     None)
        self.assertEqual('fake', metadata)

    @mock.patch('swift.common.bufferedhttp.http_connect_raw',
                return_value=FakeHTTPConn())
    def test_put_meta(self, mock_http):
        sfs = SproxydFileSystem({}, mock.Mock())
        sfs.put_meta('object_name_1', 'fake')

        self.assertEqual(1, mock_http.call_count)
        (ip, port, method, path, headers), kwargs = mock_http.call_args
        self.assertEqual('PUT', method)
        self.assertTrue('object_name_1' in path)
        self.assertTrue('x-scal-cmd' in headers)
        self.assertEqual('update-usermd', headers['x-scal-cmd'])
        self.assertTrue('x-scal-usermd' in headers)
        self.assertTrue(len(headers['x-scal-usermd']) > 0)

    def test_put_meta_with_no_metadata(self):
        sfs = SproxydFileSystem({}, mock.Mock())

        utils.assertRaisesRegexp(SproxydHTTPException, 'no usermd',
                                 sfs.put_meta, 'object_name_1', None)

    @mock.patch('swift.common.bufferedhttp.http_connect_raw',
                return_value=FakeHTTPConn())
    def test_del_object(self, mock_http):
        sfs = SproxydFileSystem({}, mock.Mock())
        sfs.del_object('object_name_1')

        self.assertEqual(1, mock_http.call_count)
        mock_http.assert_called_with(mock.ANY, mock.ANY, 'DELETE',
                                     sfs.base_path + 'object_name_1',
                                     None)

    @mock.patch('eventlet.spawn')
    def test_del_instance(self, mock_spawn):
        sfs = SproxydFileSystem({}, mock.Mock())

        # Reset mock to clear some references to bound methods
        # Otherwise reference count can never go to 0
        mock_spawn.reset_mock()

        ref = weakref.ref(sfs)
        del sfs
        if ref() is not None:
            self.skipTest("GC didn't collect our object yet")

        mock_spawn().kill.assert_called_once_with()


class TestDiskFileWriter(unittest.TestCase):
    """Tests for swift_scality_backend.diskfile.DiskFileWriter"""

    @mock.patch('swift.common.bufferedhttp.http_connect_raw',
                return_value=FakeHTTPConn())
    def test_init(self, mock_http):
        sfs = SproxydFileSystem({}, mock.Mock())
        # Note the white space, to test proper URL encoding
        DiskFileWriter(sfs, 'ob j')

        self.assertEqual(1, mock_http.call_count)
        expected_header = {'transfer-encoding': 'chunked'}
        mock_http.assert_called_with(mock.ANY, mock.ANY, 'PUT',
                                     sfs.base_path + urllib.quote('ob j'),
                                     expected_header)

    @mock.patch('swift.common.bufferedhttp.http_connect_raw',
                return_value=FakeHTTPConn(404))
    def test_put_with_404_response(self, mock_http):
        sfs = SproxydFileSystem({}, mock.Mock())
        dfw = DiskFileWriter(sfs, 'obj')

        msg = r'.*404 / %s.*' % mock_http.return_value.getresponse().read()
        utils.assertRaisesRegexp(SproxydHTTPException, msg, dfw.put, {})

    @mock.patch('swift.common.bufferedhttp.http_connect_raw',
                return_value=FakeHTTPConn(200))
    @mock.patch('swift_scality_backend.diskfile.SproxydFileSystem.put_meta')
    def test_put_with_200_response(self, mock_put_meta, mock_http):
        sfs = SproxydFileSystem({}, mock.Mock())
        dfw = DiskFileWriter(sfs, 'obj')

        dfw.put({})

        mock_put_meta.assert_called_with('obj', {'name': 'obj'})


class TestDiskFile(unittest.TestCase):
    """Tests for swift_scality_backend.diskfile.DiskFile"""

    @mock.patch('swift_scality_backend.diskfile.SproxydFileSystem.get_meta',
                return_value=None)
    def test_open_when_no_metadata(self, mock_get_meta):
        sfs = SproxydFileSystem({}, mock.Mock())
        df = DiskFile(sfs, 'a', 'c', 'o')

        self.assertRaises(swift.common.exceptions.DiskFileDeleted, df.open)
        mock_get_meta.assert_called_once_with('a/c/o')

    @mock.patch('swift_scality_backend.diskfile.SproxydFileSystem.get_meta',
                return_value={'name': 'o'})
    def test_open(self, mock_get_meta):
        sfs = SproxydFileSystem({}, mock.Mock())
        df = DiskFile(sfs, 'a', 'c', 'o')

        df.open()

        self.assertEqual({'name': 'o'}, df._metadata)

    def test_get_metadata_when_diskfile_not_open(self):
        sfs = SproxydFileSystem({}, mock.Mock())
        df = DiskFile(sfs, 'a', 'c', 'o')

        self.assertRaises(swift.common.exceptions.DiskFileNotOpen,
                          df.get_metadata)

    @mock.patch('swift_scality_backend.diskfile.SproxydFileSystem.get_meta',
                return_value={'name': 'o'})
    def test_read_metadata(self, mock_get_meta):
        sfs = SproxydFileSystem({}, mock.Mock())
        df = DiskFile(sfs, 'a', 'c', 'o')

        metadata = df.read_metadata()

        self.assertEqual({'name': 'o'}, metadata)

    def test_reader(self):
        sfs = SproxydFileSystem({}, mock.Mock())
        df = DiskFile(sfs, 'a', 'c', 'o')

        reader = df.reader()
        self.assertTrue(isinstance(reader, DiskFileReader))

    @mock.patch('swift.common.bufferedhttp.http_connect_raw',
                return_value=FakeHTTPConn())
    def test_create(self, mock_http):
        sfs = SproxydFileSystem({}, mock.Mock())
        df = DiskFile(sfs, 'a', 'c', 'o')

        with df.create() as writer:
            self.assertTrue(isinstance(writer, DiskFileWriter))

    @mock.patch('swift_scality_backend.diskfile.SproxydFileSystem.put_meta')
    def test_write_metadata(self, mock_put_meta):
        sfs = SproxydFileSystem({}, mock.Mock())
        df = DiskFile(sfs, 'a', 'c', 'o')

        df.write_metadata({'k': 'v'})

        self.assertEqual(1, mock_put_meta.call_count)
        mock_put_meta.assert_called_with('a/c/o', {'k': 'v'})

    @mock.patch('swift_scality_backend.diskfile.SproxydFileSystem.del_object')
    def test_delete(self, mock_del_object):
        sfs = SproxydFileSystem({}, mock.Mock())
        df = DiskFile(sfs, 'a', 'c', 'o')

        df.delete("ignored")

        self.assertEqual(1, mock_del_object.call_count)
        mock_del_object.assert_called_with('a/c/o')

    @utils.skipIf(not hasattr(swift.common.utils, 'Timestamp'), 'Swift2+ only')
    def test_timestamps_when_no_metadata(self):
        sfs = SproxydFileSystem({}, mock.Mock())
        df = DiskFile(sfs, 'a', 'c', 'o')

        # assertRaises expects a `callable`, but `timestamp` is a property
        # We could use the `with self.assertRaises(exc):` form but that's
        # Python 2.7+ only
        self.assertRaises(swift.common.exceptions.DiskFileNotOpen,
                          lambda: df.timestamp)
        self.assertRaises(swift.common.exceptions.DiskFileNotOpen,
                          lambda: df.data_timestamp)

    @utils.skipIf(not hasattr(swift.common.utils, 'Timestamp'), 'Swift2+ only')
    @mock.patch('swift.common.utils.Timestamp')
    def test_timestamp(self, mock_timestamp):
        sfs = SproxydFileSystem({}, mock.Mock())
        df = DiskFile(sfs, 'a', 'c', 'o')

        df._metadata = mock.Mock()
        df.timestamp

        df._metadata.get.assert_called_once_with('X-Timestamp')
        mock_timestamp.assert_called_once_with(df._metadata.get())


def test_ping_when_network_exception_is_raised():

    def assert_ping_failed(expected_exc):
        logger = mock.Mock()
        filesystem = SproxydFileSystem({}, logger)

        with mock.patch.object(urllib, 'urlopen', side_effect=expected_exc):
            ping_result = filesystem.ping('http://ignored/')

            assert ping_result is False, ('Ping returned %r, '
                                          'not False' % ping_result)
            assert logger.info.called
            (msg, _, exc), _ = logger.info.call_args
            assert type(exc) is expected_exc
            assert "network error" in msg

    for exc in [eventlet.Timeout, httplib.HTTPException, IOError]:
        yield assert_ping_failed, exc
