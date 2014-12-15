# Copyright (c) 2010-2012 OpenStack Foundation
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

import httplib
import logging
import unittest
import urllib

import eventlet
import mock
import swift.common.exceptions

import swift_scality_backend.diskfile
import swift_scality_backend.utils
from swift_scality_backend.exceptions import SproxydConfException

_metadata = {}
CONN_TIMEOUT = 1
PROXY_TIMEOUT = 1


def _mock_get_meta(self, obj):
    """get the metadata from a fake dictionary."""
    return _metadata


def fake_put_meta(name, metadata):
    """set the metadata to a fake dictionary."""
    _metadata[name] = metadata


class FakeHTTPResponse:
    """fake response class for faking HTTP answers."""
    def __init__(self):
        self.status = 0
        self.msg = ""
        self.reason = ""

    def read(self):
        """Return error message."""
        return self.msg

    def getheaders(self):
        """Return list of (header, value) tuples."""
        items = {}
        items["foo"] = "bar"
        return items


def _mock_conn_getresponse_404(self, conn):
    """Simulate an error 404."""
    resp = FakeHTTPResponse()
    resp.status = 404
    return resp


def _mock_conn_getresponse_500(self, conn):
    """Simulate an error 500."""
    resp = FakeHTTPResponse()
    resp.status = 500
    resp.msg = "Internal Server Error"
    resp.reason = "Sproxyd Internal Failure"
    return resp


def _mock_conn_getresponse_timeout(self, conn):
    """Simulate an error 500."""
    resp = FakeHTTPResponse()
    resp.status = 200
    eventlet.sleep(PROXY_TIMEOUT + 0.1)
    return resp


class FakeConn:
    """fake response class for faking HTTP answers."""

    def close(self):
        """Close conn."""
        pass


def _mock_do_connect(self, ipaddr, port, method, path, headers=None,
                     query_string=None, ssl=False):
    conn = FakeConn()
    return conn


class TestSproxydDiskFile(unittest.TestCase):
    """Tests for swift_scality_backend.diskfile."""

    def setUp(self):
        self._orig_tpool_exc = eventlet.tpool.execute
        eventlet.tpool.execute = lambda f, *args, **kwargs: f(*args, **kwargs)
        self.fake_logger = logging.getLogger(__name__)

        self.conf = dict(sproxyd_conn_timeout=CONN_TIMEOUT,
                         sproxyd_proxy_timeout=PROXY_TIMEOUT,
                         ipaddr="42.42.42.42",
                         port=4242,
                         path="/proxy/foo")
        self.filesystem = swift_scality_backend.diskfile.SproxydFileSystem(self.conf, self.fake_logger)
        swift_scality_backend.diskfile.SproxydFileSystem.do_connect = _mock_do_connect

    def tearDown(self):
        eventlet.tpool.execute = self._orig_tpool_exc
        self.fake_logger = None

    def _get_diskfile(self, a, c, o, **kwargs):
        return self.filesystem.get_diskfile(a, c, o, **kwargs)

    def test_get_diskfile(self):
        gdf = self._get_diskfile("accountX", "containerY", "objZ")
        assert gdf._filesystem is self.filesystem

    def test_get_meta(self):
        gdf = self._get_diskfile("accountX", "containerY", "objZ")

        fake_put_meta("meta1", "value1")
        with mock.patch('swift_scality_backend.diskfile.SproxydFileSystem.get_meta', _mock_get_meta):
            meta = gdf.read_metadata()
        self.assertIn("meta1", meta)
        self.assertEqual(meta["meta1"], "value1")

    def test_read_metadata_404(self):
        gdf = self._get_diskfile("accountX", "containerY", "objZ")

        with mock.patch('swift_scality_backend.diskfile.SproxydFileSystem.conn_getresponse', _mock_conn_getresponse_404):
            self.assertRaises(swift.common.exceptions.DiskFileDeleted, gdf.read_metadata)

    def test_read_metadata_500(self):
        gdf = self._get_diskfile("accountX", "containerY", "objZ")

        with mock.patch('swift_scality_backend.diskfile.SproxydFileSystem.conn_getresponse', _mock_conn_getresponse_500):
            self.assertRaises(swift.common.exceptions.DiskFileError, gdf.read_metadata)

    def test_read_metadata_timeout(self):
        gdf = self._get_diskfile("accountX", "containerY", "objZ")

        with mock.patch('swift_scality_backend.diskfile.SproxydFileSystem.conn_getresponse', _mock_conn_getresponse_timeout):
            self.assertRaises(eventlet.Timeout, gdf.read_metadata)

    def test_write_metadata_none(self):
        gdf = self._get_diskfile("accountX", "containerY", "objZ")

        self.assertRaises(swift.common.exceptions.DiskFileError, gdf.write_metadata, None)

    def test_write_metadata_500(self):
        gdf = self._get_diskfile("accountX", "containerY", "objZ")

        metadata = {'foo': 'bar', 'qux': 'baz'}
        with mock.patch('swift_scality_backend.diskfile.SproxydFileSystem.conn_getresponse', _mock_conn_getresponse_500):
            self.assertRaises(swift.common.exceptions.DiskFileError, gdf.write_metadata, metadata)

    def test_write_metadata_timeout(self):
        gdf = self._get_diskfile("accountX", "containerY", "objZ")

        metadata = {'foo': 'bar', 'qux': 'baz'}
        with mock.patch('swift_scality_backend.diskfile.SproxydFileSystem.conn_getresponse', _mock_conn_getresponse_timeout):
            self.assertRaises(eventlet.Timeout, gdf.write_metadata, metadata)

    def test_delete_404(self):
        gdf = self._get_diskfile("accountX", "containerY", "objZ")

        timestamp = 'foo'
        with mock.patch('swift_scality_backend.diskfile.SproxydFileSystem.conn_getresponse', _mock_conn_getresponse_404):
            gdf.delete(timestamp)

    def test_delete_500(self):
        gdf = self._get_diskfile("accountX", "containerY", "objZ")

        timestamp = 'foo'
        with mock.patch('swift_scality_backend.diskfile.SproxydFileSystem.conn_getresponse', _mock_conn_getresponse_500):
            self.assertRaises(swift.common.exceptions.DiskFileError, gdf.delete, timestamp)

    def test_delete_timeout(self):
        gdf = self._get_diskfile("accountX", "containerY", "objZ")

        timestamp = 'foo'
        with mock.patch('swift_scality_backend.diskfile.SproxydFileSystem.conn_getresponse', _mock_conn_getresponse_timeout):
            self.assertRaises(eventlet.Timeout, gdf.delete, timestamp)

# test connection to sproxyd
# test notfound exception
# test bad usermd


def test_ping_when_nw_exception_is_raised():

        def assert_ping_failed(expected_exc):
            logger = mock.Mock()
            filesystem = swift_scality_backend.diskfile.SproxydFileSystem({}, logger)

            with mock.patch.object(urllib, 'urlopen', side_effect=expected_exc):
                ping_result = filesystem.ping('http://ignored/')

                assert ping_result is False, ('Ping returned %r, '
                                              'not False' % ping_result)
                assert logger.info.called
                (msg, url, exc), _ = logger.info.call_args
                assert type(exc) is expected_exc
                assert "network error" in msg

        for exc in [eventlet.Timeout, httplib.HTTPException, IOError]:
            yield assert_ping_failed, exc


class TestSproxydDiskFile2(unittest.TestCase):
    """Tests for swift_scality_backend.diskfile."""

    def setUp(self):
        conf = {}
        self.logger = mock.Mock()
        self.filesystem = swift_scality_backend.diskfile.SproxydFileSystem(conf, self.logger)

    @mock.patch.object(urllib, 'urlopen')
    @mock.patch('swift_scality_backend.utils.is_sproxyd_conf_valid',
                side_effect=SproxydConfException(""))
    def test_ping_with_bad_sproxyd_conf(self, conf_checker_mock, urlopen_mock):
        ping_result = self.filesystem.ping('http://ignored')

        self.assertFalse(ping_result)
        self.assertTrue(self.logger.warning.called)
        (msg, url, exc), _ = self.logger.warning.call_args
        self.assertIs(type(exc), SproxydConfException)
        self.assertIn("is invalid:", msg)

    @mock.patch.object(urllib, 'urlopen', side_effect=Exception)
    def test_ping_with_unexpected_exc(self, urlopen_mock):
        ping_result = self.filesystem.ping('http://ignored')

        self.assertFalse(ping_result)
        self.assertTrue(self.logger.exception.called)
        (msg, url), _ = self.logger.exception.call_args
        self.assertIn("Unexpected", msg)
