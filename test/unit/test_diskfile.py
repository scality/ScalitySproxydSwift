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

import logging
import unittest

import eventlet
import mock
import swift.common.exceptions

import swift_scality_backend.diskfile

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
