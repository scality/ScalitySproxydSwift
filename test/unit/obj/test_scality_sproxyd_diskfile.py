#-*- coding:utf-8 -*-
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

"""Tests for swift.obj.scality_sproxyd_diskfile"""

import os
import stat
import errno
import unittest
import tempfile
import shutil
import mock
import time
import eventlet
from eventlet import Timeout
from eventlet import tpool
from mock import Mock, patch
from hashlib import md5
from copy import deepcopy
from swift.common.exceptions import DiskFileNotExist, DiskFileError, \
    DiskFileNoSpace, DiskFileNotOpen, DiskFileDeleted
from swift.common.utils import ThreadPool

import swift.common.utils
from swift.common.utils import normalize_timestamp
import swift.obj.diskfile
from swift.obj.diskfile import DiskFileWriter, DiskFile, DiskFileManager
import swift.obj.scality_sproxyd_diskfile
from swift.obj.scality_sproxyd_diskfile import DiskFileWriter, DiskFile, DiskFileReader, ScalitySproxydFileSystem

from test.unit import FakeLogger

_metadata = {}
CONN_TIMEOUT=1
PROXY_TIMEOUT=1

def _mock_get_meta(self, obj):
    """
    get the metadata from a fake dictionary
    """
    global _metadata
    return _metadata

def fake_put_meta(name, metadata):
    """
    set the metadata to a fake dictionary
    """
    global _metadata
    _metadata[name] = metadata

class FakeHTTPResponse:
    """
    fake response class for faking HTTP answers
    """
    def __init__(self):
        self.status = 0
        self.msg = ""
        self.reason = ""

    def read(self):
        """Return error message"""
        return self.msg;

    def getheaders(self):
        """Return list of (header, value) tuples."""
        items = {}
        items["foo"] = "bar"
        return items


def _mock_conn_getresponse_404(self, conn):
    """
    Simulate an error 404
    """
    resp = FakeHTTPResponse()
    resp.status = 404
    return resp

def _mock_conn_getresponse_500(self, conn):
    """
    Simulate an error 500
    """
    resp = FakeHTTPResponse()
    resp.status = 500
    resp.msg = "Internal Server Error"
    resp.reason = "Sproxyd Internal Failure"
    return resp

def _mock_conn_getresponse_timeout(self, conn):
    """
    Simulate an error 500
    """
    global PROXY_TIMEOUT
    resp = FakeHTTPResponse()
    resp.status = 200
    eventlet.sleep(PROXY_TIMEOUT+1)
    return resp

class FakeConn:
    """
    fake response class for faking HTTP answers
    """
    def __init__(self):
        self.foo = 0

    def close(self):
        """Close conn"""
        pass

def _mock_do_connect(self, ipaddr, port, method, path, headers=None,
                     query_string=None, ssl=False):
    conn = FakeConn()
    return conn

def debuglog(msg):
    print msg

class MockException(Exception):
    """
    """
    pass

class TestScalitySproxydDiskFile(unittest.TestCase):
    """ Tests for swift.obj.scality_sproxyd_diskfile """

    def setUp(self):
        self._orig_tpool_exc = tpool.execute
        tpool.execute = lambda f, *args, **kwargs: f(*args, **kwargs)
        self.fake_logger = FakeLogger()

        global CONN_TIMEOUT, PROXY_TIMEOUT
        self.conf = dict(conn_timeout=CONN_TIMEOUT,
                         proxy_timeout=PROXY_TIMEOUT, 
                         ipaddr="42.42.42.42", 
                         port=4242,
                         path="/proxy/foo")
        self.filesystem = ScalitySproxydFileSystem(self.conf, self.fake_logger)
        saved_do_connect = swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.do_connect
        swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.do_connect = _mock_do_connect

    def tearDown(self):
        tpool.execute = self._orig_tpool_exc
        self.fake_logger = None

    def _get_diskfile(self, a, c, o, **kwargs):
        return self.filesystem.get_diskfile(a, c, o, **kwargs)

    def test_filesystem(self):
        gdf = self._get_diskfile("accountX", "containerY", "objZ")
        assert gdf._filesystem is self.filesystem

    def test_get_diskfile(self):
        debuglog("test_get_diskfile")
        gdf = self._get_diskfile("accountX", "containerY", "objZ")
        assert gdf._filesystem is self.filesystem

    def test_get_meta(self):
        debuglog("test_get_meta")
        gdf = self._get_diskfile("accountX", "containerY", "objZ")
        saved_get_meta = swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.get_meta
        swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.get_meta = _mock_get_meta
        fake_put_meta("meta1", "value1")
        metadata = gdf.read_metadata()
        swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.get_meta = saved_get_meta

    def test_read_metadata_404(self):
        debuglog("test_read_metadata_404")
        gdf = self._get_diskfile("accountX", "containerY", "objZ")
        saved_conn_getresponse = swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse
        swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse = _mock_conn_getresponse_404
        try:
            metadata = gdf.read_metadata()
        except DiskFileDeleted:
            pass
        else:
            assert False
        swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse = saved_conn_getresponse

    def test_read_metadata_500(self):
        debuglog("test_read_metadata_500")
        gdf = self._get_diskfile("accountX", "containerY", "objZ")
        saved_conn_getresponse = swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse
        swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse = _mock_conn_getresponse_500
        try:
            metadata = gdf.read_metadata()
        except DiskFileError:
            pass
        else:
            assert False
        swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse = saved_conn_getresponse

    def test_read_metadata_timeout(self):
        debuglog("test_read_metadata_timeout")
        gdf = self._get_diskfile("accountX", "containerY", "objZ")
        saved_conn_getresponse = swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse
        swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse = _mock_conn_getresponse_timeout
        try:
            metadata = gdf.read_metadata()
        except Timeout:
            pass
        else:
            assert False
        swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse = saved_conn_getresponse

    def test_write_metadata_none(self):
        debuglog("test_write_metadata_none")
        gdf = self._get_diskfile("accountX", "containerY", "objZ")
        saved_conn_getresponse = swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse
        swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse = _mock_conn_getresponse_500
        try:
            gdf.write_metadata(None)
        except DiskFileError:
            pass
        else:
            assert False
        swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse = saved_conn_getresponse

    def test_write_metadata_500(self):
        debuglog("test_write_metadata_500")
        gdf = self._get_diskfile("accountX", "containerY", "objZ")
        saved_conn_getresponse = swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse
        swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse = _mock_conn_getresponse_500
        metadata = {'foo': 'bar', 'qux': 'baz'}
        try:
            gdf.write_metadata(metadata)
        except DiskFileError:
            pass
        else:
            assert False
        swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse = saved_conn_getresponse

    def test_write_metadata_timeout(self):
        debuglog("test_write_metadata_timeout")
        gdf = self._get_diskfile("accountX", "containerY", "objZ")
        saved_conn_getresponse = swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse
        swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse = _mock_conn_getresponse_timeout
        metadata = {'foo': 'bar', 'qux': 'baz'}
        try:
            gdf.write_metadata(metadata)
        except Timeout:
            pass
        else:
            assert False
        swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse = saved_conn_getresponse

    def test_delete_404(self):
        debuglog("test_delete_404")
        gdf = self._get_diskfile("accountX", "containerY", "objZ")
        saved_conn_getresponse = swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse
        swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse = _mock_conn_getresponse_404
        timestamp = 'foo'
        try:
            gdf.delete(timestamp)
        except:
            assert False
        else:
            pass
        swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse = saved_conn_getresponse

    def test_delete_500(self):
        debuglog("test_delete_500")
        gdf = self._get_diskfile("accountX", "containerY", "objZ")
        saved_conn_getresponse = swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse
        swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse = _mock_conn_getresponse_500
        timestamp = 'foo'
        try:
            gdf.delete(timestamp)
        except DiskFileError:
            pass
        else:
            assert False
        swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse = saved_conn_getresponse

    def test_delete_timeout(self):
        debuglog("test_delete_timeout")
        gdf = self._get_diskfile("accountX", "containerY", "objZ")
        saved_conn_getresponse = swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse
        swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse = _mock_conn_getresponse_timeout
        timestamp = 'foo'
        try:
            gdf.delete(timestamp)
        except Timeout:
            pass
        else:
            assert False
        swift.obj.scality_sproxyd_diskfile.ScalitySproxydFileSystem.conn_getresponse = saved_conn_getresponse


#test connection to sproxyd
#test notfound exception
#test bad usermd
#
