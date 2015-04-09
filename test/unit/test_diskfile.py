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

import httplib
import StringIO
import unittest

import eventlet
import eventlet.wsgi
import mock
import swift.common.exceptions
import swift.common.utils

from swift_scality_backend.diskfile import DiskFileWriter, \
    DiskFileReader, DiskFile, DiskFileManager
from scality_sproxyd_client.exceptions import SproxydHTTPException
from scality_sproxyd_client.sproxyd_client import SproxydClient
from . import utils


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


eventlet.monkey_patch()


class FakeHTTPResp(httplib.HTTPResponse):

    def __init__(self, status=200):
        self.status = status
        self.reason = 'because'

    def read(self):
        return 'My mock msg'


class FakeHTTPConn(mock.Mock):

    def __init__(self, *args, **kwargs):
        super(FakeHTTPConn, self).__init__(*args, **kwargs)
        self.resp_status = kwargs.get('resp_status', 200)
        self._buffer = StringIO.StringIO()

    def getresponse(self):
        return FakeHTTPResp(self.resp_status)

    def send(self, data):
        self._buffer.write(data)


class TestDiskFileManager(unittest.TestCase):
    """Tests for swift_scality_backend.diskfile.DiskFileManager"""

    def test_init_with_default_splice(self):
        dfm = DiskFileManager({}, mock.Mock())
        self.assertFalse(dfm.use_splice)

    def test_init_with_splice_no(self):
        dfm = DiskFileManager({'splice': 'no'}, mock.Mock())
        self.assertFalse(dfm.use_splice)

    def _test_init_splice_unavailable(self):
        dfm = DiskFileManager({'splice': 'no'}, mock.Mock())
        self.assertFalse(dfm.use_splice, "Splice not wanted by conf and not " +
                         "available from system: use_splice should be False")

        mock_logger = mock.Mock()
        dfm = DiskFileManager({'splice': 'yes'}, mock_logger)
        self.assertFalse(dfm.use_splice, "Splice wanted by conf but not " +
                         "available from system: use_splice should be False")
        self.assertTrue(mock_logger.warn.called)

    def _test_init_splice_available(self):
        dfm = DiskFileManager({'splice': 'yes'}, mock.Mock())
        self.assertTrue(dfm.use_splice, "Splice wanted by conf and " +
                        "available from system: use_splice should be True")

        dfm = DiskFileManager({'splice': 'no'}, mock.Mock())
        self.assertFalse(dfm.use_splice, "Splice not wanted by conf though " +
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

    def test_get_diskfile(self):
        sproxyd_client = SproxydClient(['http://host:81/path/'], logger=mock.Mock())
        dfm = DiskFileManager({}, mock.Mock())

        diskfile = dfm.get_diskfile(sproxyd_client, 'a', 'c', 'o')
        self.assertTrue(isinstance(diskfile, DiskFile))


class TestDiskFileWriter(unittest.TestCase):
    """Tests for swift_scality_backend.diskfile.DiskFileWriter"""

    @mock.patch('scality_sproxyd_client.sproxyd_client.SproxydClient.get_http_conn_for_put',
                return_value=(None, None))
    def test_init(self, mock_http):
        sproxyd_client = SproxydClient(['http://host:81/path/'], logger=mock.Mock())
        # Note the white space, to test proper URL encoding
        DiskFileWriter(sproxyd_client, 'ob j')

        expected_header = {'transfer-encoding': 'chunked'}
        mock_http.assert_called_once_with('ob j', expected_header)

    @mock.patch('scality_sproxyd_client.sproxyd_client.SproxydClient.get_http_conn_for_put',
                return_value=(FakeHTTPConn(resp_status=404), None))
    def test_put_with_404_response(self, mock_http):
        sproxyd_client = SproxydClient(['http://host:81/path/'], logger=mock.Mock())
        dfw = DiskFileWriter(sproxyd_client, 'obj')

        fake_http_conn = mock_http.return_value[0]
        msg = r'.*404 / %s.*' % fake_http_conn.getresponse().read()
        utils.assertRaisesRegexp(SproxydHTTPException, msg, dfw.put, {})

        fake_http_conn.close.assert_called_once_with()

    @mock.patch('scality_sproxyd_client.sproxyd_client.SproxydClient.get_http_conn_for_put',
                return_value=(FakeHTTPConn(), mock.Mock()))
    @mock.patch('scality_sproxyd_client.sproxyd_client.SproxydClient.put_meta')
    def test_put_with_200_response(self, mock_put_meta, mock_http):
        sproxyd_client = SproxydClient(['http://host:81/path/'], logger=mock.Mock())
        dfw = DiskFileWriter(sproxyd_client, 'obj')

        dfw.put({'meta1': 'val'})

        fake_http_conn = mock_http.return_value[0]
        self.assertEqual('0\r\n\r\n', fake_http_conn._buffer.getvalue())

        mock_release_conn = mock_http.return_value[1]
        mock_release_conn.assert_called_once_with()

        mock_put_meta.assert_called_with('obj', {'meta1': 'val', 'name': 'obj'})


class TestDiskFile(unittest.TestCase):
    """Tests for swift_scality_backend.diskfile.DiskFile"""

    @mock.patch('scality_sproxyd_client.sproxyd_client.SproxydClient.get_meta',
                return_value=None)
    def test_open_when_no_metadata(self, mock_get_meta):
        sproxyd_client = SproxydClient(['http://host:81/path/'], logger=mock.Mock())
        df = DiskFile(sproxyd_client, 'a', 'c', 'o', use_splice=False)

        self.assertRaises(swift.common.exceptions.DiskFileDeleted, df.open)
        mock_get_meta.assert_called_once_with('a/c/o')

    @mock.patch('scality_sproxyd_client.sproxyd_client.SproxydClient.get_meta',
                return_value={'name': 'o'})
    def test_open(self, mock_get_meta):
        sproxyd_client = SproxydClient(['http://host:81/path/'], logger=mock.Mock())
        df = DiskFile(sproxyd_client, 'a', 'c', 'o', use_splice=False)

        df.open()

        self.assertEqual({'name': 'o'}, df._metadata)

    def test_get_metadata_when_diskfile_not_open(self):
        sproxyd_client = SproxydClient(['http://host:81/path/'], logger=mock.Mock())
        df = DiskFile(sproxyd_client, 'a', 'c', 'o', use_splice=False)

        self.assertRaises(swift.common.exceptions.DiskFileNotOpen,
                          df.get_metadata)

    @mock.patch('scality_sproxyd_client.sproxyd_client.SproxydClient.get_meta',
                return_value={'name': 'o'})
    def test_read_metadata(self, mock_get_meta):
        sproxyd_client = SproxydClient(['http://host:81/path/'], logger=mock.Mock())
        df = DiskFile(sproxyd_client, 'a', 'c', 'o', use_splice=False)

        metadata = df.read_metadata()

        self.assertEqual({'name': 'o'}, metadata)

    def test_reader(self):
        sproxyd_client = SproxydClient(['http://host:81/path/'], logger=mock.Mock())
        df = DiskFile(sproxyd_client, 'a', 'c', 'o', use_splice=False)

        reader = df.reader()
        self.assertTrue(isinstance(reader, DiskFileReader))

    @mock.patch('scality_sproxyd_client.sproxyd_client.SproxydClient.get_http_conn_for_put',
                return_value=(FakeHTTPConn(), mock.Mock()))
    def test_create(self, mock_http):
        sproxyd_client = SproxydClient(['http://host:81/path/'], logger=mock.Mock())
        df = DiskFile(sproxyd_client, 'a', 'c', 'o', use_splice=False)

        with df.create() as writer:
            self.assertTrue(isinstance(writer, DiskFileWriter))

    @mock.patch('scality_sproxyd_client.sproxyd_client.SproxydClient.put_meta')
    def test_write_metadata(self, mock_put_meta):
        sproxyd_client = SproxydClient(['http://host:81/path/'], logger=mock.Mock())
        df = DiskFile(sproxyd_client, 'a', 'c', 'o', use_splice=False)

        df.write_metadata({'k': 'v'})

        mock_put_meta.assert_called_once_with('a/c/o', {'k': 'v'})

    @mock.patch('scality_sproxyd_client.sproxyd_client.SproxydClient.del_object')
    def test_delete(self, mock_del_object):
        sproxyd_client = SproxydClient(['http://host:81/path/'], logger=mock.Mock())
        df = DiskFile(sproxyd_client, 'a', 'c', 'o', use_splice=False)

        df.delete("ignored")

        mock_del_object.assert_called_once_with('a/c/o')
