# Copyright (c) 2010-2013 OpenStack, LLC.
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

""" Sproxyd Disk File Interface for Swift Object Server"""

import contextlib
import httplib
import urlparse

import eventlet
import eventlet.green.os
import swift.common.bufferedhttp
import swift.common.exceptions
import swift.common.swob
import swift.common.utils

try:
    import swift.common.splice
    HAS_NEW_SPLICE = True
except ImportError:
    HAS_NEW_SPLICE = False

from scality_sproxyd_client.exceptions import SproxydHTTPException
import swift_scality_backend.http_utils
import swift_scality_backend.splice_utils
from swift_scality_backend import utils


class DiskFileWriter(object):
    """A simple sproxyd pass-through

    Encapsulation of the write context for servicing PUT REST API
    requests. Serves as the context manager object for DiskFile's create()
    method.

    :param filesystem: internal file system object to use
    :param name: standard object name
    """
    def __init__(self, filesystem, name):
        self._filesystem = filesystem
        self._name = name
        self._upload_size = 0
        headers = {
            'transfer-encoding': 'chunked'
        }
        self.logger.debug("DiskFileWriter for %r initialized", self._name)

        self._conn, self._release_conn = filesystem.get_http_conn_for_put(
            self._name, headers)

    def __repr__(self):
        ret = 'DiskFileWriter(filesystem=%r, object_name=%r)'
        return ret % (self._filesystem, self._name)

    logger = property(lambda self: self._filesystem._logger)

    def write(self, chunk):
        """Write a chunk of data.

        :param chunk: the chunk of data to write as a string object
        """
        self._conn.send('%x\r\n%s\r\n' % (len(chunk), chunk))
        self._upload_size += len(chunk)
        return self._upload_size

    @utils.trace
    def put(self, metadata):
        """Finalize writing the object.

        :param metadata: dictionary of metadata to be associated with the
                         object
        """
        self._conn.send('0\r\n\r\n')
        try:
            resp = self._conn.getresponse()
            msg = resp.read()
            if resp.status != 200:
                raise SproxydHTTPException("putting: %s / %s" % (
                    str(resp.status), str(msg)))
        except Exception:
            conn, self._conn = self._conn, None
            try:
                conn.close()
            except Exception:
                self.logger.exception('Failure while closing connection')
            raise

        self._release_conn()
        metadata['name'] = self._name
        self.logger.debug("Data successfully written for object : %r", self._name)
        self._filesystem.put_meta(self._name, metadata)

    def commit(self, timestamp):
        """
        Perform any operations necessary to mark the object as durable. For
        replication policy type this is a no-op.
        :param timestamp: object put timestamp, an instance of
                          :class:`~swift.common.utils.Timestamp`
        """
        pass


class DiskFileReader(object):
    """A simple sproxyd pass-through

    Encapsulation of the read context for servicing GET REST API
    requests. Serves as the context manager object for DiskFile's reader()
    method.

    :param filesystem: internal file system object to use
    :param name: object name
    :param use_splice: if true, use zero-copy splice() to send data
    """
    def __init__(self, filesystem, name, use_splice):
        self._filesystem = filesystem
        self._name = name
        self._use_splice = use_splice

    def __repr__(self):
        ret = 'DiskFileReader(filesystem=%r, object_name=%r, use_splice=%r)'
        return ret % (self._filesystem, self._name, self._use_splice)

    logger = property(lambda self: self._filesystem._logger)

    @utils.trace
    def __iter__(self):
        headers, data = self._filesystem.get_object(self._name)
        return data

    @utils.trace
    def can_zero_copy_send(self):
        return self._use_splice

    @utils.trace
    def zero_copy_send(self, wsockfd):
        object_url = urlparse.urlparse(self._filesystem.get_url_for_object(
            self._name))
        conn = None

        with swift.common.exceptions.ConnectionTimeout(
                self._filesystem.conn_timeout):
            conn = swift_scality_backend.http_utils.SomewhatBufferedHTTPConnection(
                object_url.netloc)

            try:
                conn.putrequest('GET', object_url.path, skip_host=False)
                conn.endheaders()
            except:
                conn.close()
                raise

        with conn:
            resp = conn.getresponse()

            if resp.status != httplib.OK:
                raise SproxydHTTPException(
                    'Unexpected response code: %s' % resp.status,
                    url=object_url.geturl(),
                    http_status=resp.status, http_reason=resp.reason)

            if resp.chunked:
                raise SproxydHTTPException(
                    'Chunked response not supported',
                    url=object_url.geturl(),
                    http_status=resp.status, http_reason=resp.reason)

            buff = resp.fp.get_buffered()
            buff_len = len(buff)

            while buff:
                written = eventlet.green.os.write(wsockfd, buff)
                buff = buff[written:]

            to_splice = resp.length - buff_len if resp.length is not None else None

            swift_scality_backend.splice_utils.splice_socket_to_socket(
                resp.fileno(), wsockfd, length=to_splice)

    @utils.trace
    def app_iter_range(self, start, stop):
        """iterate over a range."""
        headers = {
            'range': 'bytes=' + str(start) + '-' + str(stop)
        }

        headers, data = self._filesystem.get_object(self._name, headers)
        return data

    @utils.trace
    def app_iter_ranges(self, ranges, content_type, boundary, size):
        """iterate over multiple ranges."""
        if not ranges:
            yield ''
        else:
            for chunk in swift.common.swob.multi_range_iterator(
                    ranges, content_type, boundary, size, self.app_iter_range):
                yield chunk


class DiskFile(object):
    """A simple sproxyd pass-through

    :param filesystem: internal file system object to use
    :param account: account name for the object
    :param container: container name for the object
    :param obj: object name for the object
    :param use_splice: if true, use zero-copy splice() to send data
    """

    def __init__(self, filesystem, account, container, obj, use_splice):
        self._name = '/'.join((account, container, obj))
        self._metadata = None
        self._filesystem = filesystem

        self._account = account
        self._container = container
        self._obj = obj
        self._use_splice = use_splice

    logger = property(lambda self: self._filesystem._logger)

    def __repr__(self):
        ret = ('DiskFile(filesystem=%r, account=%r, container=%r, obj=%r, '
               'use_splice=%r)')
        return ret % (self._filesystem, self._account, self._container,
                      self._obj, self._use_splice)

    @utils.trace
    def open(self):
        """Open the file and read the metadata.

        This method must populate the _metadata attribute.

        :raise DiskFileDeleted: if it does not exist
        """
        metadata = self._filesystem.get_meta(self._name)
        if metadata is None:
            raise swift.common.exceptions.DiskFileDeleted()
        self._metadata = metadata or {}
        return self

    @utils.trace
    def __enter__(self):
        if self._metadata is None:
            raise swift.common.exceptions.DiskFileNotOpen()
        return self

    @utils.trace
    def __exit__(self, t, v, tb):
        pass

    @utils.trace
    def get_metadata(self):
        """Provide the metadata for an object as a dictionary.

        :returns: object's metadata dictionary
        """
        if self._metadata is None:
            raise swift.common.exceptions.DiskFileNotOpen()
        return self._metadata

    @utils.trace
    def read_metadata(self):
        """Return the metadata for an object.

        :returns: metadata dictionary for an object
        """
        with self.open():
            return self.get_metadata()

    @utils.trace
    def reader(self, keep_cache=False):
        """Return a swift.common.swob.Response class compatible "app_iter" object.

        :param keep_cache: ignored. Kept for compatibility with the native
                          `DiskFile` class in Swift
        """
        dr = DiskFileReader(self._filesystem, self._name,
                            use_splice=self._use_splice)
        return dr

    @utils.trace
    @contextlib.contextmanager
    def create(self, size=None):
        """Context manager to create a file.

        :param size: ignored. Kept for compatibility with the native
                     `DiskFile` class in Swift. This `create` method is
                     called externally only by the `ObjectController`
        """
        yield DiskFileWriter(self._filesystem, self._name)

    @utils.trace
    def write_metadata(self, metadata):
        """Write a block of metadata to an object."""
        self._filesystem.put_meta(self._name, metadata)

    @utils.trace
    def delete(self, timestamp):
        """Perform a delete for the given object in the given container under
        the given account.

        :param timestamp: ignored. Kept for compatibility with the native
                          `DiskFile` class in Swift. This `delete` method is
                          called externally only by the `ObjectController`
        """
        self._filesystem.del_object(self._name)


class DiskFileManager(object):
    """
    Management class for devices, providing common place for shared parameters
    and methods not provided by the DiskFile class (which primarily services
    the object server REST API layer).

    The `get_diskfile()` method is how this implementation creates a `DiskFile`
    object.
    """

    def __init__(self, conf, logger):
        """
        :param conf: caller provided configuration object
        :param logger: caller provided logger
        """
        self.logger = logger

        self.use_splice = False

        conf_wants_splice = swift.common.utils.config_true_value(
            conf.get('splice', 'no'))

        if HAS_NEW_SPLICE:
            system_has_splice = swift.common.splice.splice.available
        else:
            try:
                system_has_splice = swift.common.utils.system_has_splice()
            except AttributeError:  # Old Swift versions
                system_has_splice = False

        if conf_wants_splice and not system_has_splice:
            self.logger.warn(
                "Use of splice() requested (config says \"splice = %s\"), "
                "but the system does not support it. "
                "splice() will not be used." % conf.get('splice'))

        if conf_wants_splice and system_has_splice:
            self.use_splice = True

    def get_diskfile(self, sproxyd_client, account, container, obj):
        return DiskFile(sproxyd_client, account, container, obj,
                        use_splice=self.use_splice)
