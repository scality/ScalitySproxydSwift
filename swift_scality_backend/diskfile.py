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
import hashlib
import httplib
import operator
import time
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

    :param client_collection: Client collection to use to perform operations
    :type client_collection: `swift_scality_backend.http_utils.ClientCollection`
    :param name: standard object name
    :type name: `str`
    :param logger: Logger to use within the `DiskFileWriter`
    :type logger: `logging.Logger`
    """
    def __init__(self, client_collection, name, logger):
        self._client_collection = client_collection
        self._name = name
        self._logger = logger

        self._upload_size = 0
        headers = {
            'transfer-encoding': 'chunked'
        }
        self.logger.debug("DiskFileWriter for %r initialized", self._name)

        client = self._client_collection.get_write_client()
        self._conn, self._release_conn = client.get_http_conn_for_put(
            self._name, headers)

    def __repr__(self):
        ret = 'DiskFileWriter(client_collection=%r, name=%r, logger=%r)'
        return ret % (self._client_collection, self._name, self._logger)

    logger = property(operator.attrgetter('_logger'))

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

        self._client_collection.get_write_client().put_meta(
            self._name, metadata)

    def commit(self, timestamp):
        """
        Perform any operations necessary to mark the object as durable.

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
    def __init__(self, client_collection, name, use_splice, logger):
        self._client_collection = client_collection
        self._name = name
        self._use_splice = use_splice
        self._logger = logger

    def __repr__(self):
        ret = (
            'DiskFileReader(client_collection=%r, name=%r, use_splice=%r, '
            'logger=%r)')
        return ret % (self._client_collection, self._name, self._use_splice,
                      self.logger)

    logger = property(operator.attrgetter('_logger'))

    @utils.trace
    def __iter__(self):
        headers, data = self._client_collection.try_read(
            lambda client: client.get_object(self._name))
        return data

    @utils.trace
    def can_zero_copy_send(self):
        return self._use_splice

    @utils.trace
    def zero_copy_send(self, wsockfd):
        client = self._client_collection.get_read_client()

        object_url = urlparse.urlparse(client.get_url_for_object(self._name))
        conn = None

        with swift.common.exceptions.ConnectionTimeout(client.conn_timeout):
            conn = swift_scality_backend.http_utils.SomewhatBufferedHTTPConnection(
                object_url.netloc)

            try:
                conn.putrequest('GET', object_url.path, skip_host=False)
                conn.endheaders()
            except:  # noqa
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
        """
        Iterate over a range.

        :param start: First byte to read from (inclusive)
        :type start: int
        :param stop: Last byte to read (exclusive)
        :type stop: int
        """
        # HTTP range is inclusive on both ends.
        headers = {
            'range': 'bytes=%d-%d' % (start, stop - 1)
        }

        headers, data = self._client_collection.try_read(
            lambda client: client.get_object(self._name, headers))
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

    :param client_collection: Client collection to use to perform operations
    :type client_collection: `swift_scality_backend.http_utils.ClientCollection`
    :param account: account name for the object
    :type account: `str`
    :param container: container name for the object
    :type container: `str`
    :param obj: object name for the object
    :type obj: `str`
    :param use_splice: if true, use zero-copy splice() to send data
    :type use_splice: `bool`
    """

    def __init__(self, client_collection, account, container, obj, use_splice,
                 logger):
        # We hash the account, container and object name so that no 'special'
        # character will get in our way.
        sha1 = hashlib.sha1()
        for part in [account, container, obj]:
            sha1.update(part)
        self._name = sha1.hexdigest()
        self._metadata = None
        self._client_collection = client_collection
        self._logger = logger

        self._account = account
        self._container = container
        self._obj = obj
        self._use_splice = use_splice

    logger = property(operator.attrgetter('_logger'))
    client_collection = property(operator.attrgetter('_client_collection'))

    def __repr__(self):
        ret = ('DiskFile(client_collection=%r, account=%r, container=%r, obj=%r, '
               'use_splice=%r, logger=%r)')
        return ret % (self._client_collection, self._account, self._container,
                      self._obj, self._use_splice, self._logger)

    @utils.trace
    def open(self):
        """Open the file and read the metadata.

        This method must populate the _metadata attribute.

        :raise DiskFileDeleted: if it does not exist
        """
        metadata = self.client_collection.try_read(
            lambda client: client.get_meta(self._name))

        if metadata is None:
            raise swift.common.exceptions.DiskFileDeleted()

        self._metadata = metadata or {}

        try:
            x_delete_at = int(self._metadata['X-Delete-At'])
        except KeyError:
            pass
        else:
            if x_delete_at <= time.time():
                raise swift.common.exceptions.DiskFileExpired(
                    metadata=self._metadata)

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
        dr = DiskFileReader(self.client_collection, self._name,
                            use_splice=self._use_splice,
                            logger=self.logger)
        return dr

    @utils.trace
    @contextlib.contextmanager
    def create(self, size=None):
        """Context manager to create a file.

        :param size: ignored. Kept for compatibility with the native
                     `DiskFile` class in Swift. This `create` method is
                     called externally only by the `ObjectController`
        """
        yield DiskFileWriter(self.client_collection, self._name, self.logger)

    @utils.trace
    def write_metadata(self, metadata):
        """Write a block of metadata to an object."""
        self.client_collection.get_write_client().put_meta(self._name, metadata)

    @utils.trace
    def delete(self, timestamp):
        """Perform a delete for the given object in the given container under
        the given account.

        :param timestamp: ignored. Kept for compatibility with the native
                          `DiskFile` class in Swift. This `delete` method is
                          called externally only by the `ObjectController`
        """
        self.client_collection.get_write_client().del_object(self._name)

    # Class `swift.common.utils.Timestamp` is Swift 2.0+
    if hasattr(swift.common.utils, 'Timestamp'):
        @property
        def timestamp(self):
            if self._metadata is None:
                raise swift.common.exceptions.DiskFileNotOpen()
            return swift.common.utils.Timestamp(self._metadata.get('X-Timestamp'))

        data_timestamp = timestamp

        @property
        def durable_timestamp(self):
            return None

        @property
        def fragments(self):
            return None


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

    def get_diskfile(self, client_collection, account, container, obj):
        return DiskFile(client_collection, account, container, obj,
                        use_splice=self.use_splice, logger=self.logger)

    def pickle_async_update(self, *args, **kwargs):
        pass
