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
import copy

import eventlet
import eventlet.green.os
import swift.common.bufferedhttp
import swift.common.exceptions
import swift.common.swob
import swift.common.utils
from swift.common.request_helpers import is_sys_meta

try:
    import swift.common.splice
    HAS_NEW_SPLICE = True
except ImportError:
    HAS_NEW_SPLICE = False

from scality_sproxyd_client.utils import get_urllib3
from scality_sproxyd_client.exceptions import SproxydHTTPException
import swift_scality_backend.http_utils
import swift_scality_backend.splice_utils
from swift_scality_backend import utils

urllib3 = get_urllib3()

# These are system-set metadata keys that cannot be changed with a POST.
# They should be lowercase.
RESERVED_DATAFILE_META = {'content-length', 'deleted', 'etag'}
DATAFILE_SYSTEM_META = {'x-static-large-object'}


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
        self._md5sum = hashlib.md5()

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
        self._md5sum.update(chunk)
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

        metadata_to_put = copy.copy(metadata)
        metadata_to_put['name'] = self._name
        metadata_to_put['ETag'] = self._md5sum.hexdigest()

        metadata_to_put.update({
            'df': metadata,
            'mf': {},
        })

        self.logger.debug("Data successfully written for object : %r", self._name)

        self._client_collection.get_write_client().put_meta(
            self._name, metadata_to_put)

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
                if client._url_username and client._url_password:
                    creds_str = ('%s:%s' % (client._url_username, client._url_password))
                    basic_auth_header = urllib3.util.make_headers(basic_auth=creds_str)
                    for (key, value) in basic_auth_header.items():
                        conn.putheader(key, value)
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

    @staticmethod
    def merge_df_mf_metadata(df_md_source, mf_md_source):
        """Merge the datafile metadata and metafile metadata dictionaries together.

        Datafile metadata refers to metadata originally included when the object
        was first PUT, and does not include metadata set by any subsequent POST.

        Metafile metadata refers to metadata written by a POST, and does not
        include any persistent metadata that was set by the original PUT.
        """
        md_dest = {}

        if not mf_md_source:
            md_dest.update(df_md_source)
        else:
            sys_metadata = {
                key: val for key, val in df_md_source.items() if key.lower() in
                (RESERVED_DATAFILE_META | DATAFILE_SYSTEM_META) or is_sys_meta('object', key)
            }
            md_dest.update(mf_md_source)
            md_dest.update(sys_metadata)

        return md_dest

    @utils.trace
    def open(self, current_time=None):
        """Open the file and read the metadata.

        :param current_time: Unix time used in checking expiration. If not
             present, the current time will be used.

        This method must populate the _metadata attribute.

        :raise DiskFileDeleted: if it does not exist
        """
        try:
            metadata = self.client_collection.try_read(
                lambda client: client.get_meta(self._name))
        except EOFError:
            self.logger.error(
                'ERROR in DiskFile.open(): metadata not found on Scality RING for key %s'
                % self._name)
            metadata = None

        if metadata is None:
            raise swift.common.exceptions.DiskFileDeleted()

        # 'df' subdictionary refers to DataFile metadata, 'mf' to MetaFile metadata
        if 'df' in metadata and 'mf' in metadata:
            self._metadata = DiskFile.merge_df_mf_metadata(metadata['df'], metadata['mf'])
            self._metadata['df'] = metadata['df']
            self._metadata['mf'] = metadata['mf']
        else:
            self._metadata = metadata
            self._metadata['df'] = copy.copy(metadata)
            self._metadata['mf'] = {}

        try:
            x_delete_at = int(self._metadata['X-Delete-At'])
        except KeyError:
            pass
        else:
            if current_time is None:
                current_time = time.time()
            if x_delete_at <= current_time:
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
        md_to_return = copy.deepcopy(self._metadata)
        md_to_return.pop('df')
        md_to_return.pop('mf')
        return md_to_return

    @utils.trace
    def read_metadata(self, current_time=None):
        """Return the metadata for an object without requiring the caller
        to open the object first.

        :param current_time: Unix time used in checking expiration. If not
             present, the current time will be used.
        :returns: metadata dictionary for an object
        """
        with self.open(current_time=current_time):
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
    def write_metadata(self, md_to_add):
        """Write a block of metadata to an object."""

        if not self._metadata:
            md_to_write = copy.deepcopy(md_to_add)
            md_to_write.update({
                'df': {
                    'name': self._name,
                },
                'mf': md_to_add
            })
            self.client_collection.get_write_client().put_meta(self._name, md_to_write)
            return

        # Keep the most recent Content-Type from either datafile or metafile metadata
        if 'Content-Type' not in md_to_add and self._metadata is not None and \
           'Content-Type' in self._metadata['mf'] and \
           self._metadata['mf']['Content-Type'] != self._metadata['df']['Content-Type'] and \
           self._metadata['mf']['Content-Type-Timestamp'] > \
           self._metadata['df']['X-Timestamp']:
            md_to_add['Content-Type'] = self._metadata['mf']['Content-Type']
            md_to_add['Content-Type-Timestamp'] = self._metadata['mf']['Content-Type-Timestamp']

        df_md_dict = copy.deepcopy(self._metadata['df'])
        md_to_write = DiskFile.merge_df_mf_metadata(df_md_dict, md_to_add)
        md_to_write.update({
            'df': df_md_dict,
            'mf': md_to_add,
        })

        self.client_collection.get_write_client().put_meta(self._name, md_to_write)

    @utils.trace
    def delete(self, timestamp):
        """Perform a delete for the given object in the given container under
        the given account.

        :param timestamp: ignored. Kept for compatibility with the native
                          `DiskFile` class in Swift. This `delete` method is
                          called externally only by the `ObjectController`
        """
        self.client_collection.get_write_client().del_object(self._name)

    def get_metafile_metadata(self):
        """Provide the metafile metadata for a previously opened object as a dictionary.

        This is metadata that was written by a POST,
        and does not include any persistent metadata that was set by the original PUT.
        """
        if self._metadata is None:
            self._metadata = self.client_collection.try_read(
                lambda client: client.get_meta(self._name))
        return self._metadata['mf']

    def get_datafile_metadata(self):
        """Provide the datafile metadata for a previously opened object as a dictionary.

        This is metadata that was included when the object was first PUT,
        and does not include metadata set by any subsequent POST.
        """
        if self._metadata is None:
            self._metadata = self.client_collection.try_read(
                lambda client: client.get_meta(self._name))
        return self._metadata['df']

    @property
    def content_length(self):
        if self._metadata is None:
            raise swift.common.exceptions.DiskFileNotOpen()
        return self._metadata.get('Content-Length')

    @property
    def timestamp(self):
        if self._metadata is None:
            raise swift.common.exceptions.DiskFileNotOpen()
        t = self._metadata.get('X-Timestamp')
        return swift.common.utils.Timestamp(t)

    @property
    def data_timestamp(self):
        """Provides the datafile timestamp
        """
        if self._metadata is None:
            raise swift.common.exceptions.DiskFileNotOpen()
        t = self._metadata['df'].get('X-Timestamp')
        return swift.common.utils.Timestamp(t)

    @property
    def durable_timestamp(self):
        """Related to erasure coding storage policy type, not to replication
        policy type.

        Provides the timestamp of the newest data file found in the object
        directory, i.e. the one which call to open() populated the self._metadata
        attribute.
        """
        if self._metadata is None:
            raise swift.common.exceptions.DiskFileNotOpen()
        t = self._metadata['df'].get('X-Timestamp')
        return swift.common.utils.Timestamp(t)

    @property
    def fragments(self):
        return None

    @property
    def content_type(self):
        if self._metadata is None:
            raise swift.common.exceptions.DiskFileNotOpen()
        return self._metadata.get('Content-Type')

    @property
    def content_type_timestamp(self):
        """Provides the content-type timestamp if the Content-Type metadata
        attribute was ever modified, or the datafile metadata timestamp if not.
        """
        if self._metadata is None:
            raise swift.common.exceptions.DiskFileNotOpen()
        t = self._metadata.get('Content-Type-Timestamp',
                               self._metadata['df'].get('X-Timestamp'))
        return swift.common.utils.Timestamp(t)


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

        https_used = False
        sproxyd_endpoints = conf.get('sproxyd_endpoints')
        if not sproxyd_endpoints:
            sproxyd_endpoints = conf.get('sproxyd_host', '')

        for sproxyd_url in sproxyd_endpoints.split(','):
            url = urlparse.urlparse(sproxyd_url)
            if url.scheme == 'https':
                https_used = True
                break

        if conf_wants_splice and https_used:
            self.logger.warn(
                "Use of splice() requested (config says \"splice = %s\"), "
                "but splice() cannot be used with an HTTPS connection "
                "to sproxyd. splice() will not be used." % conf.get('splice'))

        if conf_wants_splice and not system_has_splice:
            self.logger.warn(
                "Use of splice() requested (config says \"splice = %s\"), "
                "but the system does not support it. "
                "splice() will not be used." % conf.get('splice'))

        if conf_wants_splice and system_has_splice and not https_used:
            self.use_splice = True

    def get_diskfile(self, client_collection, account, container, obj):
        return DiskFile(client_collection, account, container, obj,
                        use_splice=self.use_splice, logger=self.logger)

    def pickle_async_update(self, *args, **kwargs):
        pass
