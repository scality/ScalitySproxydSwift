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

import base64
import contextlib
import functools
import httplib
import itertools
import pickle
import types
import urllib

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

from swift_scality_backend.exceptions import SproxydHTTPException, \
    SproxydConfException
import swift_scality_backend.http_utils
import swift_scality_backend.splice_utils
from swift_scality_backend import utils

urllib3 = utils.get_urllib3()


class SproxydFileSystem(object):
    """A sproxyd file system scheme."""

    def __init__(self, conf, logger):
        self.logger = logger
        self.conn_timeout = float(conf.get('sproxyd_conn_timeout', 10))
        self.proxy_timeout = float(conf.get('sproxyd_proxy_timeout', 3))

        path = conf.get('sproxyd_path', '/proxy/chord')
        self.base_path = '/%s/' % path.strip('/')

        self.healthcheck_threads = []
        self.sproxyd_hosts_set = set()
        hosts = conf.get('sproxyd_host', 'localhost:81').strip(',')
        for host in hosts.split(","):
            ip_addr, port = host.strip().split(':')
            self.sproxyd_hosts_set.add((ip_addr, int(port)))

            url = 'http://%s:%d%s.conf' % (ip_addr, int(port), self.base_path)
            ping_url = functools.partial(self.ping, url)
            on_up = functools.partial(self.on_sproxyd_up, ip_addr, int(port))
            on_down = functools.partial(self.on_sproxyd_down, ip_addr, int(port))
            thread = eventlet.spawn(utils.monitoring_loop, ping_url, on_up, on_down)
            self.healthcheck_threads.append(thread)

        self.sproxyd_hosts = itertools.cycle(list(self.sproxyd_hosts_set))

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

        timeout = urllib3.Timeout(connect=self.conn_timeout,
                                  read=self.proxy_timeout)
        # One HTTP Connection pool per sproxyd host
        self.http_pools = urllib3.PoolManager(len(self.sproxyd_hosts_set),
                                              timeout=timeout, retries=False,
                                              maxsize=32)

    def ping(self, url):
        """Retrieves the Sproxyd active configuration for health checking."""
        try:
            timeout = urllib3.Timeout(1)
            conf = self.http_pools.request('GET', url, timeout=timeout)
            return utils.is_sproxyd_conf_valid(conf.data)
        except (IOError, urllib3.exceptions.HTTPError) as exc:
            self.logger.info("Could not read Sproxyd configuration at %s "
                             "due to a network error: %r", url, exc)
        except SproxydConfException as exc:
            self.logger.warning("Sproxyd configuration at %s is invalid: "
                                "%s", url, exc)
        except:
            self.logger.exception("Unexpected exception during Sproxyd "
                                  "health check of %s", url)

        return False

    def on_sproxyd_up(self, host, port):
        self.logger.info("Sproxyd connector at %s:%d is up", host, port)
        self.sproxyd_hosts_set.add((host, port))
        self.sproxyd_hosts = itertools.cycle(list(self.sproxyd_hosts_set))
        self.logger.debug('sproxyd_hosts_set is now: %r', self.sproxyd_hosts_set)

    def on_sproxyd_down(self, host, port):
        self.logger.warning("Sproxyd connector at %s:%d is down " +
                            "or misconfigured", host, port)
        self.sproxyd_hosts_set.remove((host, port))
        self.sproxyd_hosts = itertools.cycle(list(self.sproxyd_hosts_set))
        self.logger.debug('sproxyd_hosts_set is now: %r', self.sproxyd_hosts_set)

    def __del__(self):
        for thread in self.healthcheck_threads:
            try:
                thread.kill()
            except:
                msg = "Exception while killing healthcheck thread"
                self.logger.exception(msg)

    def __repr__(self):
        ret = 'SproxydFileSystem(conn_timeout=%r, proxy_timeout=%r, ' + \
            'base_path=%r, hosts_list=%r)'
        return ret % (
            self.conn_timeout, self.proxy_timeout, self.base_path,
            self.sproxyd_hosts_set)

    @utils.trace
    def _do_http(self, caller_name, handlers, method, path, headers=None):
        '''Common code for handling a single HTTP request

        Handler functions passed through `handlers` will be called with the HTTP
        response object.

        :param caller_name: Name of the caller function, used in exceptions
        :type caller_name: `str`
        :param handlers: Dictionary mapping HTTP response codes to handlers
        :type handlers: `dict` of `int` to `callable`
        :param method: HTTP request method
        :type method: `str`
        :param path: HTTP request path
        :type path: `str`
        :param headers: HTTP request headers
        :type headers: `dict` of `str` to `str`

        :raises SproxydHTTPException: Received an unhandled HTTP response
        '''

        address, port = self.sproxyd_hosts.next()
        pool = self.http_pools.connection_from_host(address, port)
        safe_path = self.base_path + urllib.quote(path)

        def unexpected_http_status(response):
            message = response.data

            raise SproxydHTTPException(
                '%s: %s' % (caller_name, message),
                ipaddr=address, port=port,
                path=safe_path,
                http_status=response.status,
                http_reason=response.reason)

        response = pool.request(method, safe_path, headers=headers, preload_content=False)

        self.logger.debug('The HTTP connection pool to %s:%d serviced %d '
                          'requests. Its max size ever is %d.', pool.host,
                          pool.port, pool.num_requests, pool.num_connections)

        handler = handlers.get(response.status, unexpected_http_status)
        result = handler(response)

        # If the handler returns a generator, it must handle the connection
        # cleanup.
        if not isinstance(result, types.GeneratorType):
            try:
                swift_scality_backend.http_utils.drain_connection(response)
                response.release_conn()
            except Exception as exc:
                self.logger.error("Unexpected exception while releasing an "
                                  "HTTP connection to %s:%d: %r", pool.host,
                                  pool.port, exc)

        return result

    @utils.trace
    def get_meta(self, name):
        """Open a connection and get usermd."""

        def handle_200(response):
            header = response.getheader('x-scal-usermd')
            pickled = base64.b64decode(header)
            return pickle.loads(pickled)

        def handle_404(response):
            pass

        handlers = {
            200: handle_200,
            404: handle_404,
        }

        return self._do_http('get_meta', handlers, 'HEAD', name)

    @utils.trace
    def put_meta(self, name, metadata):
        """Connect to sproxyd and put usermd."""
        if metadata is None:
            raise SproxydHTTPException("no usermd")

        headers = {
            'x-scal-cmd': 'update-usermd',
            'x-scal-usermd': base64.b64encode(pickle.dumps(metadata)),
        }

        def handle_200(response):
            pass

        handlers = {
            200: handle_200,
        }

        result = self._do_http('put_meta', handlers, 'PUT', name, headers)

        self.logger.debug(
            "Metadata stored for %s%s : %s", self.base_path, name, metadata)

        return result

    @utils.trace
    def del_object(self, name):
        """Connect to sproxyd and delete object."""

        def handle_200_or_404(response):
            pass

        handlers = {
            200: handle_200_or_404,
            404: handle_200_or_404,
        }

        return self._do_http('del_object', handlers, 'DELETE', name)

    @utils.trace
    def get_object(self, name, headers=None):
        """Connect to sproxyd and get an object."""

        def handle_200_or_206(response):
            for chunk in response.stream(amt=1024 * 64):
                yield chunk
            response.release_conn()

        handlers = {
            200: handle_200_or_206,
            206: handle_200_or_206
        }

        return self._do_http('get_object', handlers, 'GET', name, headers)

    @utils.trace
    def get_diskfile(self, account, container, obj, **kwargs):
        """Get a diskfile."""
        return DiskFile(self, account, container, obj)


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
        self.logger.debug("DiskFileWriter for %s initialized", self.safe_path)

        (ipaddr, port) = self._filesystem.sproxyd_hosts.next()
        with swift.common.exceptions.ConnectionTimeout(filesystem.conn_timeout):
            self._conn = swift.common.bufferedhttp.http_connect_raw(
                ipaddr, port, 'PUT', self.safe_path, headers)

    def __repr__(self):
        ret = 'DiskFileWriter(filesystem=%r, object_name=%r)'
        return ret % (self._filesystem, self._name)

    logger = property(lambda self: self._filesystem.logger)

    @property
    def safe_path(self):
        return self._filesystem.base_path + urllib.quote(self._name)

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
        with contextlib.closing(self._conn):
            resp = self._conn.getresponse()
            if resp.status != 200:
                msg = resp.read()
                raise SproxydHTTPException("putting: %s / %s" % (
                    str(resp.status), str(msg)))

        metadata['name'] = self._name
        self.logger.debug("Data successfully written for object : %s", self.safe_path)
        self._filesystem.put_meta(self._name, metadata)


class DiskFileReader(object):
    """A simple sproxyd pass-through

    Encapsulation of the read context for servicing GET REST API
    requests. Serves as the context manager object for DiskFile's reader()
    method.

    :param filesystem: internal file system object to use
    :param name: object name
    """
    def __init__(self, filesystem, name):
        self._filesystem = filesystem
        self._name = name

    def __repr__(self):
        ret = 'DiskFileReader(filesystem=%r, object_name=%r)'
        return ret % (self._filesystem, self._name)

    logger = property(lambda self: self._filesystem.logger)

    @property
    def safe_path(self):
        return self._filesystem.base_path + urllib.quote(self._name)

    @utils.trace
    def __iter__(self):
        return self._filesystem.get_object(self._name)

    @utils.trace
    def can_zero_copy_send(self):
        return self._filesystem.use_splice

    @utils.trace
    def zero_copy_send(self, wsockfd):
        (ipaddr, port) = self._filesystem.sproxyd_hosts.next()
        conn = None

        with swift.common.exceptions.ConnectionTimeout(
                self._filesystem.conn_timeout):
            conn = swift_scality_backend.http_utils.SomewhatBufferedHTTPConnection(
                '%s:%s' % (ipaddr, port))

            try:
                conn.putrequest('GET', self.safe_path, skip_host=False)
                conn.endheaders()
            except:
                conn.close()
                raise

        with conn:
            resp = conn.getresponse()

            if resp.status != httplib.OK:
                raise SproxydHTTPException(
                    'Unexpected response code: %s' % resp.status,
                    ipaddr=ipaddr, port=port, path=self.safe_path,
                    http_status=resp.status, http_reason=resp.reason)

            if resp.chunked:
                raise SproxydHTTPException(
                    'Chunked response not supported',
                    ipaddr=ipaddr, port=port, path=self.safe_path,
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

        return self._filesystem.get_object(self._name, headers)

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
    """

    def __init__(self, filesystem, account, container, obj):
        self._name = '/'.join((account, container, obj))
        self._metadata = None
        self._filesystem = filesystem

        self._account = account
        self._container = container
        self._obj = obj

    logger = property(lambda self: self._filesystem.logger)

    def __repr__(self):
        ret = 'DiskFile(filesystem=%r, account=%r, container=%r, obj=%r)'
        return ret % (self._filesystem, self._account, self._container,
                      self._obj)

    @utils.trace
    def open(self):
        """Open the file and read the metadata.

        This method must populate the _metadata attribute.
        :raises DiskFileDeleted: if it does not exist
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
        dr = DiskFileReader(self._filesystem, self._name)
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
