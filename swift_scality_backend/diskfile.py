# Copyright (c) 2010-2013 OpenStack, LLC.
# Copyright (c) 2014 Scality
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
import urllib

import eventlet
import eventlet.green.os
import swift.common.bufferedhttp
import swift.common.exceptions
import swift.common.swob
import swift.common.utils

from swift_scality_backend.exceptions import SproxydHTTPException, \
    SproxydConfException
import swift_scality_backend.http_utils
import swift_scality_backend.splice_utils
from swift_scality_backend import utils


class SproxydFileSystem(object):
    """A sproxyd file system scheme."""

    def __init__(self, conf, logger):
        self.logger = logger
        self.conn_timeout = int(conf.get('sproxyd_conn_timeout', 10))
        self.proxy_timeout = int(conf.get('sproxyd_proxy_timeout', 3))

        path = conf.get('sproxyd_path', '/proxy/chord')
        self.base_path = '/%s/' % path.strip('/')

        self.healthcheck_threads = []
        self.sproxyd_hosts_set = set()
        hosts = conf.get('sproxyd_host', 'localhost:81')
        for host in hosts.strip(',').split(","):
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

    def ping(self, url):
        try:
            with eventlet.Timeout(1):
                conf = urllib.urlopen(url)
                return utils.is_sproxyd_conf_valid(conf.fp)
        except (IOError, httplib.HTTPException, eventlet.Timeout) as e:
            self.logger.info("Could not read Sproxyd configuration at %s "
                             "due to a network error: %r", url, e)
        except SproxydConfException as e:
            self.logger.warning("Sproxyd configuration at %s is invalid: "
                                "%r", url, e)
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
    def do_connect(self, ipaddr, port, method, path, headers=None):
        """stubable function for connecting."""
        safe_path = self.base_path + urllib.quote(path)
        conn = swift.common.bufferedhttp.http_connect_raw(
            ipaddr, port, method, safe_path, headers)
        return conn

    def conn_getresponse(self, conn):
        """stubable function for getting conn responses."""
        return conn.getresponse()

    @utils.trace
    def get_meta(self, name):
        """Open a connection and get usermd."""
        self.logger.debug("GET_meta " + self.base_path + name)
        headers = {}
        conn = None
        try:
            with swift.common.exceptions.ConnectionTimeout(self.conn_timeout):
                (ipaddr, port) = self.sproxyd_hosts.next()
                conn = self.do_connect(ipaddr, port, 'HEAD', name, headers)
            with eventlet.Timeout(self.proxy_timeout):
                resp = self.conn_getresponse(conn)
                if resp.status == 200:
                    headers = dict(resp.getheaders())
                    usermd = base64.b64decode(headers["x-scal-usermd"])
                    metadata = pickle.loads(usermd)
                elif resp.status == 404:
                    metadata = None
                else:
                    msg = resp.read()
                    raise SproxydHTTPException(
                        'get_meta: %s' % msg,
                        ipaddr=ipaddr, port=port,
                        path=self.base_path, http_status=resp.status,
                        http_reason=resp.reason)
        except EOFError:
            self.logger.debug("EOFError")
            return None
        finally:
            if conn:
                conn.close()
        self.logger.debug("Metadata retrieved for " + self.base_path + name + " : " + str(metadata))
        return metadata

    @utils.trace
    def put_meta(self, name, metadata):
        """Connect to sproxyd and put usermd."""
        self.logger.debug("PUT_meta " + self.base_path + name + " : " + str(metadata))
        if metadata is None:
            raise SproxydHTTPException("no usermd")
        headers = {}
        headers["x-scal-cmd"] = "update-usermd"
        usermd = pickle.dumps(metadata)
        headers["x-scal-usermd"] = base64.b64encode(usermd)
        conn = None
        try:
            with swift.common.exceptions.ConnectionTimeout(self.conn_timeout):
                (ipaddr, port) = self.sproxyd_hosts.next()
                conn = self.do_connect(ipaddr, port, 'PUT', name, headers)
            with eventlet.Timeout(self.proxy_timeout):
                resp = self.conn_getresponse(conn)
                if resp.status == 200:
                    resp.read()
                else:
                    msg = resp.read()
                    raise SproxydHTTPException(
                        'put_meta: %s' % msg,
                        ipaddr=ipaddr, port=port,
                        path=self.base_path, http_status=resp.status,
                        http_reason=resp.reason)
        finally:
            if conn:
                conn.close()
        self.logger.debug("Metadata stored for " + self.base_path + name + " : " + str(metadata))

    @utils.trace
    def del_object(self, name):
        """Connect to sproxyd and delete object."""
        self.logger.debug("del_object " + self.base_path + name)
        headers = {}
        conn = None
        try:
            with swift.common.exceptions.ConnectionTimeout(self.conn_timeout):
                (ipaddr, port) = self.sproxyd_hosts.next()
                conn = self.do_connect(ipaddr, port, 'DELETE', name, headers)
            with eventlet.Timeout(self.proxy_timeout):
                resp = self.conn_getresponse(conn)
                if resp.status == 200 or resp.status == 404:
                    resp.read()
                else:
                    msg = resp.read()
                    raise SproxydHTTPException(
                        'del_object: %s' % msg, ipaddr=ipaddr, port=port,
                        path=self.base_path, http_status=resp.status,
                        http_reason=resp.reason)
        finally:
            if conn:
                conn.close()

    @utils.trace
    def get_diskfile(self, account, container, obj, **kwargs):
        """Get a diskfile."""
        self.logger.debug("get_diskfile")
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
        headers = {}
        headers['transfer-encoding'] = "chunked"
        self._filesystem.logger.debug("PUT stream " + filesystem.base_path + name)
        with swift.common.exceptions.ConnectionTimeout(filesystem.conn_timeout):
            (ipaddr, port) = self._filesystem.sproxyd_hosts.next()
            self._conn = self._filesystem.do_connect(
                ipaddr, port, 'PUT', name, headers)

    logger = property(lambda self: self._filesystem.logger)

    def write(self, chunk):
        """Write a chunk of data

        :param chunk: the chunk of data to write as a string object
        """
        self._filesystem.logger.debug("writing " + self._filesystem.base_path + self._name)
        self._conn.send('%x\r\n%s\r\n' % (len(chunk), chunk))
        self._upload_size += len(chunk)
        return self._upload_size

    @utils.trace
    def put(self, metadata):
        """Make the final association

        :param metadata: dictionary of metadata to be written
        :param extension: extension to be used when making the file
        """
        self._conn.send('0\r\n\r\n')
        self._filesystem.logger.debug("write closing for : " + self._filesystem.base_path + self._name)
        try:
            resp = self._conn.getresponse()
            if resp.status != 200:
                msg = resp.read()
                raise SproxydHTTPException("putting: %s / %s" % (
                    str(resp.status), str(msg)))
        finally:
            self._conn.close()
        metadata['name'] = self._name
        self._filesystem.logger.debug("data successfully written for object : " + self._filesystem.base_path + self._name)
        self._filesystem.put_meta(self._name, metadata)


class DiskFileReader(object):
    """A simple sproxyd pass-through

    Encapsulation of the read context for servicing GET REST API
    requests. Serves as the context manager object for DiskFile's reader()
    method.

    :param filesystem: internal file system object to use
    :param name: object name
    :param obj_size: on-disk size of object in bytes
    :param etag: MD5 hash of object from metadata
    """
    def __init__(self, filesystem, name, obj_size, etag):
        self._filesystem = filesystem
        self._name = name
        #
        self._suppress_file_closing = False
        #
        self._filesystem.logger.debug("GET stream " +
                                      filesystem.base_path + name)
        self._conn = None

    logger = property(lambda self: self._filesystem.logger)

    @utils.trace
    def stream(self, resp):
        """stream input."""
        try:
            self._filesystem.logger.debug("Starting to read %s%s", self._filesystem.base_path, self._name)
            for chunk in iter(lambda: resp.read(65536), b''):
                yield chunk
            self._filesystem.logger.debug("EOF %s%s", self._filesystem.base_path, self._name)
        finally:
            if not self._suppress_file_closing:
                self.close()

    def __iter__(self):
        self._filesystem.logger.debug("__iter__ over " + self._filesystem.base_path + self._name)
        headers = {}

        with swift.common.exceptions.ConnectionTimeout(self._filesystem.conn_timeout):
            (ipaddr, port) = self._filesystem.sproxyd_hosts.next()
            self._conn = self._filesystem.do_connect(
                ipaddr, port, 'GET', self._name, headers)

        resp = self._conn.getresponse()
        for chunk in self.stream(resp):
            yield chunk

    @utils.trace
    def can_zero_copy_send(self):
        return self._filesystem.use_splice

    @utils.trace
    def zero_copy_send(self, wsockfd):
        (ipaddr, port) = self._filesystem.sproxyd_hosts.next()

        safe_path = self._filesystem.base_path + urllib.quote(self._name)

        with swift.common.exceptions.ConnectionTimeout(
                self._filesystem.conn_timeout):
            conn = swift_scality_backend.http_utils.SomewhatBufferedHTTPConnection(
                '%s:%s' % (ipaddr, port))
            conn.putrequest('GET', safe_path, skip_host=False)
            conn.endheaders()

            self._conn = conn

        resp = self._conn.getresponse()

        if resp.status != httplib.OK:
            raise SproxydHTTPException(
                'Unexpected response code: %s' % resp.status,
                ipaddr=ipaddr, port=port, path=safe_path,
                http_status=resp.status, http_reason=resp.reason)

        if resp.chunked:
            raise SproxydHTTPException(
                'Chunked response not supported',
                ipaddr=ipaddr, port=port, path=safe_path,
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
        self._filesystem.logger.debug("app_iter_range")
        headers = {}
        headers["range"] = "bytes=" + str(start) + "-" + str(stop)

        with swift.common.exceptions.ConnectionTimeout(self._filesystem.conn_timeout):
            (ipaddr, port) = self._filesystem.sproxyd_hosts.next()
            self._conn = self._filesystem.do_connect(
                ipaddr, port, 'GET', self._name, headers)

        resp = self._conn.getresponse()
        for chunk in self.stream(resp):
            yield chunk

    @utils.trace
    def app_iter_ranges(self, ranges, content_type, boundary, size):
        """iterate over multiple ranges."""
        self._filesystem.logger.debug("app_iter_ranges")
        if not ranges:
            yield ''
        else:
            try:
                self._suppress_file_closing = True
                for chunk in swift.common.swob.multi_range_iterator(
                        ranges, content_type, boundary, size,
                        self.app_iter_range):
                    yield chunk
            finally:
                self._suppress_file_closing = False
                try:
                    self.close()
                except swift.common.exceptions.DiskFileQuarantined:
                    pass

    @utils.trace
    def close(self):
        """Close the file.

        Will handle quarantining file if necessary.
        """
        self._filesystem.logger.debug("read closing for " + self._filesystem.base_path + self._name)
        if self._conn:
            self._conn.close()
            self._conn = None


class DiskFile(object):
    """A simple sproxyd pass-through

    :param mgr: DiskFileManager
    :param device_path: path to the target device or drive
    :param threadpool: thread pool to use for blocking operations
    :param partition: partition on the device in which the object lives
    :param account: account name for the object
    :param container: container name for the object
    :param obj: object name for the object
    :param keep_cache: caller's preference for keeping data read in the cache
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
        :raises DiskFileCollision: on name mis-match with metadata
        :raises DiskFileDeleted: if it does not exist, or a tombstone is
                                 present
        :raises DiskFileQuarantined: if while reading metadata of the file
                                     some data did pass cross checks
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

        The responsibility of closing the open file is passed to the
        DiskFileReader object.

        :param keep_cache:
        """
        dr = DiskFileReader(self._filesystem, self._name,
                            int(self._metadata['Content-Length']),
                            self._metadata['ETag'])
        # At this point the reader object is now responsible for
        # the file pointer.
        return dr

    @utils.trace
    @contextlib.contextmanager
    def create(self, size=None):
        """Context manager to create a file.

        :param size: optional initial size of file to explicitly allocate on
                     disk
        :raises DiskFileNoSpace: if a size is specified and allocation fails
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

        This creates a tombstone file with the given timestamp, and removes
        any older versions of the object file.  Any file that has an older
        timestamp than timestamp will be deleted.

        :param timestamp: timestamp to compare with each file
        """
        self._filesystem.del_object(self._name)
