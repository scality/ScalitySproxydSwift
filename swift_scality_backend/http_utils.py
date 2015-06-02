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

'''HTTP client utilities'''

import httplib
import operator
import socket
import sys

import eventlet.greenio

from scality_sproxyd_client.exceptions import InvariantViolation
from scality_sproxyd_client.exceptions import SproxydHTTPException


class SomewhatBufferedFileObject(socket._fileobject):
    '''A 'somewhat buffered' file-like object

    This works similar to `socket._fileobject`, which is what you get when
    calling `socket.socket.makefile`, but this one has a couple of differences:

    - All `write`-related methods are removed (raise `NotImplementedError`)
    - It's possible to retrieve the content of the buffer *once*
    - Once the content of the backing buffer has been retrieved, any
      `read`-related method will fail (raise `InvariantViolation`)
    '''

    def get_buffered(self):
        '''Retrieve the buffered data'''

        if self._rbuf is None:
            raise InvariantViolation('Using `get_buffered` more than once')

        value = self._rbuf.getvalue()
        self._rbuf = None
        return value

    def flush(self):
        # We can't raise NotImplementedError here because when an HTTPResponse
        # object is closed, flush() is called
        pass

    def write(self, data):
        raise NotImplementedError

    def writelines(self, lines):
        raise NotImplementedError

    def read(self, size=-1):
        if self._rbuf is None:
            raise InvariantViolation('Using `read` after `get_buffered`')

        return socket._fileobject.read(self, size)

    def readline(self, size=-1):
        if self._rbuf is None:
            raise InvariantViolation('Using `readline` after `get_buffered`')

        return socket._fileobject.readline(self, size)

    def readlines(self, sizehint=0):
        if self._rbuf is None:
            raise InvariantViolation('Using `readllines` after `get_buffered`')

        return socket._fileobject.readlines(self, sizehint)

    def next(self):
        if self._rbuf is None:
            raise InvariantViolation('Using `next` after `get_buffered`')

        return socket._fileobject.next(self)


class SomewhatBufferedHTTPConnection(httplib.HTTPConnection):
    '''A somewhat buffered HTTP connection

    The response type used by this class wraps the underlying socket in a
    `SomewhatBufferedFileObject`.
    '''

    class HTTPResponse(httplib.HTTPResponse):
        '''Like `httplib.HTTPResponse, but with its `fp` attribute wrapped in a
        `SomewhatBufferedFileObject`
        '''
        def __init__(self, sock, debuglevel=0, strict=0, method=None,
                     buffering=False):
            init_args = {
                'sock': sock,
                'debuglevel': debuglevel,
                'strict': strict,
                'method': method,
            }
            if sys.version_info >= (2, 7):
                init_args.update({
                    'buffering': buffering,
                })

            # Prevent makefile calling by stdlib
            makefile = socket._socketobject.makefile
            socket._socketobject.makefile = lambda *_, **_2: None
            try:
                httplib.HTTPResponse.__init__(self, **init_args)
            finally:
                socket._socketobject.makefile = makefile
            # Handle eventlet subtlety
            # https://github.com/eventlet/eventlet/blob/master/eventlet/greenio/base.py#L295
            if isinstance(sock, eventlet.greenio.GreenSocket):
                real_sock = sock.dup()
            else:
                real_sock = sock._sock

            # Fetching in chunks of 1024 bytes seems like a sensible value,
            # since we want to retrieve as little more than the HTTP headers as
            # possible.
            self.fp = SomewhatBufferedFileObject(real_sock, 'rb', 1024)

            # Eventlet finalizing
            # https://github.com/eventlet/eventlet/blob/master/eventlet/greenio/base.py#L299
            if hasattr(real_sock, '_drop') and isinstance(real_sock, eventlet.greenio.GreenSocket):
                real_sock._drop()

        if not hasattr(httplib.HTTPResponse, 'fileno'):
            # py26 compat
            def fileno(self):
                return self.fp.fileno()

    response_class = HTTPResponse

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()


class NoClientAvailable(RuntimeError):
    '''Exception raised when no client with alive endpoints is available'''


class ClientCollection(object):
    '''A collection of back-end connections'''

    def __init__(self, read_clients, write_clients):
        # It's more list rather than tuple, but there's no persistent list in
        # Python
        self._read_clients = tuple(read_clients)
        self._write_clients = tuple(write_clients)

    read_clients = property(operator.attrgetter('_read_clients'))
    write_clients = property(operator.attrgetter('_write_clients'))

    def __repr__(self):
        return 'ClientCollection(read_clients=%r, write_clients=%r)' % \
            (self.read_clients, self.write_clients)

    def __eq__(self, other):
        if isinstance(other, ClientCollection):
            return all([
                self.read_clients == other.read_clients,
                self.write_clients == other.write_clients,
            ])
        else:
            return NotImplemented

    def __ne__(self, other):
        equal = self.__eq__(other)

        if equal is NotImplemented:
            return NotImplemented
        else:
            return not equal

    def __hash__(self):
        return hash((self.read_clients, self.write_clients))

    @staticmethod
    def _get_client(clients):
        '''Retrieve a client with alive endpoints from a collection of clients

        :param clients: Client collection
        :type clients: iterable of `scality_sproxyd_client.sproxyd_client.SproxydClient`

        :return: A client with alive endpoints
        :rtype: `scality_sproxyd_client.sproxyd_client.SproxydClient`

        :raise NoClientAvailable: No client with available endpoints found
        '''

        for client in clients:
            if client.has_alive_endpoints:
                return client

        raise NoClientAvailable('No alive endpoints available')

    def get_read_client(self):
        '''Return a client at which read operations can be performed

        :return: A client usable for read operations
        :rtype: `scality_sproxyd_client.sproxyd_client.SproxydClient`

        :raise NoClientAvailable: No client with available endpoints found
        '''

        return self._get_client(self.read_clients)

    def get_write_client(self):
        '''Return a client at which write operations can be performed

        :return: A client usable for write operations
        :rtype: `scality_sproxyd_client.sproxyd_client.SproxydClient`

        :raise NoClientAvailable: No client with available endpoints found
        '''

        return self._get_client(self.write_clients)

    def try_read(self, fn):
        '''Attempt a read operation and fallback to write endpoints on 404

        This utility method attempts to retrieve a read endpoint client, and
        call the given function. If it raises an `SproxydHTTPException` with
        `http_status` 404, it falls back to looking up a write endpoint and
        retry the action.

        Other exceptions are passed through.

        :param fn: Callable to attempt the operation
        :type fn: `callable` which takes a `scality_sproxyd_client.sproxyd_client.SproxydClient`

        :return: Result of `fn`

        :raise NoClientAvailable: No client with alive endpoints available
        '''

        try:
            client = self.get_read_client()
        except NoClientAvailable:
            pass
        else:
            try:
                return fn(client)
            except SproxydHTTPException as exc:
                if exc.http_status == 404:
                    pass
                else:
                    raise

        client = self.get_write_client()
        return fn(client)
