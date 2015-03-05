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

'''HTTP client utilities'''

import httplib
import socket
import sys

from scality_sproxyd_client.exceptions import InvariantViolation


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

            httplib.HTTPResponse.__init__(self, **init_args)

            # Fetching in chunks of 1024 bytes seems like a sensible value,
            # since we want to retrieve as little more than the HTTP headers as
            # possible.
            self.fp = SomewhatBufferedFileObject(sock, 'rb', 1024)

    response_class = HTTPResponse

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()
