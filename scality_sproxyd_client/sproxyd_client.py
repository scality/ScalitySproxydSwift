# Copyright (c) 2015 Scality
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

import base64
import eventlet
import functools
import itertools
import pickle
import types
import urllib

from scality_sproxyd_client import exceptions
from scality_sproxyd_client import utils

urllib3 = utils.get_urllib3()


def drain_connection(response):
    '''Read remaining data of the `Response` to 'clean' underlying socket.'''
    while response.read(64 * 1024):
        pass


class SproxydClient(object):
    """A sproxyd file system scheme."""

    def __init__(self, endpoints, conn_timeout, proxy_timeout, logger):
        self.endpoints = frozenset(endpoints)

        for endpoint in self.endpoints:
            if endpoint.params:
                raise ValueError(
                    'Endpoint with params not supported: %r' % endpoint)
            if endpoint.query:
                raise ValueError(
                    'Endpoint with query not supported: %r' % endpoint)
            if endpoint.fragment:
                raise ValueError(
                    'Endpoint with fragment not supported: %r' % endpoint)

        self.logger = logger
        self.conn_timeout = conn_timeout
        self._proxy_timeout = proxy_timeout

        self._pool_manager = urllib3.PoolManager(
            len(self.endpoints),
            timeout=urllib3.Timeout(connect=conn_timeout, read=proxy_timeout),
            retries=False, maxsize=32)

        self._alive = frozenset(self.endpoints)
        self._cycle = itertools.cycle(self._alive)

        self._healthcheck_threads = []

        for endpoint in self.endpoints:
            url = '%(scheme)s://%(netloc)s/%(path)s/.conf' % {
                  'scheme': endpoint.scheme,
                  'netloc': endpoint.netloc,
                  'path': endpoint.path.strip('/'),
            }

            ping_url = functools.partial(self._ping, url)
            on_up = functools.partial(self._on_sproxyd_up, endpoint)
            on_down = functools.partial(self._on_sproxyd_down, endpoint)
            thread = eventlet.spawn(utils.monitoring_loop, ping_url, on_up, on_down)
            self._healthcheck_threads.append(thread)

    def __repr__(self):
        return 'SproxydClient(%s)' % ', '.join('%s=%r' % attr for attr in [
            ('endpoints', self.endpoints),
            ('conn_timeout', self.conn_timeout),
            ('proxy_timeout', self._proxy_timeout),
            ('logger', self.logger),
        ])

    def _alter_alive(self, fn):
        self._alive = fn(self._alive)
        self._cycle = itertools.cycle(self._alive)

    def get_next_endpoint(self):
        return self._cycle.next()

    def _ping(self, url):
        """Retrieves the Sproxyd active configuration for health checking."""
        try:
            timeout = urllib3.Timeout(1)
            conf = self._pool_manager.request('GET', url, timeout=timeout)
            return utils.is_sproxyd_conf_valid(conf.data)
        except (IOError, urllib3.exceptions.HTTPError) as exc:
            self.logger.info("Could not read Sproxyd configuration at %s "
                             "due to a network error: %r", url, exc)
        except exceptions.SproxydConfException as exc:
            self.logger.warning("Sproxyd configuration at %s is invalid: "
                                "%s", url, exc)
        except:
            self.logger.exception("Unexpected exception during Sproxyd "
                                  "health check of %s", url)

        return False

    def _on_sproxyd_up(self, endpoint):
        self.logger.info("Sproxyd connector at %s is up", endpoint)
        self._alter_alive(lambda s: s.union([endpoint]))
        self.logger.debug('endpoints is now: %r', self.endpoints)

    def _on_sproxyd_down(self, endpoint):
        self.logger.warning("Sproxyd connector at %s is down " +
                            "or misconfigured", endpoint)
        self._alter_alive(lambda s: s.difference([endpoint]))
        self.logger.debug('endpoints is now: %r', self.endpoints)

    @property
    def has_alive_endpoints(self):
        '''Determine whether any client endpoints are alive

        :return: Client has alive endpoints
        :rtype: `bool`
        '''

        return len(self._alive) > 0

    def __del__(self):
        for thread in self._healthcheck_threads:
            try:
                thread.kill()
            except:
                msg = "Exception while killing healthcheck thread"
                self.logger.exception(msg)

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

        endpoint = self.get_next_endpoint()
        parts = endpoint.netloc.rsplit(':', 1)
        if len(parts) == 1:
            host = parts[0]
            port = None
        else:
            host, port = parts

        pool = self._pool_manager.connection_from_host(
            host, port=port, scheme=endpoint.scheme)
        safe_path = '/%s/%s' % (endpoint.path.strip('/'), urllib.quote(path))

        def unexpected_http_status(response):
            message = response.read()

            raise exceptions.SproxydHTTPException(
                '%s: %s' % (caller_name, message),
                ipaddr=host, port=port,
                path=safe_path,
                http_status=response.status,
                http_reason=response.reason)

        response = pool.request(method, safe_path, headers=headers, preload_content=False)
        handler = handlers.get(response.status, unexpected_http_status)
        result = handler(response)

        # If the handler returns a generator, it must handle the connection
        # cleanup.
        if not isinstance(result, types.GeneratorType):
            try:
                drain_connection(response)
                response.release_conn()
            except Exception as exc:
                self.logger.error("Unexpected exception while releasing an "
                                  "HTTP connection to %s:%d: %r", pool.host,
                                  pool.port, exc)

        return result

    def get_meta(self, name):
        """Open a connection and get usermd."""

        def handle_200(response):
            header = response.getheader('x-scal-usermd')
            pickled = base64.b64decode(header)
            return pickle.loads(pickled)

        def handle_404(response):
            return None

        handlers = {
            200: handle_200,
            404: handle_404,
        }

        return self._do_http('get_meta', handlers, 'HEAD', name)

    def put_meta(self, name, metadata):
        """Connect to sproxyd and put usermd."""
        if metadata is None:
            raise exceptions.SproxydHTTPException("no usermd")

        headers = {
            'x-scal-cmd': 'update-usermd',
            'x-scal-usermd': base64.b64encode(pickle.dumps(metadata)),
        }

        handlers = {
            200: lambda _: None,
        }

        result = self._do_http('put_meta', handlers, 'PUT', name, headers)

        self.logger.debug(
            "Metadata stored for %s: %s", name, metadata)

        return result

    def del_object(self, name):
        """Connect to sproxyd and delete object."""

        def handle_200_or_404(response):
            return None

        handlers = {
            200: handle_200_or_404,
            404: handle_200_or_404,
        }

        return self._do_http('del_object', handlers, 'DELETE', name)

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
