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

'''Tests for `swift_scality_backend.http_utils`'''

import contextlib
import os
import socket
import sys
import unittest

import eventlet
import mock

import swift_scality_backend.http_utils


class TestSomewhatBufferedFileObject(unittest.TestCase):
    @contextlib.contextmanager
    def _make_socket(self, addr=None, buffsize=8):
        if not addr:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        else:
            sock = eventlet.connect(addr)

        try:
            yield swift_scality_backend.http_utils.SomewhatBufferedFileObject(
                sock, 'rb', buffsize)
        finally:
            sock.close()

    def test_flush(self):
        with self._make_socket() as sock:
            try:
                ret = sock.flush()
            except NotImplementedError:
                self.fail("Socket flush should not raised a NotImplementedError")
            self.assertTrue(ret is None)

    def test_fail_write(self):
        with self._make_socket() as sock:
            self.assertRaises(NotImplementedError, sock.write, 'abc')

    def test_fail_writelines(self):
        with self._make_socket() as sock:
            self.assertRaises(NotImplementedError, sock.writelines, [])

    def test_fail_call_get_buffered_twice(self):
        with self._make_socket() as sock:
            sock.get_buffered()
            self.assertRaises(
                swift_scality_backend.http_utils.InvariantViolation,
                sock.get_buffered)

    def test_fail_after_get_buffered_read(self):
        with self._make_socket() as sock:
            sock.get_buffered()
            self.assertRaises(
                swift_scality_backend.http_utils.InvariantViolation,
                sock.read)

    def test_fail_after_get_buffered_readline(self):
        with self._make_socket() as sock:
            sock.get_buffered()
            self.assertRaises(
                swift_scality_backend.http_utils.InvariantViolation,
                sock.readline)

    def test_fail_after_get_buffered_readlines(self):
        with self._make_socket() as sock:
            sock.get_buffered()
            self.assertRaises(
                swift_scality_backend.http_utils.InvariantViolation,
                sock.readlines)

    def test_fail_after_get_buffered_next(self):
        with self._make_socket() as sock:
            sock.get_buffered()
            self.assertRaises(
                swift_scality_backend.http_utils.InvariantViolation,
                sock.next)

    def test_interaction(self):
        server_socket = eventlet.listen(('127.0.0.1', 0))

        data = 'abcdefgh\n' * 10

        def server(client, _):
            client.sendall(data)
            client.close()

        server_thread = eventlet.spawn(
            eventlet.serve, server_socket, server)

        try:
            with self._make_socket(
                    server_socket.getsockname()) as sock:
                fst = sock.readline()
                self.assertEqual(fst, 'abcdefgh\n')
                snd = sock.read(size=3)
                self.assertEqual(snd, 'abc')
                buf = sock.get_buffered()
                self.assertEqual(buf, 'defg')

                rest = [buf]
                while True:
                    s = os.read(sock.fileno(), 32)
                    if len(s) == 0:
                        break
                    rest.append(s)

                off = len(fst) + len(snd)
                self.assertEqual(''.join(rest), data[off:])
        finally:
            server_thread.kill()


class TestSomewhatBufferedHTTPConnection(unittest.TestCase):

    def test_discard_buffering_arg_on_py26(self):
        with mock.patch('httplib.HTTPResponse.__init__') \
                as mock_http_response_init:
            kwargs = dict(
                sock=None, debuglevel=0, strict=0, method=None, buffering=False)

            response_class = \
                swift_scality_backend.http_utils.SomewhatBufferedHTTPConnection.HTTPResponse
            response_obj = response_class(**kwargs)

            if sys.version_info < (2, 7):
                del kwargs['buffering']
            mock_http_response_init.assert_called_once_with(
                response_obj, **kwargs)
