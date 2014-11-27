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

'''Tests for `swift_scality_backend.splice_utils`'''

import unittest
try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

import eventlet

import swift.common.utils

import swift_scality_backend.splice_utils


def _test_splice_socket_to_socket(test_length):
    with open('/proc/sys/fs/pipe-max-size', 'r') as fd:
        max_size = int(fd.read().strip())

    message = 'Hello, world!' * max_size
    test_message = message[:-2]
    test_length = len(test_message)

    server1 = eventlet.listen(('127.0.0.1', 0))
    addr1 = server1.getsockname()

    server2 = eventlet.listen(('127.0.0.1', 0))
    addr2 = server2.getsockname()

    result = StringIO.StringIO()

    def run_server1(sock):
        (client, addr) = sock.accept()

        remote = eventlet.connect(addr2)

        if test_length:
            swift_scality_backend.splice_utils.splice_socket_to_socket(
                client.fileno(), remote.fileno(), length=test_length)
        else:
            swift_scality_backend.splice_utils.splice_socket_to_socket(
                client.fileno(), remote.fileno())

    def run_server2(sock):
        (client, addr) = sock.accept()

        while True:
            data = client.recv(max_size)

            if len(data) == 0:
                break

            result.write(data)

    thread1 = eventlet.spawn(run_server1, server1)
    thread2 = eventlet.spawn(run_server2, server2)

    client = eventlet.connect(addr1)
    client.sendall(message)
    client.close()

    thread1.wait()
    thread2.wait()

    if test_length:
        assert result.getvalue() == test_message
    else:
        assert result.getvalue() == message


@unittest.skipUnless(
    getattr(swift.common.utils, 'system_has_splice', lambda: False)(),
    'No `splice` support')
def test_splice_socket_to_socket():
    return _test_splice_socket_to_socket(test_length=False)


@unittest.skipUnless(
    getattr(swift.common.utils, 'system_has_splice', lambda: False)(),
    'No `splice` support')
def test_splice_socket_to_socket_bounded():
    return _test_splice_socket_to_socket(test_length=True)
