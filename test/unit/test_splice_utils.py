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

import errno
import fcntl
import os
try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

import eventlet
import mock

import swift.common.utils

import swift_scality_backend.splice_utils

import utils


def _test_splice_socket_to_socket(test_length):
    # Linux 2.6 (RHEL6) doesn't have this procfs entry
    # Whilst the code in `splice_utils` handles this gracefully, the tests used
    # to fail because of this.
    try:
        with open('/proc/sys/fs/pipe-max-size', 'r') as fd:
            max_size = int(fd.read().strip())
    except IOError as exc:
        if exc.errno == errno.ENOENT:
            (rpipe, wpipe) = os.pipe()
            try:
                max_size = fcntl.fcntl(
                    rpipe, swift_scality_backend.splice_utils.F_GETPIPE_SZ)
            except IOError as exc:
                if exc.errno == errno.EINVAL:
                    max_size = \
                        swift_scality_backend.splice_utils.MAX_PIPE_SIZE_2_6_34
                else:
                    raise

            finally:
                os.close(rpipe)
                os.close(wpipe)
        else:
            raise

    orig_message = 'Hello, world!' * max_size

    if test_length:
        test_message = orig_message[:-2]
    else:
        test_message = orig_message

    test_message_length = len(test_message)

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
                client.fileno(), remote.fileno(), length=test_message_length)
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
    client.sendall(orig_message)
    client.close()

    thread1.wait()
    thread2.wait()

    assert result.getvalue() == test_message


try:
    import swift.common.splice
    HAS_SPLICE = swift.common.splice.splice.available
except ImportError:
    try:
        HAS_SPLICE = swift.common.utils.system_has_splice
    except AttributeError:
        HAS_SPLICE = False


@utils.skipIf(not HAS_SPLICE, "No `splice` support")
def test_splice_socket_to_socket():
    return _test_splice_socket_to_socket(test_length=False)


@utils.skipIf(not HAS_SPLICE, "No `splice` support")
def test_splice_socket_to_socket_bounded():
    return _test_splice_socket_to_socket(test_length=True)


@utils.skipIf(not HAS_SPLICE, "No `splice` support")
def test_splice_no_pipe_max_size():
    '''Test absence of `/proc/sys/fs/pipe-max-size`.'''

    open_mock = mock.mock_open()

    default_open = open

    def fake_open(name, *args, **kwargs):
        if name == '/proc/sys/fs/pipe-max-size':
            raise IOError(errno.ENOENT, 'No such file or directory')
        else:
            return default_open(name, *args, **kwargs)

    open_mock.side_effect = fake_open

    with mock.patch('__builtin__.open', open_mock):
        with mock.patch('fcntl.fcntl', side_effect=fcntl.fcntl) as mock_fcntl:
            swift_scality_backend.splice_utils.MAX_PIPE_SIZE = None

            try:
                _test_splice_socket_to_socket(test_length=True)

                mps = swift_scality_backend.splice_utils.MAX_PIPE_SIZE
            finally:
                swift_scality_backend.splice_utils.MAX_PIPE_SIZE = None

            assert mps == 0, 'Unexpected MAX_PIPE_SIZE value: %d' % mps

            assert mock_fcntl.call_count == 2, 'Unexpected fcntl call count'

            for (args, kwargs) in mock_fcntl.call_args_list:
                cmd = args[1]
                assert \
                    cmd == swift_scality_backend.splice_utils.F_GETPIPE_SZ, \
                    'Unexpected fcntl call: %d' % cmd
