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

'''Utility functions to work with `splice`'''

import fcntl
import logging
import os

import eventlet.hubs

import swift.common.utils

# From `bits/fcntl-linux.h`
F_GETPIPE_SZ = 1032
MAX_PIPE_SIZE = None


def splice_socket_to_socket(fd_in, fd_out, length=None):
    global MAX_PIPE_SIZE

    if MAX_PIPE_SIZE is None:
        try:
            with open('/proc/sys/fs/pipe-max-size', 'r') as fd:
                MAX_PIPE_SIZE = int(fd.read().strip())
        except:
            logging.getLogger(__name__).exception(
                'Unable to read max_pipe_size')

            MAX_PIPE_SIZE = 0

    flags = swift.common.utils.SPLICE_F_MOVE | \
        swift.common.utils.SPLICE_F_NONBLOCK | \
        swift.common.utils.SPLICE_F_MORE

    rpipe, wpipe = os.pipe()

    try:
        max_size = 0
        if MAX_PIPE_SIZE > 0:
            max_size = fcntl.fcntl(rpipe, swift.common.utils.F_SETPIPE_SZ,
                                   MAX_PIPE_SIZE)
        else:
            max_size = fcntl.fcntl(rpipe, F_GETPIPE_SZ)

        assert max_size != 0, 'Calculating max_size failed'

        while (True if length is None else length > 0):
            if length is None:
                max_read_size = max_size
            else:
                max_read_size = min(max_size, length)

            # TODO Change this once https://review.openstack.org/#/c/135319/
            # lands (0/None, EAGAIN handling etc)!
            read = swift.common.utils.splice(fd_in, 0, wpipe, 0, max_read_size,
                                             flags)

            if read is None:
                # EAGAIN
                eventlet.hubs.trampoline(fd_in, read=True)
                continue

            if read == 0:
                # EOF
                break

            todo = read

            while todo > 0:
                # TODO Same as above
                written = swift.common.utils.splice(rpipe, 0, fd_out, 0, todo,
                                                    flags)

                if written is None:
                    # EAGAIN
                    eventlet.hubs.trampoline(fd_out, write=True)
                    continue

                if written == 0:
                    raise ValueError('I/O operation on closed socket')

                todo -= written

            if length is not None:
                length -= read
    finally:
        os.close(rpipe)
        os.close(wpipe)
