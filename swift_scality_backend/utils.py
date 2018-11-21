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

import functools
import inspect
import logging
import sys

DEFAULT_LOGGER = logging.getLogger(__name__)


def log_to_file(conf, name, log_to_console, log_route, fmt, logger, adapted_logger):
    '''
    Custom log handler function passed to the object-server conf in the CI
    so that the full backtraces are logged to /tmp/swift_logfile, instead
    of being truncated in journalctl
    '''
    fh = logging.FileHandler('/tmp/swift_logfile')
    fh.setLevel(logging.WARN)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)


def trace(f):
    '''Trace calls to a decorated function

    Using this decorator on a function will cause its execution to be logged at
    `DEBUG` level, including messages when the function is called (with a call
    identifier and the arguments), as well as when the function returns (also
    including the call identifier, and a representation of the return value or
    exception if applicable).
    '''

    # 'State' hack
    tid = [0]

    name = f.func_name

    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        maybe_self = None

        if len(args) > 0:
            maybe_self = args[0]
        else:
            maybe_self = kwargs.get('self', None)

        assert maybe_self is not None

        logger = getattr(maybe_self, 'logger', DEFAULT_LOGGER)

        # Fast-path, don't do any of the 'expensive' things below if the debug
        # log level isn't enabled anyway.
        # The conditional method lookup is for compatibility with the
        # `LoggerAdapter` implementation in Python 2.6, used by Swift, which
        # lacks the `isEnabledFor` method (added in Python 2.7). We assume the
        # log level is enabled in that case.
        if not getattr(logger, 'isEnabledFor', lambda _: True)(logging.DEBUG):
            return f(*args, **kwargs)

        # Get & bump call identifier, assume non-preemptive threading
        ctid, tid[0] = tid[0], tid[0] + 1

        if sys.version_info >= (2, 7):
            all_args = inspect.getcallargs(f, *args, **kwargs)
            logger.debug('==> %s (%d): call %r', name, ctid, all_args)
        else:
            logger.debug('==> %s (%d): call', name, ctid)

        result = None

        try:
            result = f(*args, **kwargs)
        except BaseException as exc:
            logger.debug('<== %s (%d): exception %r', name, ctid, exc)
            raise
        else:
            logger.debug('<== %s (%d): return %r', name, ctid, result)
            return result

    return wrapped


def split_list(val):
    '''Split a comma-separated string into a list of strings.'''

    return (s2 for s2 in
            (s1.strip() for s1 in val.split(','))
            if s2)
