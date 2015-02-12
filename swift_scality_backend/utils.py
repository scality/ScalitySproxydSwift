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

import inspect
import logging
import functools
import sys
import re

import eventlet

import swift_scality_backend.afd
from swift_scality_backend.exceptions import SproxydConfException, \
    InvariantViolation

DEFAULT_LOGGER = logging.getLogger(__name__)

# This regex should work with Sproxyd configuration whether its
# format is JSON (Ring 5+) or INI (Ring 4)
BY_PATH_ENABLED_RE = re.compile(r'^\s*"?by_path_enabled[":=]+\s*(1|true)[",]*\s*$',
                                flags=re.IGNORECASE)


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


def monitoring_loop(ping, on_up, on_down):
    '''Generic monitoring loop that calls a callback on state change.'''
    afd = swift_scality_backend.afd.AccrualFailureDetector()
    # Flag to force a call to on_* at initialization
    is_operational = None
    while True:
        if ping():
            afd.heartbeat()
        # If ping succeeds, we must never call `on_down` even though
        # `afd.isDead` might be True because the AFD has not enough data
        elif afd.isDead() and is_operational in (True, None):
            on_down()
            is_operational = False
        if afd.isAlive() and is_operational in (False, None):
            on_up()
            is_operational = True
        eventlet.sleep(1)
    raise InvariantViolation('The `monitoring_loop` has quit')


def is_sproxyd_conf_valid(conf):
    '''Check the Sproxyd configuration is valid to use with the Swift driver

    @param conf: Sproxyd configuration
    @type conf: `str`
    '''

    for line in conf.split("\n"):
        if BY_PATH_ENABLED_RE.match(line):
            return True

    raise SproxydConfException("Make sure by_path_enabled is set "
                               "and check Sproxyd logs.")
