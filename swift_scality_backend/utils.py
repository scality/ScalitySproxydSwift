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

import contextlib
import inspect
import logging
import functools
import re
import sys

import eventlet
import pkg_resources

import swift_scality_backend
import swift_scality_backend.afd
from swift_scality_backend.exceptions import SproxydConfException, \
    InvariantViolation

DEFAULT_LOGGER = logging.getLogger(__name__)

# This regex should work with Sproxyd configuration whether its
# format is JSON (Ring 5+) or INI (Ring 4)
BY_PATH_ENABLED_RE = re.compile(r'^\s*"?by_path_enabled[":=]+\s*(1|true)[",]*\s*$',
                                flags=re.IGNORECASE)

# A Dict which values are `pkg_resources.Requirement` objects
REQUIRES = {req.project_name: req for req in pkg_resources.parse_requirements(
            swift_scality_backend.__requires__)}


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
        # log level isn't enabled anyway
        if not logger.isEnabledFor(logging.DEBUG):
            return f(*args, **kwargs)

        # Get & bump call identifier, assume non-preemptive threading
        ctid, tid[0] = tid[0], tid[0] + 1

        all_args = inspect.getcallargs(f, *args, **kwargs)
        logger.debug('==> %s (%d): call %r', name, ctid, all_args)

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


@contextlib.contextmanager
def import_specific(*requirements):
    '''Temporarily change import path to satisfy some requirements.

    `requirements` can be strings or `pkg_resources.Requirement` objects,
    specifying the distributions and versions required.
    '''
    saved_sys_path = list(sys.path)
    path_to_add = set()
    for requirement in requirements:
        distribution = pkg_resources.get_distribution(requirement)
        path_to_add.add(distribution.location)
    for path in path_to_add:
        sys.path.insert(0, path)
    try:
        yield
    finally:
        sys.path[:] = saved_sys_path


def get_urllib3():
    '''Returns the urllib3 library that matches our requirement.'''
    import urllib3
    if urllib3.__version__ not in REQUIRES['urllib3']:
        DEFAULT_LOGGER.info("The default python-urllib3 library on this "
                            "system is not new enough. The one installed from "
                            "PyPi will be used.")
        for mod in [_ for _ in sys.modules.keys() if _.startswith('urllib3')]:
            del(sys.modules[mod])
        with import_specific(REQUIRES['urllib3']):
            import urllib3

    return urllib3
