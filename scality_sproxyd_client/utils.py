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
import logging
import re
import sys

import eventlet
import pkg_resources

import scality_sproxyd_client
import scality_sproxyd_client.afd
from scality_sproxyd_client.exceptions import SproxydConfException, \
    InvariantViolation


# This regex should work with Sproxyd configuration whether its
# format is JSON (Ring 5+) or INI (Ring 4)
BY_PATH_ENABLED_RE = re.compile(r'^\s*"?by_path_enabled[":=]+\s*(1|true)[",]*\s*$',
                                flags=re.IGNORECASE)

# A Dict which values are `pkg_resources.Requirement` objects
REQUIRES = dict((req.project_name, req)
                for req in pkg_resources.parse_requirements(
                    scality_sproxyd_client.__requires__))

DEFAULT_LOGGER = logging.getLogger(__name__)


def monitoring_loop(ping, on_up, on_down):
    '''Generic monitoring loop that calls a callback on state change.'''
    afd = scality_sproxyd_client.afd.AccrualFailureDetector()
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
    '''Check that the Sproxyd configuration has 'query by path' enabled

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
