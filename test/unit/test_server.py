# Copyright (c) 2014, Scality
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

'''Tests for `swift_scality_backend.server`'''

import inspect

import swift.obj.server

import swift_scality_backend.diskfile
import swift_scality_backend.server


def test_api_compatible():
    '''
    Test whether `swift_scality_backend.server.app_factory`'s result is
    API-compatible with `swift.obj.server.app_factory`'s result
    '''
    whitelist = [
        'replication_failure_threshold',
        'replication_semaphore',
        'replication_failure_ratio',
    ]

    swift_server = swift.obj.server.app_factory({})
    conf = {'sproxyd_host': 'host1:81'}
    scality_server = swift_scality_backend.server.app_factory(conf)

    def assert_compatible(name, spec1, spec2):
        '''Assert argspecs are compatible'''

        if spec1.varargs:
            assert spec2.varargs, 'No varargs: %r' % name

        if spec1.keywords:
            assert spec2.keywords, 'No kwargs: %r' % name

        nargs1 = len(spec1.args)
        ndefs1 = len(spec1.defaults or [])
        nargs2 = len(spec2.args)
        ndefs2 = len(spec2.defaults or [])

        assert nargs2 >= nargs1, 'Less args: %r' % name
        assert nargs2 - ndefs2 <= nargs1 - ndefs1, \
            'Incompatible number of non-default args: %r' % name

        # The `policy` arg used to be named  `policy_index` or `policy_idx`
        # in Swift 2.1 and 2.2. It changed in Swift 2.3.
        # We rename the arg here to be keep compatibility and have the same
        # method signature excepted for the name of the `policy` argument.
        spec1_args, spec2_args = spec1.args, spec2.args
        replace = {'policy_idx': 'policy', 'policy_index': 'policy'}
        if name in ['get_diskfile', 'async_update']:
            spec1_args = [replace.get(arg, arg) for arg in spec1.args]
            spec2_args = [replace.get(arg, arg) for arg in spec2.args]

        assert spec1_args == spec2_args[:nargs1], \
            'Incompatible arg names: %r' % name

    def check_api_compatible(name):
        '''Check whether the API of a given method name is compatible'''

        assert hasattr(scality_server, name), 'Missing attribute: %r' % name

        swift_attr = getattr(swift_server, name)
        scality_attr = getattr(scality_server, name)

        if callable(swift_attr):
            assert callable(scality_attr), 'Not callable: %r' % name

            swift_spec = inspect.getargspec(swift_attr)
            scality_spec = inspect.getargspec(scality_attr)

            assert_compatible(name, swift_spec, scality_spec)

    for name in dir(swift_server):
        if name in whitelist or name[0] == '_':
            continue

        yield check_api_compatible, name


def test_setup_with_custom_timeout():
    conf = {'sproxyd_host': 'host1:81', 'sproxyd_proxy_timeout': "10.0",
            'sproxyd_conn_timeout': "4.1"}
    obj_serv = swift_scality_backend.server.app_factory(conf)

    assert obj_serv._filesystem.read_timeout == 10.0
    assert obj_serv._filesystem.conn_timeout == 4.1


def test_get_diskfile():
    conf = {'sproxyd_host': 'host1:81'}
    scality_server = swift_scality_backend.server.app_factory(conf)
    diskfile = scality_server.get_diskfile('dev', 'partition', 'a', 'c', 'o')

    assert isinstance(diskfile, swift_scality_backend.diskfile.DiskFile)
    assert diskfile._account == 'a'
    assert diskfile._container == 'c'
    assert diskfile._obj == 'o'
