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

import errno
import inspect
import os
import StringIO
import unittest
import urlparse

import mock

import swift.obj.server

import swift_scality_backend.diskfile
import swift_scality_backend.policy_configuration
import swift_scality_backend.server

from . import utils


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


def test_setup_with_sproxyd_endpoints():
    endpoint1 = 'http://h1:81/p'
    endpoint2 = 'http://h2:81/p'
    conf = {'sproxyd_endpoints': ' , '.join([endpoint1, endpoint2])}
    obj_serv = swift_scality_backend.server.app_factory(conf)

    assert frozenset([
        urlparse.urlparse(endpoint1),
        urlparse.urlparse(endpoint2),
    ]) == obj_serv._get_client_for_policy(0).read_clients[0]._endpoints


def test_setup_with_custom_timeout():
    conf = {'sproxyd_host': 'host1:81', 'sproxyd_proxy_timeout': "10.0",
            'sproxyd_conn_timeout': "4.1"}
    obj_serv = swift_scality_backend.server.app_factory(conf)

    assert obj_serv._get_client_for_policy(0).read_clients[0].read_timeout == 10.0
    assert obj_serv._get_client_for_policy(0).read_clients[0].conn_timeout == 4.1


def test_get_diskfile():
    conf = {'sproxyd_host': 'host1:81'}
    scality_server = swift_scality_backend.server.app_factory(conf)
    diskfile = scality_server.get_diskfile('dev', 'partition', 'a', 'c', 'o')

    assert isinstance(diskfile, swift_scality_backend.diskfile.DiskFile)
    assert diskfile._account == 'a'
    assert diskfile._container == 'c'
    assert diskfile._obj == 'o'


_mock_policy_config_file = mock.mock_open()

_real_open = open


def _open(name, arg):
    if name == os.devnull:
        return _real_open(name, arg)
    else:
        raise IOError(errno.ENOENT, 'Mock')


_mock_policy_config_file.side_effect = _open

del _open


class FakeFile(StringIO.StringIO):
    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass


@mock.patch('eventlet.spawn', mock.Mock())
@mock.patch('__builtin__.open', _mock_policy_config_file)
class TestStoragePolicySupport(unittest.TestCase):
    @staticmethod
    def _app_factory(**kwargs):
        real_kwargs = {
            'sproxyd_host': '127.0.0.1:81',
        }
        real_kwargs.update(kwargs)

        return swift_scality_backend.server.app_factory(real_kwargs)

    def test_no_policy(self):
        with mock.patch(
                'swift_scality_backend.server.ObjectController._get_client_for_policy') as mock_gc:
            self._app_factory().get_diskfile('dev', 'partition', 'a', 'c', 'o')

            mock_gc.assert_called_with(0)

    def test_policy_0(self):
        with mock.patch(
                'swift_scality_backend.server.ObjectController._get_client_for_policy') as mock_gc:
            self._app_factory().get_diskfile('dev', 'partition', 'a', 'c', 'o', policy_idx=0)

            mock_gc.assert_called_with(0)

    def test_policy_1(self):
        with mock.patch(
                'swift_scality_backend.server.ObjectController._get_client_for_policy') as mock_gc:
            self._app_factory().get_diskfile('dev', 'partition', 'a', 'c', 'o', policy_idx=1)

            mock_gc.assert_called_with(1)

    def test_policy_file_read_error(self):
        mock_open = mock.mock_open()
        mock_open.side_effect = IOError(errno.ENFILE, os.strerror(errno.ENFILE))

        with mock.patch('__builtin__.open', mock_open):
            self.assertRaises(
                IOError,
                self._app_factory)

    def test_policy_request_without_configuration(self):
        self.assertRaises(
            RuntimeError,
            self._app_factory().get_diskfile, 'dev', 'partition', 'a', 'c', 'o', policy_idx=1)

    def test_broken_policy_configuration(self):
        policy = '\n'.join(line.strip() for line in '''
            [storage-policy:1]
            write = no-such-ring
            '''.splitlines())

        mock_open = mock.mock_open()
        mock_open.return_value = FakeFile(policy)

        with mock.patch('__builtin__.open', mock_open):
            utils.assertRaisesRegexp(
                swift_scality_backend.policy_configuration.ConfigurationError,
                'Unknown \'write\' ring \'no-such-ring\' in policy 1',
                self._app_factory)

    def test_simple_policy_configuration(self):
        policy = '\n'.join(line.strip() for line in '''
            [ring:paris]
            location = paris
            sproxyd_endpoints = http://localhost:8080/chord, http://otherhost:8080/chord

            [storage-policy:1]
            read =
            write = paris
            '''.splitlines())

        mock_open = mock.mock_open()
        mock_open.return_value = FakeFile(policy)

        with mock.patch('__builtin__.open', mock_open):
            df = self._app_factory().get_diskfile(
                'dev', 'partition', 'a', 'c', 'o', policy_idx=1)

            self.assertEqual(frozenset([
                urlparse.urlparse('http://localhost:8080/chord'),
                urlparse.urlparse('http://otherhost:8080/chord'),
            ]), df._client_collection.read_clients[0]._alive)
