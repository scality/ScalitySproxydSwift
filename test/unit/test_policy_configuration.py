# Copyright (c) 2015 Scality
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

'''Tests for `swift_scality_backend.policy_configuration`.'''

import unittest
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

import utils

from swift_scality_backend.policy_configuration import Configuration
from swift_scality_backend.policy_configuration import ConfigurationError
from swift_scality_backend.policy_configuration import Endpoint
from swift_scality_backend.policy_configuration import Location
from swift_scality_backend.policy_configuration import Ring
from swift_scality_backend.policy_configuration import StoragePolicy


class TestEndpoint(unittest.TestCase):
    def test_hash_equal(self):
        url = 'http://localhost:81/path'

        self.assertEqual(
            hash(Endpoint(url)),
            hash(Endpoint(url)))

    def test_hash_not_equal(self):
        # If this fails, the subsequent test would fail as well, unexpectedly
        self.assertNotEqual(
            hash('http://localhost/path'),
            hash('http://otherhost/path'))

        self.assertNotEqual(
            hash(Endpoint('http://localhost/path')),
            hash(Endpoint('http://otherhost/path')))

    def test_eq_not_implemented(self):
        # Note: this can't be `assertNotEqual`, we want to test `__eq__`
        self.assertFalse(Endpoint('http://localhost') == 1)

    def test_ne_not_implemented(self):
        self.assertNotEqual(Endpoint('http://localhost'), 1)

    def test_reject_params(self):
        utils.assertRaisesRegexp(
            ValueError,
            r'Endpoint URL can\'t have params',
            Endpoint, 'http://localhost/;param')

    def test_reject_query(self):
        utils.assertRaisesRegexp(
            ValueError,
            r'Endpoint URL can\'t have query values',
            Endpoint, 'http://localhost/?query')

    def test_reject_fragment(self):
        utils.assertRaisesRegexp(
            ValueError,
            r'Endpoint URL can\'t have a fragment',
            Endpoint, 'http://localhost/#fragment')


class TestLocation(unittest.TestCase):
    def test_eq_not_implemented(self):
        self.assertFalse(Location('paris') == 1)

    def test_ne_not_implemented(self):
        self.assertNotEqual(Location('paris'), 1)

    def test_str(self):
        self.assertEqual('paris', str(Location('paris')))

    def test_repr(self):
        self.assertEqual('Location(name=\'paris\')', repr(Location('paris')))

    def test_hash_equal(self):
        self.assertEqual(hash(Location('paris')), hash(Location('paris')))

    def test_hash_not_equal(self):
        # Require this first... If this fails, our test would fail as well which
        # is not the intention
        self.assertNotEqual(hash('paris'), hash('london'))
        self.assertNotEqual(hash(Location('paris')), hash(Location('london')))


class TestRing(unittest.TestCase):
    def test_eq(self):
        name = 'paris-arc6+3'
        location = 'paris'
        url1 = 'http://localhost'
        url2 = 'http://otherhost'

        r1 = Ring(name, location, [url1, url2])
        r2 = Ring(name, location, [url2, url1])

        self.assertEqual(r1, r2)

    def test_eq_not_implemented(self):
        self.assertFalse(Ring('paris-arc6+3', 'paris', []) == 1)

    def test_ne(self):
        self.assertNotEqual(
            Ring('paris-arc6+3', 'paris', []),
            Ring('london-arc6+3', 'london', []))

    def test_ne_not_implemented(self):
        self.assertNotEqual(Ring('paris-arc6+3', 'paris', []), 1)

    def test_hash_equal(self):
        name = 'paris-arc6+3'
        location = 'paris'
        url1 = 'http://localhost'
        url2 = 'http://otherhost'

        r1 = Ring(name, location, [url1, url2])
        r2 = Ring(name, location, [url2, url1])

        self.assertEqual(hash(r1), hash(r2))

    def test_hash_not_equal(self):
        self.assertNotEqual(hash('paris-arc6+3'), hash('paris-chord3'))

        self.assertNotEqual(
            hash(Ring('paris-arc6+3', 'paris', [])),
            hash(Ring('paris-chord3', 'paris', [])))


class TestStoragePolicy(unittest.TestCase):
    def test_eq(self):
        index = 1
        read_set = [Ring('paris-arc6+3', 'paris', ['http://paris'])]
        write_set = [Ring('london-arc6+3', 'london', ['http://london'])]

        sp1 = StoragePolicy(index, read_set, write_set)
        sp2 = StoragePolicy(index, read_set, write_set)

        self.assertEqual(sp1, sp2)

    def test_eq_not_implemented(self):
        self.assertFalse(StoragePolicy(1, [], []) == 1)

    def test_ne(self):
        self.assertNotEqual(
            StoragePolicy(1, [], []),
            StoragePolicy(2, [], []))

        self.assertNotEqual(
            StoragePolicy(1, [Ring('paris-arc6+3', 'paris', [])], []),
            StoragePolicy(1, [Ring('london-arc6+3', 'london', [])], []))

        self.assertNotEqual(
            StoragePolicy(1, [], [Ring('paris-arc6+3', 'paris', [])]),
            StoragePolicy(1, [], []))

    def test_ne_not_implemented(self):
        self.assertNotEqual(StoragePolicy(1, [], []), 1)

    def test_hash_equal(self):
        self.assertEqual(
            hash(
                StoragePolicy(
                    1,
                    [
                        Ring('paris-arc6+3', 'paris',
                             ['http://paris1', 'http://paris2']),
                        Ring('london-arc6+3', 'london',
                             ['http://london1', 'http://london2'])],
                    [
                        Ring('paris-chord3', 'paris', ['http://paris3'])])),
            hash(
                StoragePolicy(
                    1,
                    [
                        Ring('london-arc6+3', 'london',
                             ['http://london2', 'http://london1']),
                        Ring('paris-arc6+3', 'paris',
                             ['http://paris1', 'http://paris2'])],
                    [
                        Ring('paris-chord3', 'paris', ['http://paris3'])])))

    def test_hash_not_equal(self):
        self.assertNotEqual(
            hash(StoragePolicy(1, [], [])),
            hash(StoragePolicy(2, [], [])))


class TestConfiguration(unittest.TestCase):
    TEST_CONFIGURATION = '\n'.join(line.lstrip() for line in '''
        [ring:paris-rep3]
        location = paris
        sproxyd_endpoints = http://paris1.int/rep3, http://paris2.int/rep3

        [ring:paris-arc6+3]
        location = paris
        sproxyd_endpoints = http://paris1.int/arc6+3, http://paris2.int/arc6+3

        [ring:sfo-arc6+3]
        location = sfo
        sproxyd_endpoints = http://sfo1.int/arc6+3

        [ring:nyc-arc6+3]
        location = nyc
        sproxyd_endpoints = http://nyc1.int/arc6+3

        [storage-policy:1]
        read = sfo-arc6+3
        write = paris-arc6+3

        [storage-policy:2]
        read = nyc-arc6+3
        write = paris-arc6+3

        [storage-policy:3]
        read =
        write = paris-rep3
        '''.splitlines())

    def test_duplicate_index(self):
        p1 = StoragePolicy(1, [], [])
        p2 = StoragePolicy(2, [], [])
        p3 = StoragePolicy(1, [], [])

        self.assertRaises(ValueError, Configuration, [p1, p2, p3])

    def test_eq(self):
        self.assertEqual(
            Configuration([StoragePolicy(1, [], [])]),
            Configuration([StoragePolicy(1, [], [])]))

    def test_eq_not_implemented(self):
        self.assertFalse(
            Configuration([]) == 1)

    def test_ne(self):
        self.assertNotEqual(
            Configuration([StoragePolicy(1, [], [])]),
            Configuration([]))

    def test_ne_not_implemented(self):
        self.assertNotEqual(Configuration([]), 1)

    def test_hash_equal(self):
        p = StoragePolicy(1, [], [])

        self.assertEqual(
            hash(Configuration([p])),
            hash(Configuration([p])))

    def test_from_stream(self):
        conf = Configuration.from_stream(StringIO(self.TEST_CONFIGURATION))

        p1 = conf.get_policy(1)
        self.assertEqual(p1.index, 1)
        self.assertEqual(
            list(p1.read_set),
            [Ring('sfo-arc6+3', 'sfo', ['http://sfo1.int/arc6+3'])])
        self.assertEqual(
            list(p1.write_set),
            [Ring(
                'paris-arc6+3', 'paris',
                ['http://paris1.int/arc6+3', 'http://paris2.int/arc6+3'])])

        p2 = conf.get_policy(2)
        self.assertEqual(
            p2,
            StoragePolicy(
                2,
                [Ring('nyc-arc6+3', 'nyc', ['http://nyc1.int/arc6+3'])],
                [Ring(
                    'paris-arc6+3', 'paris',
                    ['http://paris1.int/arc6+3', 'http://paris2.int/arc6+3'])]))

        p3 = conf.get_policy(3)
        self.assertEqual(
            p3,
            StoragePolicy(
                3,
                [],
                [Ring(
                    'paris-rep3', 'paris',
                    ['http://paris1.int/rep3', 'http://paris2.int/rep3'])]))

        self.assertRaises(ValueError, conf.get_policy, 4)
        self.assertRaises(ValueError, conf.get_policy, 'test')

    def test_to_stream(self):
        conf = Configuration.from_stream(StringIO(self.TEST_CONFIGURATION))

        out = StringIO()
        conf.to_stream(out)

        conf2 = Configuration.from_stream(StringIO(out.getvalue()))

        self.assertEqual(conf, conf2)

    @staticmethod
    def _format_config(conf):
        return StringIO(
            '\n'.join(
                line.lstrip() for line in conf.splitlines()))

    def test_missing_ring_name(self):
        conf = self._format_config('''
            [ring:]
            location = test
            sproxyd_endpoints = http://localhost
            ''')

        self.assertRaisesRegexp(
            ConfigurationError,
            'Invalid section name \'ring:\'',
            Configuration.from_stream, conf)

    def test_missing_ring_location(self):
        conf = self._format_config('''
            [ring:test]
            sproxyd_endpoints = http://localhost
            ''')

        utils.assertRaisesRegexp(
            ConfigurationError,
            '''Section 'ring:test' lacks a 'location' setting''',
            Configuration.from_stream, conf)

    def test_empty_ring_location(self):
        conf = self._format_config('''
            [ring:test]
            location =
            sproxyd_endpoints = http://localhost
            ''')

        utils.assertRaisesRegexp(
            ConfigurationError,
            'Invalid \'location\' setting in \'ring:test\'',
            Configuration.from_stream, conf)

    def test_empty_ring_sproxyd_endpoints(self):
        conf = self._format_config('''
            [ring:test]
            location = test
            sproxyd_endpoints =
            ''')

        utils.assertRaisesRegexp(
            ConfigurationError,
            'Invalid \'sproxyd_endpoints\' setting in \'ring:test\'',
            Configuration.from_stream, conf)

    def test_missing_ring_sproxyd_endpoints(self):
        conf = self._format_config('''
            [ring:test]
            location = paris
            ''')

        utils.assertRaisesRegexp(
            ConfigurationError,
            '''Section 'ring:test' lacks a 'sproxyd_endpoints' setting''',
            Configuration.from_stream, conf)

    def test_invalid_ring_sproxyd_endpoint(self):
        conf = self._format_config('''
            [ring:test]
            location = paris
            sproxyd_endpoints = http://localhost, http://otherhost/?a=b
            ''')

        utils.assertRaisesRegexp(
            ConfigurationError,
            'Error parsing endpoint \'http://otherhost/\?a=b\' in '
            '\'ring:test\': Endpoint URL can\'t have query values',
            Configuration.from_stream, conf)

    def test_non_int_storage_policy_index(self):
        conf = self._format_config('''
            [storage-policy:test]
            read =
            write = test
            ''')

        utils.assertRaisesRegexp(
            ConfigurationError,
            'Invalid policy index: \'test\'',
            Configuration.from_stream, conf)

    def test_empty_storage_policy_index(self):
        conf = self._format_config('''
            [storage-policy:]
            read =
            write = test
            ''')

        utils.assertRaisesRegexp(
            ConfigurationError,
            'Invalid section name \'storage-policy:\'',
            Configuration.from_stream, conf)

    def test_unknown_write_ring_in_storage_policy(self):
        conf = self._format_config('''
            [ring:test1]
            location = paris
            sproxyd_endpoints = http://localhost

            [storage-policy:1]
            read =
            write = test2
            ''')

        utils.assertRaisesRegexp(
            ConfigurationError,
            'Unknown \'write\' ring \'test2\' in policy 1',
            Configuration.from_stream, conf)

    def test_unknown_read_ring_in_storage_policy(self):
        conf = self._format_config('''
            [ring:test1]
            location = paris
            sproxyd_endpoints = http://localhost

            [storage-policy:1]
            read = test2
            write = test1
            ''')

        utils.assertRaisesRegexp(
            ConfigurationError,
            'Unknown \'read\' ring \'test2\' in policy 1',
            Configuration.from_stream, conf)

    def test_multiple_write_rings_in_storage_policy(self):
        conf = self._format_config('''
            [ring:test1]
            location = paris
            sproxyd_endpoints = http://paris

            [ring:test2]
            location = london
            sproxyd_endpoints = http://london

            [storage-policy:1]
            read =
            write = test1, test2
            ''')

        utils.assertRaisesRegexp(
            ConfigurationError,
            'Multiple \'write\' rings defined in \'storage-policy:1\'',
            Configuration.from_stream, conf)

    def test_missing_read_in_storage_policy(self):
        conf = self._format_config('''
            [ring:test1]
            location = paris
            sproxyd_endpoints = http://localhost

            [storage-policy:1]
            write = test1
            ''')

        conf = Configuration.from_stream(conf)

        read_set = conf.get_policy(1).read_set
        if len(read_set) != 0:
            raise self.failureException(
                '%r is not empty' % read_set)
