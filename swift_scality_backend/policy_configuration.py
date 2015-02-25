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

'''
Structures and procedures to handle Swift storage policy configuration files.
'''

import operator
import urlparse


class Endpoint(object):  # pylint: disable=R0903
    '''Representation of an Sproxyd endpoint URL

    This is a restricted version of `urlparse.ParseResult`. The `scheme`,
    `netloc` and `path` attributes of instances of this type correspond to the
    same attributes on `urlparse.ParseResult` objects.

    The constructor expects a URL in its `str` format, or an already-parsed URL
    compatible with `urlparse.ParseResult`::

        >>> Endpoint('http://localhost')
        Endpoint(url='http://localhost')
        >>> Endpoint(urlparse.urlparse('http://localhost'))
        Endpoint(url='http://localhost')

    `Endpoint`s can't have `params`, `query` or `fragment` portions in their
    URL. This results in `ValueError`s at construction time::

        >>> Endpoint('http://localhost/?a=b')
        Traceback (most recent call last):
        ...
        ValueError: Endpoint URL can't have query values

    The string-representation of an `Endpoint` behaves as the URL it
    represents::

        >>> print Endpoint('http://localhost')
        http://localhost

    They have some useful attributes::

        >>> localhost = Endpoint('http://localhost/path')
        >>> localhost.scheme
        'http'
        >>> localhost.netloc
        'localhost'
        >>> localhost.path
        '/path'
        >>> localhost.url # doctest: +ELLIPSIS
        ParseResult(scheme='http', netloc='localhost', path='/path', ...)

    `Endpoint`s can be compared to each other, to URL strings or
    `urlparse.ParseResult`s::

        >>> Endpoint('http://localhost/path') == 'http://localhost/path'
        True
        >>> Endpoint('http://localhost') != Endpoint('http://otherhost')
        True
        >>> Endpoint('http://localhost') == \
                urlparse.urlparse('http://otherhost')
        False
    '''

    __slots__ = '_url',

    def __init__(self, url):
        '''Construct an `Endpoint` from a URL

        :param url: URL of the endpoint
        :type url: `str` or `urlparse..ParseResult`

        :raise ValueError: URL has params
        :raise ValueError: URL has query values
        :raise ValueError: URL has a fragment
        '''

        if isinstance(url, basestring):
            url = urlparse.urlparse(url)

        if url.params:
            raise ValueError('Endpoint URL can\'t have params')

        if url.query:
            raise ValueError('Endpoint URL can\'t have query values')

        if url.fragment:
            raise ValueError('Endpoint URL can\'t have a fragment')

        self._url = url

    url = property(operator.attrgetter('_url'), doc='Parsed URL')

    def __str__(self):
        return urlparse.urlunparse(self.url)

    def __repr__(self):
        return 'Endpoint(url=%r)' % urlparse.urlunparse(self.url)

    def __eq__(self, other):
        if isinstance(other, basestring):
            return self.url == urlparse.urlparse(other)
        elif isinstance(other, urlparse.ParseResult):
            return self.url == other
        elif isinstance(other, Endpoint):
            return self.url == other.url
        else:
            return NotImplemented

    def __ne__(self, other):
        equal = self.__eq__(other)

        if equal is NotImplemented:
            return NotImplemented
        else:
            return not equal

    def __hash__(self):
        return hash(self.url)

    scheme = property(operator.attrgetter('url.scheme'),
                      doc='Endpoint URL scheme')
    netloc = property(operator.attrgetter('url.netloc'),
                      doc='Endpoint URL netloc')
    path = property(operator.attrgetter('url.path'), doc='Endpoint URL path')


class Location(object):  # pylint: disable=R0903
    '''Representation of a Ring location

    Values of this type have only one useful attribute, `name`::

        >>> Location('paris').name
        'paris'

    They can be compared to `str`s or other `Location`s::

        >>> Location('paris') == 'paris'
        True
        >>> Location('paris') == Location('london')
        False
        >>> Location('paris') != 'sfo'
        True
    '''

    __slots__ = '_name',

    def __init__(self, name):
        '''Construct a `Location`

        :param name: Name of the location
        :type name: `str`
        '''

        self._name = name

    name = property(operator.attrgetter('_name'), doc='Location name')

    def __str__(self):
        return self.name

    def __repr__(self):
        return 'Location(name=%r)' % self.name

    def __eq__(self, other):
        if isinstance(other, basestring):
            return self.name == other
        elif isinstance(other, Location):
            return self.name == other.name
        else:
            return NotImplemented

    def __ne__(self, other):
        equal = self.__eq__(other)

        if equal is NotImplemented:
            return NotImplemented
        else:
            return not equal

    def __hash__(self):
        return hash(self.name)


class Ring(object):  # pylint: disable=R0903
    '''Representation of a Ring

    A `Ring` has a name, a location and a set of endpoints, available as
    attributes. The constructor supports `str` values as inputs, but these will
    be turned into `Location` and `Endpoint` objects::

        >>> r = Ring('paris-arc6+3', location='paris', \
                     endpoints=['http://localhost', 'http://otherhost'])
        >>> r # doctest: +ELLIPSIS
        Ring(name='...', location=Location(...), endpoints=frozenset([End...
        >>> r.name, r.location, r.endpoints # doctest: +ELLIPSIS
        ('paris-arc6+3', Location(name='paris'), frozenset([Endpoint(...),...]))

    Furthermore, a `Ring` is iterable (over its endpoints).

        >>> for endpoint in sorted(r):
        ...     print endpoint.netloc
        localhost
        otherhost
        >>> 'http://localhost' in r
        True
        >>> Endpoint('http://someotherhost') in r
        False
    '''

    __slots__ = '_name', '_location', '_endpoints',

    def __init__(self, name, location, endpoints):
        '''Construct a `Ring`

        When `location` is a `basestring`, it will be converted into a
        `Location`.

        When elements of `endpoints` are no `Endpoint`s, they will be converted
        into `Endpoint`s. The `Endpoint`s are stored in a `frozenset`.

        :param name: Name of the Ring
        :type name: `str`
        :param location: Location of the Ring
        :type location: `Location` or `str`
        :param endpoints: Ring endpoints
        :type endpoints: Iterable of `str` or `Endpoint`
        '''

        self._name = name

        self._location = location if not isinstance(location, basestring) \
            else Location(location)

        self._endpoints = frozenset(
            endpoint if isinstance(endpoint, Endpoint) else Endpoint(endpoint)
            for endpoint in endpoints)

    name = property(operator.attrgetter('_name'), doc='Ring name')
    location = property(operator.attrgetter('_location'), doc='Ring location')
    endpoints = property(operator.attrgetter('_endpoints'),
                         doc='Ring endpoints')

    def __iter__(self):
        return iter(self.endpoints)

    def __repr__(self):
        return 'Ring(name=%r, location=%r, endpoints=%r)' % \
            (self.name, self.location, self.endpoints)

    def __eq__(self, other):
        if not isinstance(other, Ring):
            return NotImplemented

        svals = (self.name, self.location, self.endpoints)
        ovals = (other.name, other.location, other.endpoints)

        return svals == ovals

    def __ne__(self, other):
        equal = self.__eq__(other)

        if equal is NotImplemented:
            return NotImplemented
        else:
            return not equal

    def __hash__(self):
        return hash((self.name, self.location, self.endpoints))


class StoragePolicy(object):  # pylint: disable=R0903
    '''Representation of a storage-policy

    A `StoragePolicy` has an index, a set of read-only rings and a set of
    writable rings::

        >>> ring1 = Ring('paris-arc6+3', 'paris', ['http://paris/arc6+3'])
        >>> ring2 = Ring('london-arc6+3', 'london', ['http://london/arc6+3'])
        >>> p = StoragePolicy(1, [ring1], [ring2])
        >>> p # doctest: +ELLIPSIS
        StoragePolicy(index=1, read_set=frozenset([Ring(...)]), write_set=...)
        >>> p.index
        1
        >>> p.read_set # doctest: +ELLIPSIS
        frozenset([Ring(name='paris-arc6+3', location=..., endpoints=...)])
        >>> p.write_set # doctest: +ELLIPSIS
        frozenset([Ring(name='london-arc6+3', location=..., endpoints=...)])
    '''

    __slots__ = '_index', '_read_set', '_write_set',

    def __init__(self, index, read_set, write_set):
        '''Construct a `StoragePolicy`

        :param index: Policy index
        :type index: `int`
        :param read_set: Set of read-only rings
        :type read_set: Iterable of `Ring`s
        :param write_set: Set of writeable rings
        :type write_set: Iterable of `Ring`s
        '''

        self._index = index
        self._read_set = frozenset(read_set)
        self._write_set = frozenset(write_set)

    index = property(operator.attrgetter('_index'), doc='Policy index')
    read_set = property(operator.attrgetter('_read_set'), doc='Policy read set')
    write_set = property(operator.attrgetter('_write_set'),
                         doc='Policy write set')

    def __repr__(self):
        return 'StoragePolicy(index=%r, read_set=%r, write_set=%r)' % \
            (self.index, self.read_set, self.write_set)

    def __eq__(self, other):
        if not isinstance(other, StoragePolicy):
            return NotImplemented

        svals = (self.index, self.read_set, self.write_set)
        ovals = (other.index, other.read_set, other.write_set)

        return svals == ovals

    def __ne__(self, other):
        equal = self.__eq__(other)

        if equal is NotImplemented:
            return NotImplemented
        else:
            return not equal

    def __hash__(self):
        return hash((self.index, self.read_set, self.write_set))


class Configuration(object):
    '''Representation of a storage-policy configuration

    This is a set of `StoragePolicy` objects with utilities to retrieve one
    based on index, read them from a configuration file or write them as a new
    configuration.

        >>> conf = Configuration(
        ...     [StoragePolicy(1, [], []), StoragePolicy(2, [], [])])
        >>> conf # doctest: +ELLIPSIS
        Configuration(policies=[StoragePolicy(index=1, ...])

    A `Configuration` is iterable over its policies::

        >>> print list(sorted(policy.index for policy in conf))
        [1, 2]

    In most application, a policy will be looked up based on its index::

        >>> conf.get_policy(index=2)
        StoragePolicy(index=2, read_set=frozenset([]), write_set=frozenset([]))
        >>> conf.get_policy(index=3)
        Traceback (most recent call last):
        ...
        ValueError: Unknown policy index: 3
    '''

    __slots__ = '_policies_map',

    def __init__(self, policies):
        '''Construct a `Configuration` from a set of `StoragePolicy`s

        :param policies: Policies contained in the configuration
        :type policies: Iterable of `StoragePolicy`

        :raise ValueError: Duplicate policy index found
        '''

        self._policies_map = {}

        for policy in policies:
            if policy.index in self._policies_map:
                raise ValueError('Duplicate policy index: %r' % policy.index)

            self._policies_map[policy.index] = policy

    def __repr__(self):
        return 'Configuration(policies=%r)' % list(self)

    def __eq__(self, other):
        if not isinstance(other, Configuration):
            return NotImplemented

        return frozenset(self) == frozenset(other)

    def __ne__(self, other):
        equal = self.__eq__(other)

        if equal is NotImplemented:
            return NotImplemented
        else:
            return not equal

    def __iter__(self):
        return self._policies_map.itervalues()

    def __hash__(self):
        return hash(frozenset(self))

    def get_policy(self, index):
        '''Retrieve a policy based on its index

        :param index: Policy to look up
        :type index: Type of `StoragePolicy.index`

        :raise ValueError: Unknown policy index
        '''

        if index in self._policies_map:
            return self._policies_map[index]
        else:
            raise ValueError('Unknown policy index: %r' % index)
