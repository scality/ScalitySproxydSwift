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

import ConfigParser
import operator
import urlparse

from swift_scality_backend import utils


class Endpoint(object):  # pylint: disable=R0903
    '''Representation of an Sproxyd endpoint URL

    This is a restricted version of :class:`urlparse.ParseResult`. The
    :attr:`scheme`, :attr:`netloc` and :attr:`path` attributes of instances of
    this type correspond to the same attributes on
    :class:`urlparse.ParseResult` objects.

    The constructor expects a URL in its `str` format, or an already-parsed URL
    compatible with :class:`urlparse.ParseResult`::

        >>> Endpoint('http://localhost')
        Endpoint(url='http://localhost')
        >>> Endpoint(urlparse.urlparse('http://localhost'))
        Endpoint(url='http://localhost')

    An :class:`Endpoint` can't have `params`, `query` or `fragment` portions in
    its URL. This results in a :exc:`ValueError` at construction time::

        >>> Endpoint('http://localhost/?a=b')
        Traceback (most recent call last):
        ...
        ValueError: Endpoint URL can't have query values

    The string-representation of an :class:`Endpoint` behaves as the URL it
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

    An :class:`Endpoint` can be compared to others, to URL strings or to
    :class:`urlparse.ParseResult` instances::

        >>> Endpoint('http://localhost/path') == 'http://localhost/path'
        True
        >>> Endpoint('http://localhost') != Endpoint('http://otherhost')
        True
        >>> Endpoint('http://localhost') == \
                urlparse.urlparse('http://otherhost')
        False

    :param url: URL of the endpoint
    :type url: `str` or :class:`urlparse.ParseResult`

    :raise ValueError: URL has params
    :raise ValueError: URL has query values
    :raise ValueError: URL has a fragment
    '''

    __slots__ = '_url',

    def __init__(self, url):
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

    Values of this type have only one useful attribute, :attr:`name`::

        >>> Location('paris').name
        'paris'

    They can be compared to `str` or another :class:`Location`::

        >>> Location('paris') == 'paris'
        True
        >>> Location('paris') == Location('london')
        False
        >>> Location('paris') != 'sfo'
        True

    :param name: Name of the location
    :type name: `str`
    '''

    __slots__ = '_name',

    def __init__(self, name):
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

    A :class:`Ring` has a name, a location and a set of endpoints, available as
    attributes. The constructor supports `str` values as inputs, but these will
    be turned into :class:`Location` and :class:`Endpoint` objects::

        >>> r = Ring('paris-arc6+3', location='paris', \
                     endpoints=['http://localhost', 'http://otherhost'])
        >>> r # doctest: +ELLIPSIS
        Ring(name='...', location=Location(...), endpoints=frozenset([End...
        >>> r.name, r.location, r.endpoints # doctest: +ELLIPSIS
        ('paris-arc6+3', Location(name='paris'), frozenset([Endpoint(...),...]))

    Furthermore, a :class:`Ring` is iterable (over its endpoints).

        >>> for endpoint in sorted(e.netloc for e in r):
        ...     print endpoint
        localhost
        otherhost
        >>> 'http://localhost' in r
        True
        >>> Endpoint('http://someotherhost') in r
        False

    When `location` is a `basestring`, it will be converted into a
    :class:`Location`.

    When elements of `endpoints` are no :class:`Endpoint` instances, they are
    converted into :class:`Endpoint`.

    :param name: Name of the Ring
    :type name: `str`
    :param location: Location of the Ring
    :type location: :class:`Location` or `str`
    :param endpoints: Ring endpoints
    :type endpoints: Iterable of `str` or :class:`Endpoint`
    '''

    __slots__ = '_name', '_location', '_endpoints',

    def __init__(self, name, location, endpoints):
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

    A :class:`StoragePolicy` has an index, a set of read-only rings and a set
    of writable rings::

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

    :param index: Policy index
    :type index: `int`
    :param read_set: Set of read-only rings
    :type read_set: Iterable of :class:`Ring`
    :param write_set: Set of writeable rings
    :type write_set: Iterable of :class:`Ring`
    '''

    READ = 'read'
    WRITE = 'write'

    __slots__ = '_index', '_read_set', '_write_set',

    def __init__(self, index, read_set, write_set):
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

    def lookup(self, method, location_hints=None):
        '''Lookup endpoints for a given method

        This method calculates an (ordered) list of sets of :class:`Endpoint`
        that can be used to perform a certain type of action, based on an
        optional list of location preferences.

        A simple example::

            >>> sp = StoragePolicy(
            ...     index=1,
            ...     read_set=[
            ...         Ring(
            ...             name='sfo-arc6+3',
            ...             location='sfo',
            ...             endpoints=[
            ...                 'http://sfo1.int/arc6+3']),
            ...         Ring(
            ...             name='nyc-arc6+3',
            ...             location='nyc',
            ...             endpoints=[
            ...                 'http://nyc1.int/arc6+3',
            ...                 'http://nyc2.int/arc6+3'])],
            ...     write_set=[
            ...         Ring(
            ...             name='paris-arc6+3',
            ...             location='paris',
            ...             endpoints=[
            ...                 'http://paris1.int/arc6+3',
            ...                 'http://paris2.int/arc6+3'])])

            >>> def dump(result):
            ...     for (idx, endpoints) in enumerate(result):
            ...         print idx, sorted(str(ep) for ep in endpoints)

            >>> dump(sp.lookup(StoragePolicy.READ))
            0 ['http://nyc1.int/arc6+3', 'http://nyc2.int/arc6+3', 'http://paris1.int/arc6+3', 'http://paris2.int/arc6+3', 'http://sfo1.int/arc6+3']

            >>> dump(sp.lookup(StoragePolicy.WRITE))
            0 ['http://paris1.int/arc6+3', 'http://paris2.int/arc6+3']

            >>> dump(sp.lookup(StoragePolicy.READ, ['sfo']))
            0 ['http://sfo1.int/arc6+3']
            1 ['http://nyc1.int/arc6+3', 'http://nyc2.int/arc6+3', 'http://paris1.int/arc6+3', 'http://paris2.int/arc6+3']

            >>> dump(sp.lookup(StoragePolicy.READ, ['sfo', 'paris']))
            0 ['http://sfo1.int/arc6+3']
            1 ['http://paris1.int/arc6+3', 'http://paris2.int/arc6+3']
            2 ['http://nyc1.int/arc6+3', 'http://nyc2.int/arc6+3']

        :param method: Target action (one of :const:`StoragePolicy.READ` or
                        :const:`StoragePolicy.WRITE`)
        :type method: `str`
        :param location_hints: List of preferred locations, in descending order
        :type location_hints: Iterable of `str` or :class:`Location`

        :return: List of target :class:`Endpoint`
        :rtype: Iterable of `frozenset` of :class:`Endpoint`

        :raise ValueError: Invalid `method` passed
        '''

        # Step 1: Gather all candidate target rings
        if method == self.READ:
            candidates0 = self.read_set.union(self.write_set)
        elif method == self.WRITE:
            candidates0 = self.write_set
        else:
            raise ValueError('Invalid method: %r' % method)

        # Step 2: Sort all candidates based on the location hints
        def worker((temp_result, candidates), hint):
            # Step 2a: Find all rings we didn't process yet at current hint
            here = frozenset(
                ring for ring in candidates if ring.location == hint)

            # Step 2b: Update the result, and remove from the unused rings
            return (temp_result + [here], candidates.difference(here))

        location_hints = location_hints or []
        initial_state = ([], candidates0)
        result, leftover_candidates = reduce(worker, location_hints, initial_state)
        result.append(leftover_candidates)

        # Step 3: Turn every set of rings into a set of endpoints, skipping
        # empty sets
        return [frozenset(
            endpoint
                for ring in rings
                for endpoint in ring)
                for rings in result if rings]


RING_SECTION_PREFIX = 'ring:'
STORAGE_POLICY_SECTION_PREFIX = 'storage-policy:'

RING_LOCATION_OPTION = 'location'
RING_SPROXYD_ENDPOINTS_OPTION = 'sproxyd_endpoints'

STORAGE_POLICY_READ_OPTION = 'read'
STORAGE_POLICY_WRITE_OPTION = 'write'


class ConfigurationError(Exception):
    '''Exception thrown when a configuration is somehow invalid.'''


class Configuration(object):
    '''Representation of a storage-policy configuration

    This is a set of :class:`StoragePolicy` objects with utilities to retrieve
    one based on index, read them from a configuration file or write them as a
    new configuration.

        >>> conf = Configuration(
        ...     [StoragePolicy(1, [], []), StoragePolicy(2, [], [])])
        >>> conf # doctest: +ELLIPSIS
        Configuration(policies=[StoragePolicy(index=1, ...])

    A :class:`Configuration` is iterable over its policies::

        >>> print list(sorted(policy.index for policy in conf))
        [1, 2]

    In most application, a policy will be looked up based on its index::

        >>> conf.get_policy(index=2)
        StoragePolicy(index=2, read_set=frozenset([]), write_set=frozenset([]))
        >>> conf.get_policy(index=3)
        Traceback (most recent call last):
        ...
        ValueError: Unknown policy index: 3

    :param policies: Policies contained in the configuration
    :type policies: Iterable of :class:`StoragePolicy`

    :raise ValueError: Duplicate policy index found
    '''

    __slots__ = '_policies_map',

    def __init__(self, policies):
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
        :type index: Type of :attr:`StoragePolicy.index`

        :raise ValueError: Unknown policy index
        '''

        if index in self._policies_map:
            return self._policies_map[index]
        else:
            raise ValueError('Unknown policy index: %r' % index)

    @classmethod
    def from_stream(cls, stream, filename=None):
        '''Parse a configuration from a stream

        This functions turns a configuration file into a
        :class:`Configuration`, with some sanity checks along the way. It is
        based on :class:`ConfigParser.SafeConfigParser`.

        The `stream` object must have a :meth:`readline` method. `filename`
        will be used in error reporting, if available.

        :param stream: Stream to parse
        :type stream: File-like object
        :param filename: Filename of input
        :type filename: `str`

        :returns: Parsed :class:`Configuration`
        :rtype: :class:`Configuration`

        :raise ConfigurationError: Various configurations issues detected
        '''

        parser = ConfigParser.SafeConfigParser()
        parser.readfp(stream, filename)

        ring_sections = [
            section for section in parser.sections()
            if section.startswith(RING_SECTION_PREFIX)]
        sp_sections = [
            section for section in parser.sections()
            if section.startswith(STORAGE_POLICY_SECTION_PREFIX)]

        _default = object()

        def get(section, setting, default=_default):
            '''Safely retrieve a value from a configuration.'''

            if not parser.has_option(section, setting):
                if default is not _default:
                    return default
                else:
                    raise ConfigurationError(
                        'Section %r lacks a %r setting' % (section, setting))

            return parser.get(section, setting)

        rings = {}
        for section in ring_sections:
            name = section[len(RING_SECTION_PREFIX):]
            if not name:
                raise ConfigurationError('Invalid section name %r' % section)

            location = get(section, RING_LOCATION_OPTION)
            if not location:
                raise ConfigurationError(
                    'Invalid %r setting in %r' %
                    (RING_LOCATION_OPTION, section))

            endpoints = get(section, RING_SPROXYD_ENDPOINTS_OPTION)

            endpoints2 = set()
            for endpoint in utils.split_list(endpoints):
                try:
                    endpoints2.add(Endpoint(endpoint))
                except ValueError as exc:
                    raise ConfigurationError(
                        'Error parsing endpoint %r in %r: %s' %
                        (endpoint, section, exc.message))

            if not endpoints2:
                raise ConfigurationError(
                    'Invalid %r setting in %r' %
                    (RING_SPROXYD_ENDPOINTS_OPTION, section))

            rings[name] = Ring(name, location, endpoints2)

        policies = list()
        for section in sp_sections:
            index = section[len(STORAGE_POLICY_SECTION_PREFIX):]
            if not index:
                raise ConfigurationError('Invalid section name %r' % section)

            try:
                index2 = int(index)
            except (ValueError, TypeError):
                raise ConfigurationError('Invalid policy index: %r' % index)

            read = get(section, STORAGE_POLICY_READ_OPTION, '')
            write = get(section, STORAGE_POLICY_WRITE_OPTION)

            read2 = set()
            for ring in utils.split_list(read):
                if ring not in rings:
                    raise ConfigurationError(
                        'Unknown %r ring %r in policy %r' %
                        (STORAGE_POLICY_READ_OPTION, ring, index2))

                read2.add(rings[ring])

            write2 = list(utils.split_list(write))

            if len(write2) > 1:
                raise ConfigurationError('Multiple %r rings defined in %r' %
                                         (STORAGE_POLICY_WRITE_OPTION, section))

            write3 = set()
            for ring in write2:
                if ring not in rings:
                    raise ConfigurationError(
                        'Unknown %r ring %r in policy %r' %
                        (STORAGE_POLICY_WRITE_OPTION, ring, index2))

                write3.add(rings[ring])

            policies.append(StoragePolicy(index2, read2, write3))

        return cls(policies)

    def to_stream(self, stream):
        '''Unparse a :class:`Configuration` into a stream

        :param stream: Stream to write the :class:`Configuration` to,
                        in INI-format
        :type stream: File-like object
        '''

        rings = set()

        for policy in self:
            for ring in policy.read_set:
                rings.add(ring)

            for ring in policy.write_set:
                rings.add(ring)

        parser = ConfigParser.SafeConfigParser()

        for ring in rings:
            section_name = RING_SECTION_PREFIX + ring.name
            parser.add_section(section_name)

            parser.set(section_name, RING_LOCATION_OPTION, ring.location.name)
            parser.set(
                section_name, RING_SPROXYD_ENDPOINTS_OPTION,
                ', '.join(str(endpoint) for endpoint in ring.endpoints))

        for policy in self:
            section_name = '%s%r' % \
                (STORAGE_POLICY_SECTION_PREFIX, policy.index)
            parser.add_section(section_name)

            parser.set(
                section_name, STORAGE_POLICY_READ_OPTION,
                ', '.join(ring.name for ring in policy.read_set))
            parser.set(
                section_name, STORAGE_POLICY_WRITE_OPTION,
                ', '.join(ring.name for ring in policy.write_set))

        parser.write(stream)
