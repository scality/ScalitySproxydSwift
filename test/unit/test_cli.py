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

'''Tests for `swift_scality_backend.cli`.'''

import sys
import unittest
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

from swift_scality_backend import cli
from swift_scality_backend.policy_configuration import StoragePolicy

import utils


class FakeStream(object):
    def __init__(self, module, attr):
        self.stream = StringIO()
        self._module = module
        self._attr = attr
        self._orig_attr = None

    def __enter__(self):
        self._orig_attr = getattr(self._module, self._attr)
        setattr(self._module, self._attr, self.stream)

        return self

    def __exit__(self, exc, value, tb):
        setattr(self._module, self._attr, self._orig_attr)


class Namespace(object):
    def __init__(self, **kwargs):
        for (name, value) in kwargs.iteritems():
            setattr(self, name, value)


class TestStoragePolicyLint(unittest.TestCase):
    def test_lint_fails_on_malformed_file(self):
        config = 'test'

        args = Namespace(
            config=StringIO(config))

        with FakeStream(sys, 'stderr') as stderr:
            rc = cli.storage_policy_lint(args)

            utils.assertRegexpMatches(
                stderr.stream.getvalue(),
                'Parsing error:')

            self.assertNotEqual(0, rc)

    def test_lint_fails_on_invalid_config(self):
        config = '[ring:]'

        args = Namespace(
            config=StringIO(config))

        with FakeStream(sys, 'stderr') as stderr:
            rc = cli.storage_policy_lint(args)

            utils.assertRegexpMatches(
                stderr.stream.getvalue(),
                'Configuration error:')

            self.assertNotEqual(0, rc)

    def test_lint_fails_on_exception(self):
        class Stream(object):
            def readline(self):
                raise IOError('Oops')

        args = Namespace(
            config=Stream())

        with FakeStream(sys, 'stderr') as stderr:
            rc = cli.storage_policy_lint(args)

            utils.assertRegexpMatches(
                stderr.stream.getvalue(),
                'Error: Oops')

            self.assertNotEqual(0, rc)

    def test_lint_succeeds_on_valid_config(self):
        config = ''

        args = Namespace(
            config=StringIO(config))

        rc = cli.storage_policy_lint(args)

        self.assertEqual(0, rc)


class TestStoragePolicyQuery(unittest.TestCase):
    def test_load_fails(self):
        config = '[ring:]'

        args = Namespace(
            config=StringIO(config))

        with FakeStream(sys, 'stderr') as stderr:
            rc = cli.storage_policy_query(args)

            utils.assertRegexpMatches(
                stderr.stream.getvalue(),
                'Error: Invalid section name')

            self.assertNotEqual(0, rc)

    def test_lookup_fails(self):
        config = ''

        args = Namespace(
            config=StringIO(config),
            policy_index=1)

        with FakeStream(sys, 'stderr') as stderr:
            rc = cli.storage_policy_query(args)

            utils.assertRegexpMatches(
                stderr.stream.getvalue(),
                'Error: Unknown policy index')

            self.assertNotEqual(0, rc)

    def test_success(self):
        config = '\n'.join(s.strip() for s in '''
            [ring:paris]
            location = paris
            sproxyd_endpoints = http://paris1.int/, http://paris2.int

            [ring:sfo]
            location = sfo
            sproxyd_endpoints = http://sfo1.int

            [storage-policy:2]
            read = sfo
            write = paris
            '''.splitlines())

        args = Namespace(
            config=StringIO(config),
            policy_index=2,
            action=StoragePolicy.WRITE,
            locations=['paris'])

        with FakeStream(sys, 'stdout') as stdout:
            rc = cli.storage_policy_query(args)

            self.assertEqual(0, rc)

            out = stdout.stream.getvalue()

            self.assertTrue('http://paris1.int' in out)
            self.assertTrue('http://paris2.int' in out)

            self.assertFalse('sfo' in out)


class TestMain(unittest.TestCase):
    def test_main(self):
        def exit(code):
            raise SystemExit(code)

        orig_exit = sys.exit
        sys.exit = exit

        # Force failure even when `argparse` is installed on Python 2.6 setups
        orig_argparse = cli.argparse
        if sys.version_info < (2, 7):
            cli.argparse = None

        try:
            with FakeStream(sys, 'stdout') as stdout:
                with FakeStream(sys, 'stderr') as stderr:
                    self.assertRaises(
                        SystemExit,
                        cli.main, ['--help'])

                    if cli.argparse:
                        utils.assertRegexpMatches(
                            stdout.stream.getvalue(),
                            'storage-policy-lint')
                    else:
                        self.assertTrue(sys.version_info < (2, 7))
                        utils.assertRegexpMatches(
                            stderr.stream.getvalue(),
                            'Python 2.7')
        finally:
            sys.exit = orig_exit
            cli.argparse = orig_argparse
