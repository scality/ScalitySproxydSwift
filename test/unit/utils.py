# Copyright (c) 2014, 2015 Scality
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

"""A collection of functions that helps running unit tests"""

import functools
import re
import unittest

import nose.plugins.skip


def skipIf(condition, reason):
    """
    A `skipIf` decorator.

    Similar to `unittest.skipIf`, for Python 2.6 compatibility.
    """
    def decorator(test_item):
        @functools.wraps(test_item)
        def wrapped(*args, **kwargs):
            if condition:
                raise nose.plugins.skip.SkipTest(reason)
            else:
                return test_item(*args, **kwargs)
        return wrapped
    return decorator


def assertRaisesRegexp(expected_exception, expected_regexp,
                       callable_obj, *args, **kwargs):
    """Asserts that the message in a raised exception matches a regexp."""
    try:
        callable_obj(*args, **kwargs)
    except expected_exception as exc_value:
        if not re.search(expected_regexp, str(exc_value)):
            raise unittest.TestCase.failureException(
                '"%s" does not match "%s"' %
                (expected_regexp.pattern, str(exc_value)))
    else:
        if hasattr(expected_exception, '__name__'):
            excName = expected_exception.__name__
        else:
            excName = str(expected_exception)
        raise unittest.TestCase.failureException("%s not raised" % excName)


def assertRegexpMatches(text, expected_regexp, msg=None):
    '''Asserts the text matches the regular expression.'''

    if isinstance(expected_regexp, basestring):
        expected_regexp = re.compile(expected_regexp)

    if not expected_regexp.search(text):
        msg = msg or "Regexp didn't match"
        msg = '%s: %r not found in %r' % (msg, expected_regexp.pattern, text)
        raise unittest.TestCase.failureException(msg)
