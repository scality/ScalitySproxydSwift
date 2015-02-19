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

'''Tests for `swift_scality_backend.utils`'''

import unittest

from swift_scality_backend.utils import split_list


class TestSplitList(unittest.TestCase):
    def test_empty_string(self):
        self.assertEqual(
            [],
            list(split_list('')))

    def test_basic(self):
        self.assertEqual(
            ['1', '2', '3'],
            list(split_list('1, 2, 3')))

    def test_space_prefix(self):
        self.assertEqual(
            ['1', '2'],
            list(split_list('   1, 2')))

    def test_space_suffix(self):
        self.assertEqual(
            ['1', '2'],
            list(split_list('1, 2   ')))

    def test_words(self):
        self.assertEqual(
            ['one', 'two', 'three'],
            list(split_list(' one, two, three ')))
