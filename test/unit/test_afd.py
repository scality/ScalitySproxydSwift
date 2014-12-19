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

'''Tests for `swift_scality_backend.afd`'''

import time
import unittest

from swift_scality_backend.afd import AccrualFailureDetector


class TestAFDBehaviorOnInit(unittest.TestCase):

    def test_afd_with_no_heartbeat(self):
        afd = AccrualFailureDetector()
        self.assertTrue(afd.isDead())

    def test_afd_with_one_heartbeat(self):
        afd = AccrualFailureDetector()
        afd.heartbeat()
        self.assertTrue(afd.isDead())

    def test_afd_with_two_heartbeats(self):
        afd = AccrualFailureDetector()
        afd.heartbeat()
        time.sleep(0.01)
        afd.heartbeat()
        self.assertTrue(afd.isDead())

    def test_afd_with_three_heartbeats(self):
        afd = AccrualFailureDetector()
        afd.heartbeat()
        time.sleep(0.01)
        afd.heartbeat()
        time.sleep(0.01)
        afd.heartbeat()
        self.assertTrue(afd.isAlive())

    def test_max_sample_size(self):
        afd = AccrualFailureDetector()
        afd.max_sample_size = 3

        for i in range(5):
            afd.heartbeat()
            time.sleep(0.01)

        self.assertEqual(3, len(afd._intervals))
