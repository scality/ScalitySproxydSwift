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

import swift_scality_backend.afd


class TestAFDBehaviorOnInit(unittest.TestCase):

    def setUp(self):
        self.afd = swift_scality_backend.afd.AccrualFailureDetector()

    def test_afd_with_no_heartbeat(self):
        self.assertTrue(self.afd.isDead())

    def test_afd_with_one_heartbeat(self):
        self.afd.heartbeat()
        self.assertTrue(self.afd.isDead())

    def test_afd_with_two_heartbeats(self):
        self.afd.heartbeat()
        time.sleep(0.01)
        self.afd.heartbeat()
        self.assertTrue(self.afd.isDead())

    def test_afd_with_three_heartbeats(self):
        self.afd.heartbeat()
        time.sleep(0.01)
        self.afd.heartbeat()
        time.sleep(0.01)
        self.afd.heartbeat()
        self.assertTrue(self.afd.isAlive())
