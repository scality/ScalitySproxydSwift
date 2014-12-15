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

import time
import decimal
import math


class AccrualFailureDetector(object):
    """ Python implementation of 'The Phi Accrual Failure Detector' by Hayashibara et al.

    (Original version by Brandon Williams (github.com/driftx), modified by Roger Schildmeijer (github.com/rchildmeijer))

    Failure detection is the process of determining which nodes in a distributed fault-tolerant system have failed.
    Original Phi Accrual Failure Detection paper: http://ddg.jaist.ac.jp/pub/HDY+04.pdf

    A low threshold is prone to generate many wrong suspicions but ensures a quick detection in the event of a real crash.
    Conversely, a high threshold generates fewer mistakes but needs more time to detect actual crashes"""

    max_sample_size = 1000
    threshold = 2  # 1 = 10% error rate, 2 = 1%, 3 = 0.1%.., (eg threshold=3. no heartbeat for >6s => node marked as dead)

    def __init__(self):
        self._intervals = []
        self._timestamp = None
        self._mean = None

    def heartbeat(self):
        """ Call when host has indicated being alive (aka heartbeat) """
        if not self._timestamp:
            self._timestamp = time.time()
        else:
            now = time.time()
            interval = now - self._timestamp
            self._timestamp = now
            self._intervals.append(interval)
            if len(self._intervals) > self.max_sample_size:
                self._intervals.pop(0)
            if len(self._intervals) > 1:
                self._mean = sum(self._intervals) / float(len(self._intervals))

    def _probability(self, timestamp):
        # cassandra does this, citing: /* Exponential CDF = 1 -e^-lambda*x */
        # but the paper seems to call for a probability density function
        # which I can't figure out :/
        exponent = -1.0 * timestamp / self._mean
        return 1 - (1.0 - math.pow(math.e, exponent))

    def phi(self):
        # if we don't have enough value to take a decision
        # assume the node is dead
        if self._mean is None:
            return self.threshold + 1
        ts = time.time()
        diff = ts - self._timestamp
        prob = self._probability(diff)
        if (decimal.Decimal(str(prob)).is_zero()):
            prob = 1E-128  # a very small number, avoiding ValueError: math domain error
        return -1 * math.log10(prob)

    def isAlive(self):
        return self.phi() < self.threshold

    def isDead(self):
        return not self.isAlive()
