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

import swift.common.exceptions


class SproxydException(swift.common.exceptions.DiskFileError):
    def __init__(self, msg):
        super(SproxydException, self).__init__(msg)


class SproxydHTTPException(SproxydException):
    def __init__(self, msg, ipaddr='', port=0, path='',
                 http_status=0, http_reason=''):
        super(SproxydHTTPException, self).__init__(msg)
        self.msg = msg
        self.ipaddr = ipaddr
        self.port = port
        self.base_path = path
        self.http_status = http_status
        self.http_reason = http_reason

    def __str__(self):
        suffix = filter(bool, [
            self.ipaddr if self.ipaddr else None,
            ':%d' % int(self.port) if self.port else None,
            self.base_path if self.base_path else None,
            ' %d' % self.http_status if self.http_status else None,
            ' %s' % self.http_reason if self.http_reason else None])

        if not suffix:
            return self.msg
        else:
            return '%s %s' % (self.msg, ''.join(suffix))

    def __repr__(self):
        args = ', '.join('%s=%r' % arg for arg in [
            ('msg', self.msg),
            ('ipaddr', self.ipaddr),
            ('port', self.port),
            ('path', self.base_path),
            ('http_status', self.http_status),
            ('http_reason', self.http_reason)])

        return 'SproxydException(%s)' % args


class SproxydConfException(SproxydException):
    '''Exception raised when an invalid Sproxyd conf is detected.'''


class InvariantViolation(RuntimeError):
    '''Exception raised when some invariant is violated

    If this ever occurs at runtime, something is very wrong.
    '''
