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

import inspect
import logging
import functools

DEFAULT_LOGGER = logging.getLogger(__name__)

TRACE = 5
logging.addLevelName(TRACE, 'TRACE')

def log_trace(self, msg, *args, **kwargs):
    if self.isEnabledFor(TRACE):
        self._log(TRACE, msg, args, **kwargs)

logging.Logger.trace = log_trace
del log_trace

def trace(f):
    tid = [0]

    name = f.func_name

    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        ctid, tid[0] = tid[0], tid[0] + 1

        maybe_self = None

        if len(args) > 0:
            maybe_self = args[0]
        else:
            maybe_self = kwargs.get('self', None)

        logger = getattr(maybe_self, 'logger', DEFAULT_LOGGER)

        all_args = inspect.getcallargs(f, *args, **kwargs)
        logger.trace('==> %s (%d): call %r', name, ctid, all_args)

        result = None

        try:
            result = f(*args, **kwargs)
        except BaseException as exc:
            logger.trace('<== %s (%d): exception %r', name, ctid, exc)
            raise
        else:
            logger.trace('<== %s (%d): return %r', name, ctid, result)
            return result

    return wrapped
