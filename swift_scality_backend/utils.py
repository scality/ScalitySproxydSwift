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

# Monkey-patch Python logging to support `trace` logging
TRACE_LEVEL = 5


def monkey_patch_log_trace(level):
    '''Monkey-patch `trace` support onto `logging.Logger`'''

    assert not hasattr(logging.Logger, 'trace')

    logging.addLevelName(level, 'TRACE')

    def trace(self, msg, *args, **kwargs):
        '''Logo 'msg % args' with severity 'TRACE'.

        To pass exception information, use the keyword argument exc_info with a
        true value, e.g.

        logger.trace("Houston, we have a %s", "thorny problem", exc_info=1)
        '''

        if self.isEnabledFor(level):
            self._log(level, msg, args, **kwargs)

    logging.Logger.trace = trace

monkey_patch_log_trace(TRACE_LEVEL)
del monkey_patch_log_trace
# End of monkey-patch


def trace(f):
    '''Trace calls to a decorated function

    Using this decorator on a function will cause its execution to be logged at
    `TRACE` level, including messages when the function is called (with a call
    identifier and the arguments), as well as when the function returns (also
    including the call identifier, and a representation of the return value or
    exception if applicable).
    '''

    # 'State' hack
    tid = [0]

    name = f.func_name

    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        maybe_self = None

        if len(args) > 0:
            maybe_self = args[0]
        else:
            maybe_self = kwargs.get('self', None)

        assert maybe_self is not None

        logger = getattr(maybe_self, 'logger', DEFAULT_LOGGER)

        # Fast-path, don't do any of the 'expensive' things below if the trace
        # log level isn't enabled anyway
        if not logger.isEnabledFor(TRACE_LEVEL):
            return f(*args, **kwargs)

        # Get & bump call identifier, assume non-preemptive threading
        ctid, tid[0] = tid[0], tid[0] + 1

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
