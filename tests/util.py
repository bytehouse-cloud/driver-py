"""
This is the MIT license: http://www.opensource.org/licenses/mit-license.php

Copyright (c) 2017 by Konstantin Lebedev.

Copyright 2022- 2023 Bytedance Ltd. and/or its affiliates

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import os
from contextlib import contextmanager
from functools import wraps
import logging
from io import StringIO
from time import tzset
from unittest.mock import patch

import tzlocal


def skip_by_server_version(testcase, version_required):
    testcase.skipTest(
        'Minimum revision required: {}'.format(
            '.'.join(str(x) for x in version_required)
        )
    )


def require_server_version(*version_required):
    def check(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            self = args[0]
            self.client.connection.connect()

            current = self.client.connection.server_info.version_tuple()

            if version_required <= current:
                return f(*args, **kwargs)
            else:
                skip_by_server_version(self, version_required)

        return wrapper
    return check


class LoggingCapturer(object):
    def __init__(self, logger_name, level):
        self.old_stdout_handlers = []
        self.logger = logging.getLogger(logger_name)
        self.level = level
        super(LoggingCapturer, self).__init__()

    def __enter__(self):
        buffer = StringIO()

        self.new_handler = logging.StreamHandler(buffer)
        self.logger.addHandler(self.new_handler)
        self.old_logger_level = self.logger.level
        self.logger.setLevel(self.level)

        return buffer

    def __exit__(self, *exc_info):
        self.logger.setLevel(self.old_logger_level)
        self.logger.removeHandler(self.new_handler)


capture_logging = LoggingCapturer


def bust_tzlocal_cache():
    try:
        tzlocal.unix._cache_tz = None
        tzlocal.unix._cache_tz_name = None
    except AttributeError:
        pass

    try:
        tzlocal.win32._cache_tz = None
        tzlocal.unix._cache_tz_name = None
    except AttributeError:
        pass


@contextmanager
def patch_env_tz(tz_name):
    bust_tzlocal_cache()

    # Although in many cases, changing the TZ environment variable may
    # affect the output of functions like localtime() without calling
    # tzset(), this behavior should not be relied on.
    # https://docs.python.org/3/library/time.html#time.tzset
    with patch.dict(os.environ, {'TZ': tz_name}):
        tzset()
        yield

    tzset()
