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

import logging

from ..writer import write_binary_str, write_binary_uint8
from .available import settings as available_settings


logger = logging.getLogger(__name__)


def write_settings(settings, buf, settings_as_strings, is_important=False):
    for setting, value in (settings or {}).items():
        # If the server support settings as string we do not need to know
        # anything about them, so we can write any setting.
        if settings_as_strings:
            write_binary_str(setting, buf)
            write_binary_uint8(int(is_important), buf)
            write_binary_str(str(value), buf)

        else:
            # If the server requires string in binary,
            # then they cannot be written without type.
            setting_writer = available_settings.get(setting)
            if not setting_writer:
                logger.warning('Unknown setting %s. Skipping', setting)
                continue
            write_binary_str(setting, buf)
            setting_writer.write(value, buf)

    write_binary_str('', buf)  # end of settings
