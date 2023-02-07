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

from datetime import date, datetime
from enum import Enum
from uuid import UUID

from pytz import timezone


escape_chars_map = {
    "\b": "\\b",
    "\f": "\\f",
    "\r": "\\r",
    "\n": "\\n",
    "\t": "\\t",
    "\0": "\\0",
    "\a": "\\a",
    "\v": "\\v",
    "\\": "\\\\",
    "'": "\\'"
}


def escape_datetime(item, context):
    server_tz = timezone(context.server_info.timezone)

    if item.tzinfo is not None:
        item = item.astimezone(server_tz)

    return "'%s'" % item.strftime('%Y-%m-%d %H:%M:%S')


def escape_param(item, context):
    if item is None:
        return 'NULL'

    elif isinstance(item, datetime):
        return escape_datetime(item, context)

    elif isinstance(item, date):
        return "'%s'" % item.strftime('%Y-%m-%d')

    elif isinstance(item, str):
        return "'%s'" % ''.join(escape_chars_map.get(c, c) for c in item)

    elif isinstance(item, list):
        return "[%s]" % ', '.join(str(escape_param(x, context)) for x in item)

    elif isinstance(item, tuple):
        return "(%s)" % ', '.join(str(escape_param(x, context)) for x in item)

    elif isinstance(item, Enum):
        return escape_param(item.value, context)

    elif isinstance(item, UUID):
        return "'%s'" % str(item)

    else:
        return item


def escape_params(params, context):
    escaped = {}

    for key, value in params.items():
        escaped[key] = escape_param(value, context)

    return escaped
