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

logger = logging.getLogger(__name__)


log_priorities = (
    'Unknown',
    'Fatal',
    'Critical',
    'Error',
    'Warning',
    'Notice',
    'Information',
    'Debug',
    'Trace'
)


def log_block(block):
    if block is None:
        return

    column_names = [x[0] for x in block.columns_with_types]

    for row in block.get_rows():
        row = dict(zip(column_names, row))

        if 1 <= row['priority'] <= 8:
            priority = log_priorities[row['priority']]
        else:
            priority = row[0]

        # thread_number in servers prior 20.x
        thread_id = row.get('thread_id') or row['thread_number']

        logger.info(
            '[ %s ] [ %s ] {%s} <%s> %s: %s',
            row['host_name'],
            thread_id,
            row['query_id'],
            priority,
            row['source'],
            row['text']
        )
