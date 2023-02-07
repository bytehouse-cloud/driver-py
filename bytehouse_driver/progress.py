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

from . import defines
from .varint import read_varint


class Progress(object):
    def __init__(self):
        self.rows = 0
        self.bytes = 0
        self.total_rows = 0
        self.written_rows = 0
        self.written_bytes = 0

        super(Progress, self).__init__()

    def read(self, server_revision, fin):
        self.rows = read_varint(fin)
        self.bytes = read_varint(fin)

        revision = server_revision
        if revision >= defines.DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS:
            self.total_rows = read_varint(fin)

        if revision >= defines.DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO:
            self.written_rows = read_varint(fin)
            self.written_bytes = read_varint(fin)

    def increment(self, another_progress):
        self.rows += another_progress.rows
        self.bytes += another_progress.bytes
        self.total_rows += another_progress.total_rows
        self.written_rows += another_progress.written_rows
        self.written_bytes += another_progress.written_bytes
