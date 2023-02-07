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

from .reader import read_binary_uint8
from .varint import read_varint


class BlockStreamProfileInfo(object):
    def __init__(self):
        self.rows = 0
        self.blocks = 0
        self.bytes = 0
        self.applied_limit = False  # bool
        self.rows_before_limit = 0
        self.calculated_rows_before_limit = 0  # bool

        super(BlockStreamProfileInfo, self).__init__()

    def read(self, fin):
        self.rows = read_varint(fin)
        self.blocks = read_varint(fin)
        self.bytes = read_varint(fin)
        self.applied_limit = bool(read_binary_uint8(fin))
        self.rows_before_limit = read_varint(fin)
        self.calculated_rows_before_limit = bool(read_binary_uint8(fin))
