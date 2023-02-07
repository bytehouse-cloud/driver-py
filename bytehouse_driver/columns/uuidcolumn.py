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

from uuid import UUID

from .base import FormatColumn
from .. import errors
from ..writer import MAX_UINT64


class UUIDColumn(FormatColumn):
    ch_type = 'UUID'
    py_types = (str, UUID)
    format = 'Q'

    # UUID is stored by two uint64 numbers.
    def write_items(self, items, buf):
        n_items = len(items)

        uint_64_pairs = [None] * 2 * n_items
        for i, x in enumerate(items):
            i2 = 2 * i
            uint_64_pairs[i2] = (x >> 64) & MAX_UINT64
            uint_64_pairs[i2 + 1] = x & MAX_UINT64

        s = self.make_struct(2 * n_items)
        buf.write(s.pack(*uint_64_pairs))

    def read_items(self, n_items, buf):
        # TODO: cythonize
        s = self.make_struct(2 * n_items)
        items = s.unpack(buf.read(s.size))

        uint_128_items = [None] * n_items
        for i in range(n_items):
            i2 = 2 * i
            uint_128_items[i] = (items[i2] << 64) + items[i2 + 1]

        return tuple(uint_128_items)

    def after_read_items(self, items, nulls_map=None):
        if nulls_map is None:
            return tuple(UUID(int=item) for item in items)
        else:
            return tuple(
                (None if is_null else UUID(int=items[i]))
                for i, is_null in enumerate(nulls_map)
            )

    def before_write_items(self, items, nulls_map=None):
        null_value = self.null_value

        for i, item in enumerate(items):
            if nulls_map and nulls_map[i]:
                items[i] = null_value
                continue

            try:
                if not isinstance(item, UUID):
                    item = UUID(item)

            except ValueError:
                raise errors.CannotParseUuidError(
                    "Cannot parse uuid '{}'".format(item)
                )

            items[i] = item.int
