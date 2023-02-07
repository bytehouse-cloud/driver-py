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

import re
from .base import Column
from .intcolumn import UInt64Column
from ..util.helpers import pairwise


comma_re = re.compile(r',(?![^()]*\))')


class MapColumn(Column):
    py_types = (dict, )

    null_value = {}

    def __init__(self, key_column, value_column, **kwargs):
        self.offset_column = UInt64Column()
        self.key_column = key_column
        self.value_column = value_column
        super(MapColumn, self).__init__(**kwargs)

    def read_state_prefix(self, buf):
        self.key_column.read_state_prefix(buf)
        self.value_column.read_state_prefix(buf)

    def write_state_prefix(self, buf):
        self.key_column.write_state_prefix(buf)
        self.value_column.write_state_prefix(buf)

    def read_items(self, n_items, buf):
        offsets = list(self.offset_column.read_items(n_items, buf))
        last_offset = offsets[-1]
        keys = self.key_column.read_data(last_offset, buf)
        values = self.value_column.read_data(last_offset, buf)

        offsets.insert(0, 0)

        return [
            dict(zip(keys[begin:end], values[begin:end]))
            for begin, end in pairwise(offsets)
        ]

    def write_items(self, items, buf):
        offsets = []
        keys = []
        values = []

        total = 0
        for x in items:
            total += len(x)
            offsets.append(total)
            keys.extend(x.keys())
            values.extend(x.values())

        self.offset_column.write_items(offsets, buf)
        self.key_column.write_data(keys, buf)
        self.value_column.write_data(values, buf)


def create_map_column(spec, column_by_spec_getter, column_options):
    # Match commas outside of parentheses so we don't match the comma in
    # Decimal types.
    key, value = comma_re.split(spec[4:-1])
    key_column = column_by_spec_getter(key.strip())
    value_column = column_by_spec_getter(value.strip())

    return MapColumn(key_column, value_column, **column_options)
