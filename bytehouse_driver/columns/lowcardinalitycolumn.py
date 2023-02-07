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

from math import log

from ..reader import read_binary_uint64
from ..writer import write_binary_int64
from .base import Column
from .intcolumn import UInt8Column, UInt16Column, UInt32Column, UInt64Column


def create_low_cardinality_column(spec, column_by_spec_getter, column_options):
    inner = spec[15:-1]
    nested = column_by_spec_getter(inner)
    return LowCardinalityColumn(nested, **column_options)


class LowCardinalityColumn(Column):
    """
    Stores column as index (unique elements) and keys.
    Good for de-duplication of large values with low cardinality.
    """
    int_types = {
        0: UInt8Column,
        1: UInt16Column,
        2: UInt32Column,
        3: UInt64Column
    }

    # Need to read additional keys.
    # Additional keys are stored before indexes as value N and N keys
    # after them.
    has_additional_keys_bit = 1 << 9
    # Need to update dictionary.
    # It means that previous granule has different dictionary.
    need_update_dictionary = 1 << 10

    serialization_type = has_additional_keys_bit | need_update_dictionary

    def __init__(self, nested_column, **kwargs):
        self.nested_column = nested_column
        super(LowCardinalityColumn, self).__init__(**kwargs)

    def read_state_prefix(self, buf):
        return read_binary_uint64(buf)

    def write_state_prefix(self, buf):
        # KeysSerializationVersion. See ByteHouse docs.
        write_binary_int64(1, buf)

    def _write_data(self, items, buf):
        index, keys = [], []
        key_by_index_element = {}

        if self.nested_column.nullable:
            # First element represents NULL if column is nullable.
            index.append(self.nested_column.null_value)
            # Prevent null map writing. Reset nested column nullable flag.
            self.nested_column.nullable = False

            for x in items:
                if x is None:
                    # Zero element for null.
                    keys.append(0)

                else:
                    key = key_by_index_element.get(x)
                    # Get key from index or add it to index.
                    if key is None:
                        key = len(key_by_index_element)
                        key_by_index_element[x] = key
                        index.append(x)

                    keys.append(key + 1)
        else:
            for x in items:
                key = key_by_index_element.get(x)

                # Get key from index or add it to index.
                if key is None:
                    key = len(key_by_index_element)
                    key_by_index_element[x] = len(key_by_index_element)
                    index.append(x)

                keys.append(key)

        # Do not write anything for empty column.
        # May happen while writing empty arrays.
        if not len(index):
            return

        int_type = int(log(len(index), 2) / 8)
        int_column = self.int_types[int_type]()

        serialization_type = self.serialization_type | int_type

        write_binary_int64(serialization_type, buf)
        write_binary_int64(len(index), buf)

        self.nested_column.write_data(index, buf)
        write_binary_int64(len(items), buf)
        int_column.write_items(keys, buf)

    def _read_data(self, n_items, buf, nulls_map=None):
        if not n_items:
            return tuple()

        serialization_type = read_binary_uint64(buf)

        # Lowest byte contains info about key type.
        key_type = serialization_type & 0xf
        keys_column = self.int_types[key_type]()

        nullable = self.nested_column.nullable
        # Prevent null map reading. Reset nested column nullable flag.
        self.nested_column.nullable = False

        index_size = read_binary_uint64(buf)
        index = self.nested_column.read_data(index_size, buf)
        if nullable:
            index = (None, ) + index[1:]

        read_binary_uint64(buf)  # number of keys
        keys = keys_column.read_data(n_items, buf)

        return tuple(index[x] for x in keys)
