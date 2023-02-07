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

from struct import Struct, error as struct_error

from . import exceptions


class Column(object):
    ch_type = None
    py_types = None

    check_item = None
    after_read_items = None
    before_write_items = None

    types_check_enabled = False

    null_value = 0

    def __init__(self, types_check=False, **kwargs):
        self.nullable = False
        self.types_check_enabled = types_check
        self.input_null_as_default = False
        if 'context' in kwargs:
            settings = kwargs['context'].client_settings
            self.input_null_as_default = settings \
                .get('input_format_null_as_default', False)

        super(Column, self).__init__()

    def make_null_struct(self, n_items):
        return Struct('<{}B'.format(n_items))

    def _read_nulls_map(self, n_items, buf):
        s = self.make_null_struct(n_items)
        return s.unpack(buf.read(s.size))

    def _write_nulls_map(self, items, buf):
        s = self.make_null_struct(len(items))
        items = [x is None for x in items]
        buf.write(s.pack(*items))

    def check_item_type(self, value):
        if not isinstance(value, self.py_types):
            raise exceptions.ColumnTypeMismatchException(value)

    def prepare_items(self, items):
        nullable = self.nullable
        null_value = self.null_value
        null_as_default = self.input_null_as_default

        check_item = self.check_item
        if self.types_check_enabled:
            check_item_type = self.check_item_type
        else:
            check_item_type = False

        if (not (self.nullable or null_as_default) and not check_item_type and
                not check_item and not self.before_write_items):
            return items

        nulls_map = [False] * len(items) if self.nullable else None
        for i, x in enumerate(items):
            if x is None:
                if nullable:
                    nulls_map[i] = True
                    x = null_value
                elif null_as_default:
                    x = null_value

            else:
                if check_item_type:
                    check_item_type(x)

                if check_item:
                    check_item(x)

            items[i] = x

        if self.before_write_items:
            self.before_write_items(items, nulls_map=nulls_map)

        return items

    def write_data(self, items, buf):
        if self.nullable:
            self._write_nulls_map(items, buf)

        self._write_data(items, buf)

    def _write_data(self, items, buf):
        prepared = self.prepare_items(items)
        self.write_items(prepared, buf)

    def write_items(self, items, buf):
        raise NotImplementedError

    def read_data(self, n_items, buf):
        if self.nullable:
            nulls_map = self._read_nulls_map(n_items, buf)
        else:
            nulls_map = None

        return self._read_data(n_items, buf, nulls_map=nulls_map)

    def _read_data(self, n_items, buf, nulls_map=None):
        items = self.read_items(n_items, buf)

        if self.after_read_items:
            return self.after_read_items(items, nulls_map)
        elif nulls_map is not None:
            return tuple(
                (None if is_null else items[i])
                for i, is_null in enumerate(nulls_map)
            )
        return items

    def read_items(self, n_items, buf):
        raise NotImplementedError

    def read_state_prefix(self, buf):
        pass

    def write_state_prefix(self, buf):
        pass


class FormatColumn(Column):
    """
    Uses struct.pack for bulk items writing.
    """

    format = None

    def make_struct(self, n_items):
        return Struct('<{}{}'.format(n_items, self.format))

    def write_items(self, items, buf):
        s = self.make_struct(len(items))
        try:
            buf.write(s.pack(*items))

        except struct_error as e:
            raise exceptions.StructPackException(e)

    def read_items(self, n_items, buf):
        s = self.make_struct(n_items)
        return s.unpack(buf.read(s.size))
