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

from .base import Column
from .util import get_inner_spec, get_inner_columns


class TupleColumn(Column):
    py_types = (list, tuple)

    def __init__(self, nested_columns, **kwargs):
        self.nested_columns = nested_columns
        super(TupleColumn, self).__init__(**kwargs)
        self.null_value = tuple(x.null_value for x in nested_columns)

    def write_data(self, items, buf):
        items = self.prepare_items(items)
        items = list(zip(*items))

        for i, x in enumerate(self.nested_columns):
            x.write_data(list(items[i]), buf)

    def write_items(self, items, buf):
        return self.write_data(items, buf)

    def read_data(self, n_items, buf):
        rv = [x.read_data(n_items, buf) for x in self.nested_columns]
        return list(zip(*rv))

    def read_items(self, n_items, buf):
        return self.read_data(n_items, buf)


def create_tuple_column(spec, column_by_spec_getter, column_options):
    inner_spec = get_inner_spec('Tuple', spec)
    columns = get_inner_columns(inner_spec)

    return TupleColumn([column_by_spec_getter(x) for x in columns],
                       **column_options)
