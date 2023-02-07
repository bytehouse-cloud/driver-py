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

import numpy as np

from .base import NumpyColumn
from ..util import get_inner_spec, get_inner_columns


class TupleColumn(NumpyColumn):
    def __init__(self, nested_columns, **kwargs):
        self.nested_columns = nested_columns
        super(TupleColumn, self).__init__(**kwargs)

    def write_data(self, items, buf):
        names = items.dtype.names
        for i, (x, name) in enumerate(zip(self.nested_columns, names)):
            x.write_data(items[name], buf)

    def write_items(self, items, buf):
        return self.write_data(items, buf)

    def read_data(self, n_items, buf):
        data = [x.read_data(n_items, buf) for x in self.nested_columns]
        dtype = [('f{}'.format(i), x.dtype) for i, x in enumerate(data)]
        rv = np.empty(n_items, dtype=dtype)
        for i, x in enumerate(data):
            rv['f{}'.format(i)] = x
        return rv

    def read_items(self, n_items, buf):
        return self.read_data(n_items, buf)


def create_tuple_column(spec, column_by_spec_getter, column_options):
    inner_spec = get_inner_spec('Tuple', spec)
    columns = get_inner_columns(inner_spec)

    return TupleColumn([column_by_spec_getter(x) for x in columns],
                       **column_options)
