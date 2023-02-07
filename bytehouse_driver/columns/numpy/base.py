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
import pandas as pd

from ..base import Column


class NumpyColumn(Column):
    dtype = None

    normalize_null_value = True

    def read_items(self, n_items, buf):
        data = buf.read(n_items * self.dtype.itemsize)
        return np.frombuffer(data, self.dtype.newbyteorder('<'), n_items)

    def write_items(self, items, buf):
        buf.write(items.astype(self.dtype.newbyteorder('<')).tobytes())

    def _write_nulls_map(self, items, buf):
        s = self.make_null_struct(len(items))
        nulls_map = self._get_nulls_map(items)
        buf.write(s.pack(*nulls_map))

    def _get_nulls_map(self, items):
        return [bool(x) for x in pd.isnull(items)]

    def _read_data(self, n_items, buf, nulls_map=None):
        items = self.read_items(n_items, buf)

        if self.after_read_items:
            return self.after_read_items(items, nulls_map)
        elif nulls_map is not None:
            items = np.array(items, dtype=object)
            np.place(items, nulls_map, None)

        return items

    def prepare_items(self, items):
        nulls_map = pd.isnull(items)

        # Always replace null values to null_value for proper inserts into
        # non-nullable columns.
        if isinstance(items, np.ndarray) and self.normalize_null_value:
            items = np.array(items)
            np.place(items, nulls_map, self.null_value)

        return items
