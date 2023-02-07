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

from ... import defines
from .base import NumpyColumn


class NumpyStringColumn(NumpyColumn):
    null_value = ''

    default_encoding = defines.STRINGS_ENCODING

    def __init__(self, encoding=default_encoding, **kwargs):
        self.encoding = encoding
        super(NumpyStringColumn, self).__init__(**kwargs)

    def read_items(self, n_items, buf):
        return np.array(
            buf.read_strings(n_items, encoding=self.encoding), dtype=self.dtype
        )

    def write_items(self, items, buf):
        return buf.write_strings(items.tolist(), encoding=self.encoding)


class NumpyByteStringColumn(NumpyColumn):
    null_value = b''

    def read_items(self, n_items, buf):
        return np.array(buf.read_strings(n_items), dtype=self.dtype)

    def write_items(self, items, buf):
        return buf.write_strings(items.tolist())


class NumpyFixedString(NumpyStringColumn):
    def __init__(self, length, **kwargs):
        self.length = length
        super(NumpyFixedString, self).__init__(**kwargs)

    def read_items(self, n_items, buf):
        return np.array(buf.read_fixed_strings(
            n_items, self.length, encoding=self.encoding
        ), dtype=self.dtype)

    def write_items(self, items, buf):
        return buf.write_fixed_strings(
            items.tolist(), self.length, encoding=self.encoding
        )


class NumpyByteFixedString(NumpyByteStringColumn):
    def __init__(self, length, **kwargs):
        self.length = length
        super(NumpyByteFixedString, self).__init__(**kwargs)

    def read_items(self, n_items, buf):
        return np.array(
            buf.read_fixed_strings(n_items, self.length), dtype=self.dtype
        )

    def write_items(self, items, buf):
        return buf.write_fixed_strings(items.tolist(), self.length)


def create_string_column(spec, column_options):
    client_settings = column_options['context'].client_settings
    strings_as_bytes = client_settings['strings_as_bytes']
    encoding = client_settings.get(
        'strings_encoding', NumpyStringColumn.default_encoding
    )

    if spec == 'String':
        cls = NumpyByteStringColumn if strings_as_bytes else NumpyStringColumn
        return cls(encoding=encoding, **column_options)
    else:
        length = int(spec[12:-1])
        cls = NumpyByteFixedString if strings_as_bytes else NumpyFixedString
        return cls(length, encoding=encoding, **column_options)
