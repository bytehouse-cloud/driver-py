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

from ctypes import c_float

from .base import FormatColumn


class FloatColumn(FormatColumn):
    py_types = (float, int)


class Float32Column(FloatColumn):
    ch_type = 'Float32'
    format = 'f'

    def __init__(self, types_check=False, **kwargs):
        super(Float32Column, self).__init__(types_check=types_check, **kwargs)

        if types_check:
            # Chop only bytes that fit current type.
            # Cast to -nan or nan if overflows.
            def before_write_items(items, nulls_map=None):
                null_value = self.null_value

                for i, item in enumerate(items):
                    if nulls_map and nulls_map[i]:
                        items[i] = null_value
                    else:
                        items[i] = c_float(item).value

            self.before_write_items = before_write_items


class Float64Column(FloatColumn):
    ch_type = 'Float64'
    format = 'd'
