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


class NumpyInt8Column(NumpyColumn):
    dtype = np.dtype(np.int8)
    ch_type = 'Int8'


class NumpyUInt8Column(NumpyColumn):
    dtype = np.dtype(np.uint8)
    ch_type = 'UInt8'


class NumpyInt16Column(NumpyColumn):
    dtype = np.dtype(np.int16)
    ch_type = 'Int16'


class NumpyUInt16Column(NumpyColumn):
    dtype = np.dtype(np.uint16)
    ch_type = 'UInt16'


class NumpyInt32Column(NumpyColumn):
    dtype = np.dtype(np.int32)
    ch_type = 'Int32'


class NumpyUInt32Column(NumpyColumn):
    dtype = np.dtype(np.uint32)
    ch_type = 'UInt32'


class NumpyInt64Column(NumpyColumn):
    dtype = np.dtype(np.int64)
    ch_type = 'Int64'


class NumpyUInt64Column(NumpyColumn):
    dtype = np.dtype(np.uint64)
    ch_type = 'UInt64'
