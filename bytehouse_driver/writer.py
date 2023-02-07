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

import struct

from .varint import write_varint


MAX_UINT64 = (1 << 64) - 1
MAX_INT64 = (1 << 63) - 1


def _byte(b):
    return bytes((b, ))


def write_binary_str(text, buf):
    text = text.encode('utf-8')
    write_binary_bytes(text, buf)


def write_binary_bytes(text, buf):
    write_varint(len(text), buf)
    buf.write(text)


def write_binary_int(number, buf, fmt):
    """
    Writes int from buffer with provided format.
    """
    fmt = '<' + fmt
    buf.write(struct.pack(fmt, number))


def write_binary_int8(number, buf):
    write_binary_int(number, buf, 'b')


def write_binary_int16(number, buf):
    write_binary_int(number, buf, 'h')


def write_binary_int32(number, buf):
    write_binary_int(number, buf, 'i')


def write_binary_int64(number, buf):
    write_binary_int(number, buf, 'q')


def write_binary_uint8(number, buf):
    write_binary_int(number, buf, 'B')


def write_binary_uint16(number, buf):
    write_binary_int(number, buf, 'H')


def write_binary_uint32(number, buf):
    write_binary_int(number, buf, 'I')


def write_binary_uint64(number, buf):
    write_binary_int(number, buf, 'Q')


def write_binary_uint128(number, buf):
    fmt = '<QQ'
    packed = struct.pack(fmt, (number >> 64) & MAX_UINT64, number & MAX_UINT64)
    buf.write(packed)
