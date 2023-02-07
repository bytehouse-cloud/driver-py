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

from io import BytesIO

from ..reader import read_binary_uint32
from ..writer import write_binary_uint8, write_binary_uint32
from .. import errors

try:
    from clickhouse_cityhash.cityhash import CityHash128
except ImportError:
    raise RuntimeError(
        'Package clickhouse-cityhash is required to use compression'
    )


class BaseCompressor(object):
    """
    Partial file-like object with write method.
    """
    method = None
    method_byte = None

    def __init__(self):
        self.data = BytesIO()

        super(BaseCompressor, self).__init__()

    def get_value(self):
        value = self.data.getvalue()
        self.data.seek(0)
        self.data.truncate()
        return value

    def write(self, p_str):
        self.data.write(p_str)

    def compress_data(self, data):
        raise NotImplementedError

    def get_compressed_data(self, extra_header_size):
        rv = BytesIO()

        data = self.get_value()
        compressed = self.compress_data(data)

        header_size = extra_header_size + 4 + 4  # sizes

        write_binary_uint32(header_size + len(compressed), rv)
        write_binary_uint32(len(data), rv)
        rv.write(compressed)

        return rv.getvalue()


class BaseDecompressor(object):
    method = None
    method_byte = None

    def __init__(self, real_stream):
        self.stream = real_stream
        super(BaseDecompressor, self).__init__()

    def decompress_data(self, data, uncompressed_size):
        raise NotImplementedError

    def check_hash(self, compressed_data, compressed_hash):
        if CityHash128(compressed_data) != compressed_hash:
            raise errors.ChecksumDoesntMatchError()

    def get_decompressed_data(self, method_byte, compressed_hash,
                              extra_header_size):
        size_with_header = read_binary_uint32(self.stream)
        compressed_size = size_with_header - extra_header_size - 4

        compressed = BytesIO(self.stream.read(compressed_size))

        block_check = BytesIO()
        write_binary_uint8(method_byte, block_check)
        write_binary_uint32(size_with_header, block_check)
        block_check.write(compressed.getvalue())

        self.check_hash(block_check.getvalue(), compressed_hash)

        uncompressed_size = read_binary_uint32(compressed)

        compressed = compressed.read(compressed_size - 4)

        return self.decompress_data(compressed, uncompressed_size)
