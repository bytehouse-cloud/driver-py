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

from lz4 import block

from .base import BaseCompressor, BaseDecompressor
from ..protocol import CompressionMethod, CompressionMethodByte


class Compressor(BaseCompressor):
    method = CompressionMethod.LZ4
    method_byte = CompressionMethodByte.LZ4
    mode = 'default'

    def compress_data(self, data):
        return block.compress(data, store_size=False, mode=self.mode)


class Decompressor(BaseDecompressor):
    method = CompressionMethod.LZ4
    method_byte = CompressionMethodByte.LZ4

    def decompress_data(self, data, uncompressed_size):
        return block.decompress(data, uncompressed_size=uncompressed_size)
