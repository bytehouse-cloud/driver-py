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

import socket
from unittest import TestCase, mock

from bytehouse_driver.bufferedreader import BufferedSocketReader


class BufferedReaderTestCase(TestCase):
    def test_overflow_signed_int_string_size(self):
        data = b'\xFF\xFE\xFC\xFE\xFE\xFE\xFE\xFE\x29\x80\x40\x00\x00\x01'

        def recv_into(buf):
            size = len(data)
            buf[0:size] = data
            return size

        with mock.patch('socket.socket') as mock_socket:
            mock_socket.return_value.recv_into.side_effect = recv_into
            reader = BufferedSocketReader(socket.socket(), 1024)

            # Trying to allocate huge amount of memory.
            with self.assertRaises(MemoryError):
                reader.read_strings(5, encoding='utf-8')
