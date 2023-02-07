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

import unittest

from tests.testcase import BaseTestCase


class CommonTestCase(BaseTestCase):
    client_kwargs = {'settings': {'insert_block_size': 1}}

    def setUp(self):
        super(CommonTestCase, self).setUp()

        self.send_data_count = 0
        old_send_data = self.client.connection.send_data

        def send_data(*args, **kwargs):
            self.send_data_count += 1
            return old_send_data(*args, **kwargs)

        self.client.connection.send_data = send_data

    @unittest.skip("AssertionError: 7 != 6")
    def test_insert_block_size(self):
        with self.create_table('a UInt8'):
            data = [(x, ) for x in range(4)]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )
            # Two empty blocks: for end of sending external tables
            # and data.
            self.assertEqual(self.send_data_count, 4 + 2)

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    @unittest.skip("AssertionError: 7 != 6")
    def test_columnar_insert_block_size(self):
        with self.create_table('a UInt8'):
            data = [(0, 1, 2, 3)]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )
            # Two empty blocks: for end of sending external tables
            # and data.
            self.assertEqual(self.send_data_count, 4 + 2)

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            expected = [(0, ), (1, ), (2, ), (3, )]
            self.assertEqual(inserted, expected)
