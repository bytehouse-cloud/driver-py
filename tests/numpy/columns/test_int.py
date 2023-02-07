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

try:
    import numpy as np
except ImportError:
    np = None

from tests.numpy.testcase import NumpyBaseTestCase


class IntTestCase(NumpyBaseTestCase):
    n = 10

    def check_result(self, rv, col_type):
        self.assertArraysEqual(rv[0], np.array(range(self.n)))
        self.assertEqual(rv[0].dtype, col_type)

    def get_query(self, ch_type):
        with self.create_table('a {}'.format(ch_type)):
            data = [np.array(range(self.n))]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            expected = []
            for x in range(self.n):
                expected.append(np.array(x, dtype=inserted[0].dtype))
            self.assertEqual(
                inserted, expected
            )
            return self.client.execute(query, columnar=True)

    def test_int8(self):
        rv = self.get_query('Int8')
        self.check_result(rv, np.int8)

    def test_int16(self):
        rv = self.get_query('Int16')
        self.check_result(rv, np.int16)

    def test_int32(self):
        rv = self.get_query('Int32')
        self.check_result(rv, np.int32)

    def test_int64(self):
        rv = self.get_query('Int64')
        self.check_result(rv, np.int64)

    def test_uint8(self):
        rv = self.get_query('UInt8')
        self.check_result(rv, np.uint8)

    def test_uint16(self):
        rv = self.get_query('UInt16')
        self.check_result(rv, np.uint16)

    def test_uint32(self):
        rv = self.get_query('UInt32')
        self.check_result(rv, np.uint32)

    def test_uint64(self):
        rv = self.get_query('UInt64')
        self.check_result(rv, np.uint64)

    def test_insert_nan_into_non_nullable(self):
        with self.create_table('a Int32'):
            data = [
                np.array([123, np.nan], dtype=object)
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            expected = [np.array(123, dtype='int32'), np.array(0, dtype='int32')]
            self.assertEqual(
                inserted,
                expected
            )

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(inserted[0], np.array([123, 0]))
            self.assertEqual(inserted[0].dtype, np.int32)

    def test_nullable(self):
        with self.create_table('a Nullable(Int32)'):
            data = [np.array([2, None, 4, None, 8])]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(inserted[0], data[0])
            self.assertEqual(inserted[0].dtype, object)