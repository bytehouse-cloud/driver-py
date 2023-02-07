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

from parameterized import parameterized

try:
    import numpy as np
except ImportError:
    np = None

from tests.numpy.testcase import NumpyBaseTestCase


class FloatTestCase(NumpyBaseTestCase):
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

    def test_float32(self):
        rv = self.get_query('Float32')
        self.check_result(rv, np.float32)

    def test_float64(self):
        rv = self.get_query('Float64')
        self.check_result(rv, np.float64)

    def test_fractional_round_trip(self):
        with self.create_table('a Float32'):
            data = [np.array([0.5, 1.5], dtype=np.float32)]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(inserted[0], data[0])

    @parameterized.expand(['Float32', 'Float64'])
    def test_nullable(self, float_type):
        with self.create_table('a Nullable({})'.format(float_type)):
            data = [np.array([np.nan, 0.5, None, 1.5], dtype=object)]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(
                inserted[0].astype(str), data[0].astype(str)
            )
            self.assertEqual(inserted[0].dtype, object)

    def test_nan(self):
        with self.create_table('a Float32'):
            data = [np.array([float('nan'), 0.5], dtype=np.float32)]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(
                inserted[0].astype(str), data[0].astype(str)
            )
            self.assertEqual(inserted[0].dtype, np.float32)
