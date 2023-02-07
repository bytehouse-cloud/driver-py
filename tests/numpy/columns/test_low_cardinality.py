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

try:
    import pandas as pd
except ImportError:
    pd = None


from tests.numpy.testcase import NumpyBaseTestCase


class LowCardinalityTestCase(NumpyBaseTestCase):
    def check_result(self, inserted, data):
        self.assertArraysEqual(inserted[0], data[0])
        self.assertIsInstance(inserted[0], pd.Categorical)

    def test_uint8(self):
        with self.create_table('a LowCardinality(UInt8)'):
            data = [np.array(range(255))]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, columnar=True)
            self.check_result(inserted, data)

    def test_int8(self):
        with self.create_table('a LowCardinality(Int8)'):
            data = [np.array([x - 127 for x in range(255)])]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, columnar=True)
            self.check_result(inserted, data)

    def test_nullable_int8(self):
        with self.create_table('a LowCardinality(Nullable(Int8))'):
            data = [np.array([None, -1, 0, 1, None], dtype=object)]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(
                inserted[0].astype(str),
                pd.Categorical(data[0]).astype(str)
            )
            self.assertIsInstance(inserted[0], pd.Categorical)

    def test_date(self):
        with self.create_table('a LowCardinality(Date)'):
            data = [np.array(list(range(300)), dtype='datetime64[D]')]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query, columnar=True)
            self.check_result(inserted, data)

    def test_float(self):
        with self.create_table('a LowCardinality(Float)'):
            data = [np.array([float(x) for x in range(300)])]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query, columnar=True)
            self.check_result(inserted, data)

    def test_string(self):
        with self.create_table('a LowCardinality(String)'):
            data = [
                np.array(['test', 'low', 'cardinality', 'test', 'test', ''])
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, columnar=True)
            self.check_result(inserted, data)

    def test_insert_nan_string_into_non_nullable(self):
        with self.create_table('a LowCardinality(String)'):
            data = [
                np.array(['test', None], dtype=object)
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, columnar=True)
            self.check_result(inserted, [np.array(['test', ''])])

    def test_fixed_string(self):
        with self.create_table('a LowCardinality(FixedString(12))'):
            data = [
                np.array(['test', 'low', 'cardinality', 'test', 'test', ''])
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, columnar=True)
            self.check_result(inserted, data)

    def test_nullable_string(self):
        with self.create_table('a LowCardinality(Nullable(String))'):
            data = [
                np.array(['test', '', None], dtype=object)
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(
                inserted[0].astype(str), pd.Categorical(data[0]).astype(str)
            )
            self.assertIsInstance(inserted[0], pd.Categorical)
