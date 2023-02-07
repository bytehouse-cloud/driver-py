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
from bytehouse_driver import errors

ErrorCodes = errors.ErrorCodes


class NullableTestCase(NumpyBaseTestCase):
    def test_simple(self):
        columns = 'a Nullable(Int32)'

        data = [np.array([3, None, 2], dtype=object)]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(inserted[0], data[0])
            self.assertEqual(inserted[0].dtype, object)

    def test_simple_dataframe(self):
        columns = (
            'a Int64, '
            'b Nullable(Float64), '
            'c Nullable(String), '
            'd Nullable(Int64)'
        )

        df = pd.DataFrame({
            'a': [1, 2, 3],
            'b': [1.0, None, np.nan],
            'c': ['a', None, np.nan],
            'd': [1, None, None],
        }, dtype=object)
        expected = pd.DataFrame({
            'a': np.array([1, 2, 3], dtype=np.int64),
            'b': np.array([1.0, None, np.nan], dtype=object),
            'c': np.array(['a', None, None], dtype=object),
            'd': np.array([1, None, None], dtype=object),
        })

        with self.create_table(columns):
            rv = self.client.insert_dataframe('INSERT INTO test VALUES', df)
            self.assertEqual(rv, 3)
            df2 = self.client.query_dataframe('SELECT * FROM test ORDER BY a')
            self.assertTrue(expected.equals(df2))
