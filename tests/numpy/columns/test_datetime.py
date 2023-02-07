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

from datetime import datetime, date
from unittest.mock import patch

try:
    import numpy as np
except ImportError:
    np = None

try:
    import pandas as pd
except ImportError:
    pd = None

from pytz import UnknownTimeZoneError
import tzlocal

from tests.numpy.testcase import NumpyBaseTestCase


class BaseDateTimeTestCase(NumpyBaseTestCase):
    def make_numpy_d64ns(self, items):
        return np.array(items, dtype='datetime64[ns]')


class DateTimeTestCase(BaseDateTimeTestCase):
    def test_datetime_type(self):
        query = 'SELECT now()'

        rv = self.client.execute(query, columnar=True)
        self.assertIsInstance(rv[0][0], np.datetime64)

    def test_datetime64_type(self):
        query = 'SELECT now64()'

        rv = self.client.execute(query, columnar=True)
        self.assertIsInstance(rv[0][0], np.datetime64)

    def test_simple(self):
        with self.create_table('a Date, b DateTime'):
            data = [
                np.array(['2012-10-25'], dtype='datetime64[D]'),
                np.array(['2012-10-25T14:07:19'], dtype='datetime64[ns]')
            ]
            self.client.execute(
                'INSERT INTO test (a, b) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(
                inserted[0], np.array(['2012-10-25'], dtype='datetime64[D]')
            )
            self.assertArraysEqual(
                inserted[1], self.make_numpy_d64ns(['2012-10-25T14:07:19'])
            )

    def test_nullable_date(self):
        with self.create_table('a Nullable(Date)'):
            data = [
                np.array([None, date(2012, 10, 25), None, date(2017, 6, 23)],
                         dtype=object)
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(inserted[0], data[0])
            self.assertEqual(inserted[0].dtype, object)

    def test_nullable_datetime(self):
        with self.create_table('a Nullable(DateTime)'):
            data = [
                np.array([
                    None, datetime(2012, 10, 25, 14, 7, 19),
                    None, datetime(2017, 6, 23, 19, 10, 15)
                ], dtype=object)
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, columnar=True)

            self.assertArraysEqual(inserted[0], data[0])
            self.assertEqual(inserted[0].dtype, object)

    def test_handle_errors_from_tzlocal(self):
        with patch('tzlocal.get_localzone') as mocked:
            mocked.side_effect = UnknownTimeZoneError()
            self.client.execute('SELECT now()')

        if hasattr(tzlocal, 'get_localzone_name'):
            with patch('tzlocal.get_localzone_name') as mocked:
                mocked.side_effect = None
                self.client.execute('SELECT now()')

    def test_insert_integer_bounds(self):
        with self.create_table('a DateTime'):
            self.client.execute(
                'INSERT INTO test (a) VALUES',
                [np.array([0, 1, 1500000000, 2 ** 32 - 1], dtype=np.uint32)],
                columnar=True
            )

            query = 'SELECT toUInt32(a) FROM test ORDER BY a'
            inserted = self.client.execute(query)
            self.assertEqual(inserted[0], np.array(0))
