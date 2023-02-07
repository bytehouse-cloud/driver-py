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

from datetime import date, datetime
from unittest.mock import patch

from pytz import UnknownTimeZoneError
import tzlocal

from tests.testcase import BaseTestCase


class DateTimeTestCase(BaseTestCase):
    def test_simple(self):
        with self.create_table('a Date, b DateTime'):
            data = [(date(2012, 10, 25), datetime(2012, 10, 25, 14, 7, 19))]
            self.client.execute(
                'INSERT INTO test (a, b) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nullable_date(self):
        with self.create_table('a Nullable(Date)'):
            data = [
                (None,), (date(2012, 10, 25),),
                (None,), (date(2017, 6, 23),)
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nullable_datetime(self):
        with self.create_table('a Nullable(DateTime)'):
            data = [
                (None,), (datetime(2012, 10, 25, 14, 7, 19),),
                (None,), (datetime(2017, 6, 23, 19, 10, 15),)
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_handle_errors_from_tzlocal(self):
        with patch('tzlocal.get_localzone') as mocked:
            mocked.side_effect = UnknownTimeZoneError()
            self.client.execute('SELECT now()')

        if hasattr(tzlocal, 'get_localzone_name'):
            with patch('tzlocal.get_localzone_name') as mocked:
                mocked.side_effect = None
                self.client.execute('SELECT now()')

    def test_insert_integers(self):
        with self.create_table('a DateTime'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', [(1530211034,)]
            )

            query = 'SELECT toUInt32(a), a FROM test'
            inserted = self.client.execute(query)
            delta = 8 * 3600
            self.assertEqual(inserted, [(1530211034, datetime.fromtimestamp(1530211034 - delta))])

    def test_insert_integer_bounds(self):
        with self.create_table('a DateTime'):
            self.client.execute(
                'INSERT INTO test (a) VALUES',
                [(0,), (1,), (1500000000,), (2 ** 32 - 1,)]
            )

            query = 'SELECT toUInt32(a) FROM test ORDER BY a'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [(0,), (1,), (1500000000,), (2 ** 32 - 1,)])
