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

import os
from datetime import date, datetime
from unittest.mock import patch

from freezegun import freeze_time

from tests.testcase import BaseTestCase


class DateTestCase(BaseTestCase):
    @freeze_time('2017-03-05 03:00:00')
    def test_do_not_use_timezone(self):
        with self.create_table('a Date'):
            data = [(date(1970, 1, 2), )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            with patch.dict(os.environ, {'TZ': 'US/Hawaii'}):
                inserted = self.client.execute(query)
                self.assertEqual(inserted, data)

    def test_insert_datetime_to_date(self):
        with self.create_table('a Date'):
            testTime = datetime(2015, 6, 6, 12, 30, 54)
            self.client.execute(
                'INSERT INTO test (a) VALUES', [(testTime, )]
            )
            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [(testTime.date(), )])

    def test_wrong_date_insert(self):
        with self.create_table('a Date'):
            data = [
                (date(5555, 1, 1), ),
                (date(1, 1, 1), ),
                (date(2149, 6, 7), )
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)
            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            expected = [
                (date(1970, 1, 1), ),
                (date(1970, 1, 1), ),
                (date(1970, 1, 1), )
            ]
            self.assertEqual(inserted, expected)

    def test_boundaries(self):
        with self.create_table('a Date'):
            data = [
                (date(1970, 1, 1), ),
                (date(2106, 2, 7), )
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
