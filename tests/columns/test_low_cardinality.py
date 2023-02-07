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

from datetime import date, timedelta
from decimal import Decimal

from tests.testcase import BaseTestCase


class LowCardinalityTestCase(BaseTestCase):
    def test_uint8(self):
        with self.create_table('a LowCardinality(UInt8)'):
            data = [(x, ) for x in range(255)]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_int8(self):
        with self.create_table('a LowCardinality(Int8)'):
            data = [(x - 127, ) for x in range(255)]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nullable_int8(self):
        with self.create_table('a LowCardinality(Nullable(Int8))'):
            data = [(None, ), (-1, ), (0, ), (1, ), (None, )]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_date(self):
        with self.create_table('a LowCardinality(Date)'):
            start = date(1970, 1, 1)
            data = [(start + timedelta(x), ) for x in range(300)]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_float(self):
        with self.create_table('a LowCardinality(Float)'):
            data = [(float(x),) for x in range(300)]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_decimal(self):
        with self.create_table('a LowCardinality(Float)'):
            data = [(Decimal(x),) for x in range(300)]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_array(self):
        with self.create_table('a Array(LowCardinality(Int16))'):
            data = [([100, 500], )]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_empty_array(self):
        with self.create_table('a Array(LowCardinality(Int16))'):
            data = [([], )]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_string(self):
        with self.create_table('a LowCardinality(String)'):
            data = [
                ('test', ), ('low', ), ('cardinality', ),
                ('test', ), ('test', ), ('', )
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_fixed_string(self):
        with self.create_table('a LowCardinality(FixedString(12))'):
            data = [
                ('test', ), ('low', ), ('cardinality', ),
                ('test', ), ('test', ), ('', )
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nullable_string(self):
        with self.create_table('a LowCardinality(Nullable(String))'):
            data = [
                ('test', ), ('', ), (None, )
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
