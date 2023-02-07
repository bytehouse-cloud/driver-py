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

from decimal import Decimal

from bytehouse_driver import errors
from tests.testcase import BaseTestCase
from tests.util import require_server_version


class DecimalTestCase(BaseTestCase):
    def test_simple(self):
        with self.create_table('a Decimal(9, 5)'):
            data = [(Decimal('300.42'), ), (300.42, ), (-300, )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, types_check=True
            )
            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (Decimal('300.42'), ),
                (Decimal('300.42'), ),
                (Decimal('-300'), )
            ])

    def test_different_precisions(self):
        columns = 'a Decimal32(2), b Decimal64(2), c Decimal128(2)'

        with self.create_table(columns):
            data = [(
                Decimal('300.42'),
                # 300.42 + (1 << 34)
                Decimal('17179869484.42'),
                # 300.42 + (1 << 100)
                Decimal('1267650600228229401496703205676.42')
            )]
            self.client.execute(
                'INSERT INTO test (a, b, c) VALUES', data
            )

            # Casting to string saves precision.
            query = (
                'SELECT '
                'CAST(a AS String), CAST(b AS String), CAST(c AS String)'
                'FROM test'
            )

            inserted = self.client.execute('SELECT * FROM test')
            self.assertEqual(inserted, data)

    def test_different_precisions_negative(self):
        columns = 'a Decimal32(2), b Decimal64(2), c Decimal128(2)'

        with self.create_table(columns):
            data = [(
                Decimal('-300.42'),
                # 300.42 + (1 << 34)
                Decimal('-17179869484.42'),
                # 300.42 + (1 << 100)
                Decimal('-1267650600228229401496703205676.42')
            )]
            self.client.execute(
                'INSERT INTO test (a, b, c) VALUES', data
            )

            # Casting to string saves precision.
            query = (
                'SELECT '
                'CAST(a AS String), CAST(b AS String), CAST(c AS String)'
                'FROM test'
            )

            inserted = self.client.execute('SELECT * FROM test')
            self.assertEqual(inserted, data)

    def test_max_precisions(self):
        columns = 'a Decimal32(0), b Decimal64(0), c Decimal128(0)'

        with self.create_table(columns):
            data = [(
                Decimal(10**9 - 1),
                Decimal(10**18 - 1),
                Decimal(10**38 - 1)
            ), (
                Decimal(-10**9 + 1),
                Decimal(-10**18 + 1),
                Decimal(-10**38 + 1)
            )]
            self.client.execute(
                'INSERT INTO test (a, b, c) VALUES', data
            )

            # Casting to string saves precision.
            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nullable(self):
        with self.create_table('a Nullable(Decimal32(3))'):
            data = [(300.42, ), (None, )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, [(Decimal('300.42'), ), (None, ), ])

    def test_no_scale(self):
        with self.create_table('a Decimal32(0)'):
            data = [(2147483647, )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, [(Decimal('2147483647'), )])

    def test_type_mismatch(self):
        data = [(2147483649, )]
        with self.create_table('a Decimal32(0)'):
            with self.assertRaises(errors.TypeMismatchError) as e:
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data, types_check=True
                )

            self.assertIn('2147483649 for column "a"', str(e.exception))

            with self.assertRaises(errors.TypeMismatchError) as e:
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

            self.assertIn('Column a', str(e.exception))

    def test_preserve_precision(self):
        data = [(1.66, ), (1.15, )]

        with self.create_table('a Decimal(18, 2)'):
            self.client.execute('INSERT INTO test (a) VALUES', data)
            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (Decimal('1.66'), ),
                (Decimal('1.15'), )
            ])

    def test_precision_one_sign_after_point(self):
        data = [(1.6, ), (1.0, ), (12312.0, ), (999999.6, )]

        with self.create_table('a Decimal(8, 1)'):
            self.client.execute('INSERT INTO test (a) VALUES', data)
            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (Decimal('1.6'),),
                (Decimal('1.0'),),
                (Decimal('12312.0'),),
                (Decimal('999999.6'),)
            ])

    def test_truncates_scale(self):
        with self.create_table('a Decimal(9, 4)'):
            data = [(3.14159265358,), (2.7182,)]
            expected = [(Decimal('3.1415'),), (Decimal('2.7182'),)]
            self.client.execute(
                'INSERT INTO test (a) VALUES',
                data,
                types_check=True,
            )
            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, expected)
