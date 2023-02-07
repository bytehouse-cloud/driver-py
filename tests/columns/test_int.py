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

from tests.testcase import BaseTestCase
from bytehouse_driver import errors
from tests.util import require_server_version


class IntTestCase(BaseTestCase):
    def test_chop_to_type(self):
        with self.create_table('a UInt8'):
            data = [(300, )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, types_check=True
            )

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [(44, )])

        with self.create_table('a Int8'):
            data = [(-300,)]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, types_check=True
            )

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [(-44, )])

    def test_raise_struct_error(self):
        with self.create_table('a UInt8'):
            with self.assertRaises(errors.TypeMismatchError) as e:
                data = [(300, )]
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

            exc = str(e.exception)
            self.assertIn('Column a', exc)
            self.assertIn('types_check=True', exc)

    def test_uint_type_mismatch(self):
        data = [(-1, )]
        with self.create_table('a UInt8'):
            with self.assertRaises(errors.TypeMismatchError) as e:
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data, types_check=True
                )

            self.assertIn('-1 for column "a"', str(e.exception))

            with self.assertRaises(errors.TypeMismatchError) as e:
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

            self.assertIn('Column a', str(e.exception))

    def test_all_sizes(self):
        columns = (
            'a Int8, b Int16, c Int32, d Int64, '
            'e UInt8, f UInt16, g UInt32, h UInt64'
        )

        data = [
            (-10, -300, -123581321, -123581321345589144,
             10, 300, 123581321, 123581321345589144)
        ]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a, b, c, d, e, f, g, h) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_corner_cases(self):
        columns = (
            'a Int8, b Int16, c Int32, d Int64, '
            'e UInt8, f UInt16, g UInt32, h UInt64'
        )

        data = [
            (-128, -32768, -2147483648, -9223372036854775808,
             255, 65535, 4294967295, 18446744073709551615),
            (127, 32767, 2147483647, 9223372036854775807, 0, 0, 0, 0),
        ]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a, b, c, d, e, f, g, h) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nullable(self):
        with self.create_table('a Nullable(Int32)'):
            data = [(2, ), (None, ), (4, ), (None, ), (8, )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
