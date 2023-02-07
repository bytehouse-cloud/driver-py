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

import unittest
from datetime import date

from tests.testcase import BaseTestCase
from bytehouse_driver import errors
from bytehouse_driver.errors import ServerException
from tests.util import require_server_version


class InsertTestCase(BaseTestCase):
    def test_type_mismatch(self):
        with self.create_table('a Float32'):
            with self.assertRaises(errors.TypeMismatchError):
                data = [(date(2012, 10, 25), )]
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

    def test_no_such_column(self):
        with self.create_table('a Float32'):
            with self.assertRaises(ServerException):
                data = [(1, )]
                self.client.execute(
                    'INSERT INTO test (b) VALUES', data
                )

    def test_data_malformed_rows(self):
        with self.create_table('a Int8'), self.assertRaises(TypeError):
            data = [1]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

    def test_data_less_columns_then_expected(self):
        with self.create_table('a Int8, b Int8'):
            with self.assertRaises(ValueError) as e:
                data = [(1, )]
                self.client.execute(
                    'INSERT INTO test (a, b) VALUES', data
                )
            self.assertEqual(str(e.exception), 'Expected 2 columns, got 1')

    def test_data_more_columns_then_expected(self):
        with self.create_table('a Int8, b Int8'):
            with self.assertRaises(ValueError) as e:
                data = [(1, 2, 3)]
                self.client.execute(
                    'INSERT INTO test (a, b) VALUES', data
                )
            self.assertEqual(str(e.exception), 'Expected 2 columns, got 3')

    def test_data_different_rows_length(self):
        with self.create_table('a Int8, b Int8'):
            with self.assertRaises(ValueError) as e:
                data = [(1, 2), (3, )]
                self.client.execute(
                    'INSERT INTO test (a, b) VALUES', data
                )
            self.assertEqual(str(e.exception), 'Different rows length')

    def test_data_different_rows_length_from_dicts(self):
        with self.create_table('a Int8, b Int8'):
            with self.assertRaises(KeyError):
                data = [{'a': 1, 'b': 2}, {'a': 3}]
                self.client.execute(
                    'INSERT INTO test (a, b) VALUES', data
                )

    def test_data_unsupported_row_type(self):
        with self.create_table('a Int8'):
            with self.assertRaises(TypeError) as e:
                data = [1]
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data, types_check=True
                )
            self.assertIn('dict, list or tuple is expected', str(e.exception))

    def test_data_dicts_ok(self):
        with self.create_table('a Int8, b Int8'):
            data = [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}]
            self.client.execute(
                'INSERT INTO test (a, b) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [(1, 2), (3, 4)])

    def test_data_generator_type(self):
        with self.create_table('a Int8'):
            data = ((x, ) for x in range(3))
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )
            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [(0,), (1, ), (2, )])

    def test_data_dicts_mixed_with_lists(self):
        with self.create_table('a Int8'):
            with self.assertRaises(TypeError) as e:
                data = [{'a': 1}, (2, )]
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data, types_check=True
                )

            self.assertIn('dict is expected', str(e.exception))

            with self.assertRaises(TypeError) as e:
                data = [(1, ), {'a': 2}]
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data, types_check=True
                )

            self.assertIn('list or tuple is expected', str(e.exception))

    @unittest.skip("no data for insert query")
    def test_empty_insert(self):
        with self.create_table('a Int8'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', []
            )

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [])

    def test_insert_from_select(self):
        with self.create_table('a UInt64'):
            inserted = self.client.execute(
                'INSERT INTO test (a) '
                'SELECT 0'
            )
            self.assertEqual(inserted, [])

    def test_insert_return(self):
        with self.create_table('a Int8'):
            rv = self.client.execute(
                'INSERT INTO test (a) VALUES', [(x,) for x in range(5)]
            )
            self.assertEqual(rv, 5)

            rv = self.client.execute(
                'INSERT INTO test (a) VALUES', [(x,) for x in range(5)]
            )
            self.assertEqual(rv, 5)

    @unittest.skip("cnch: Unknown table function input")
    def test_insert_from_input(self):
        with self.create_table('a Int8'):
            data = [{'a': 1}]
            self.client.execute(
                "INSERT INTO test (a) "
                "SELECT a from input ('a Int8')",
                data
            )

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [1])


class InsertColumnarTestCase(BaseTestCase):
    def test_insert_tuple_ok(self):
        with self.create_table('a Int8, b Int8'):
            data = [(1, 2, 3), (4, 5, 6)]
            self.client.execute(
                'INSERT INTO test (a, b) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [(1, 4), (2, 5), (3, 6)])
            inserted = self.client.execute(query, columnar=True)
            self.assertEqual(inserted, [(1, 2, 3), (4, 5, 6)])

    def test_insert_data_different_column_length(self):
        with self.create_table('a Int8, b Int8'):
            with self.assertRaises(ValueError) as e:
                data = [(1, 2, 3), (4, 5)]
                self.client.execute(
                    'INSERT INTO test (a, b) VALUES', data, columnar=True
                )
            self.assertEqual(str(e.exception), 'Expected 3 rows, got 2')

    def test_data_less_columns_then_expected(self):
        with self.create_table('a Int8, b Int8'):
            with self.assertRaises(ValueError) as e:
                data = [(1, 2)]
                self.client.execute(
                    'INSERT INTO test (a, b) VALUES', data, columnar=True
                )
            self.assertEqual(str(e.exception), 'Expected 2 columns, got 1')

    def test_data_more_columns_then_expected(self):
        with self.create_table('a Int8, b Int8'):
            with self.assertRaises(ValueError) as e:
                data = [(1, 2), (3, 4), (5, 6)]
                self.client.execute(
                    'INSERT INTO test (a, b) VALUES', data, columnar=True
                )
            self.assertEqual(str(e.exception), 'Expected 2 columns, got 3')

    def test_data_invalid_types(self):
        with self.create_table('a Int8, b Int8'):
            with self.assertRaises(TypeError) as e:
                data = [(1, 2), {'a': 3, 'b': 4}]
                self.client.execute(
                    'INSERT INTO test (a, b) VALUES', data,
                    types_check=True, columnar=True
                )

            self.assertIn('list or tuple is expected', str(e.exception))

            with self.assertRaises(TypeError) as e:
                data = [(1, 2), 3]
                self.client.execute(
                    'INSERT INTO test (a, b) VALUES', data,
                    types_check=True, columnar=True
                )

            self.assertIn('list or tuple is expected', str(e.exception))
