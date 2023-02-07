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

from bytehouse_driver import errors
from tests.testcase import BaseTestCase
from tests.util import require_server_version


class TupleTestCase(BaseTestCase):
    def entuple(self, lst):
        return tuple(
            self.entuple(x) if isinstance(x, list) else x for x in lst
        )

    def test_simple(self):
        columns = 'a Tuple(Int32, String)'
        data = [((1, 'a'), ), ((2, 'b'), )]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_tuple_single_element(self):
        columns = 'a Tuple(Int32)'
        data = [((1, ), ), ((2, ), )]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nullable(self):
        data = [
            ((1, 'a'),),
            ((2, None),), ((None, None),), ((None, 'd'),),
            ((5, 'e'),)
        ]

        with self.create_table('a Tuple(Nullable(Int32), Nullable(String))'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nested_tuple_with_common_types(self):
        columns = 'a Tuple(String, Tuple(Int32, String), String)'
        data = [
            (('one', (1, 'a'), 'two'),),
            (('three', (2, 'b'), 'four'),)
        ]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_tuple_of_tuples(self):
        columns = (
            "a Tuple("
            "Tuple(Int32, String),"
            "Tuple(Enum8('hello' = 1, 'world' = 2), Date)"
            ")"
        )
        data = [
            (((1, 'a'), (1, date(2020, 3, 11))),),
            (((2, 'b'), (2, date(2020, 3, 12))),)
        ]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(
                inserted, [
                    (((1, 'a'), ('hello', date(2020, 3, 11))), ),
                    (((2, 'b'), ('world', date(2020, 3, 12))), )
                ]
            )

    def test_tuple_of_arrays(self):
        data = [(([1, 2, 3],),), (([4, 5, 6],),)]

        with self.create_table('a Tuple(Array(Int32))'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    # Bug in Array of Tuple handing before 19.16.13:
    # DESCRIBE TABLE test
    #
    # | a.1  | Array(UInt8) |
    # | a.2  | Array(UInt8) |
    # | a.3  | Array(UInt8) |
    # https://github.com/ClickHouse/ClickHouse/pull/8866
    @unittest.skip("No such column a in table test: require clickhouse server version(19, 16, 13)")
    def test_array_of_tuples(self):
        data = [
            ([(1, 2, 3), (4, 5, 6)],),
            ([(7, 8, 9)],),
        ]

        with self.create_table('a Array(Tuple(UInt8, UInt8, UInt8))'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_type_mismatch_error(self):
        columns = 'a Tuple(Int32)'
        data = [('test', )]

        with self.create_table(columns):
            with self.assertRaises(errors.TypeMismatchError):
                self.client.execute('INSERT INTO test (a) VALUES', data)
