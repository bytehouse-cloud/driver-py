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

from uuid import UUID

from tests.testcase import BaseTestCase
from bytehouse_driver import errors
from tests.util import require_server_version


class ArrayTestCase(BaseTestCase):
    def test_empty(self):
        columns = 'a Array(Int32)'

        data = [([], )]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_simple(self):
        columns = 'a Array(Int32)'
        data = [([100, 500], )]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_write_column_as_nested_array(self):
        columns = 'a Array(Int32)'
        data = [([100, 500], ), ([100, 500], )]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nested_with_enum(self):
        columns = "a Array(Array(Enum8('hello' = -1, 'world' = 2)))"

        data = [([['hello', 'world'], ['hello']], )]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nested_of_nested(self):
        columns = 'a Array(Array(Array(Int32))), b Array(Array(Array(Int32)))'
        data = [([
            [[255, 170], [127, 127, 127, 127, 127], [170, 170, 170], [170]],
            [[255, 255, 255], [255]], [[255], [255], [255]]
        ], [
            [[255, 170], [127, 127, 127, 127, 127], [170, 170, 170], [170]],
            [[255, 255, 255], [255]], [[255], [255], [255]]
        ])]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a, b) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_multidimensional(self):
        columns = "a Array(Array(Array(Nullable(String))))"
        data = [([[['str1_1', 'str1_2', None], [None]],
                  [['str1_3', 'str1_4', None], [None]]], ),
                ([[['str2_1', 'str2_2', None], [None]]], ),
                ([[['str3_1', 'str3_2', None], [None]]],)]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_empty_nested(self):
        columns = "a Array(Array(Array(Int32))), b Array(Array(Array(Int32)))"
        data = [
            ([], [[]],),
        ]

        with self.create_table(columns):
            self.client.execute("INSERT INTO test (a, b) VALUES", data)

            query = "SELECT * FROM test"

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_type_mismatch_error(self):
        columns = 'a Array(Int32)'
        data = [('test', )]

        with self.create_table(columns):
            with self.assertRaises(errors.TypeMismatchError):
                self.client.execute('INSERT INTO test (a) VALUES', data)

        data = [(['test'], )]

        with self.create_table(columns):
            with self.assertRaises(errors.TypeMismatchError):
                self.client.execute('INSERT INTO test (a) VALUES', data)

    def test_string_array(self):
        columns = 'a Array(String)'
        data = [(['aaa', 'bbb'], )]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_string_nullable_array(self):
        columns = 'a Array(Nullable(String))'
        data = [(['aaa', None, 'bbb'], )]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_uuid_array(self):
        columns = 'a Array(UUID)'
        data = [([
            UUID('c0fcbba9-0752-44ed-a5d6-4dfb4342b89d'),
            UUID('2efcead4-ff55-4db5-bdb4-6b36a308d8e0')
        ], )]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_uuid_nullable_array(self):
        columns = 'a Array(Nullable(UUID))'
        data = [([
            UUID('c0fcbba9-0752-44ed-a5d6-4dfb4342b89d'),
            None,
            UUID('2efcead4-ff55-4db5-bdb4-6b36a308d8e0')
        ], )]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
