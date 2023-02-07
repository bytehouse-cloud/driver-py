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

from enum import IntEnum

from tests.testcase import BaseTestCase
from bytehouse_driver import errors


class A(IntEnum):
    hello = -1
    world = 2


class B(IntEnum):
    foo = -300
    bar = 300


class EnumTestCase(BaseTestCase):
    def test_simple(self):
        columns = (
            "a Enum8('hello' = -1, 'world' = 2), "
            "b Enum16('foo' = -300, 'bar' = 300)"
        )

        data = [(A.hello, B.bar), (A.world, B.foo), (-1, 300), (2, -300)]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a, b) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(
                inserted, [
                    ('hello', 'bar'), ('world', 'foo'),
                    ('hello', 'bar'), ('world', 'foo')
                ]
            )

    def test_enum_by_string(self):
        columns = "a Enum8('hello' = 1, 'world' = 2)"
        data = [('hello', ), ('world', )]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_errors(self):
        columns = "a Enum8('test' = 1, 'me' = 2)"
        data = [(A.world, )]
        with self.create_table(columns):
            with self.assertRaises(errors.LogicalError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

        columns = "a Enum8('test' = 1, 'me' = 2)"
        data = [(3, )]
        with self.create_table(columns):
            with self.assertRaises(errors.LogicalError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

    def test_nullable(self):
        columns = "a Nullable(Enum8('hello' = -1, 'world' = 2))"

        data = [(None, ), (A.hello, ), (None, ), (A.world, )]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(
                inserted, [
                    (None, ), ('hello', ), (None, ), ('world', ),
                ]
            )
