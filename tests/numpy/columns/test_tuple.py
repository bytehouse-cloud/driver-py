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

try:
    import numpy as np
except ImportError:
    np = None

from tests.numpy.testcase import NumpyBaseTestCase


class TupleTestCase(NumpyBaseTestCase):
    def test_simple(self):
        columns = 'a Tuple(Int32, String)'
        dtype = [('f0', np.int32), ('f1', '<U1')]
        data = [
            np.array([(1, 'a'), (2, 'b')], dtype=dtype)
        ]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(inserted[0], data[0])
            self.assertEqual(inserted[0].dtype, dtype)

    def test_tuple_single_element(self):
        columns = 'a Tuple(Int32)'
        dtype = [('f0', np.int32)]
        data = [
            np.array([(1, ), (2, )], dtype=dtype)
        ]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(inserted[0], data[0])
            self.assertEqual(inserted[0].dtype, dtype)

    def test_nullable(self):
        with self.create_table('a Tuple(Nullable(Int32), Nullable(String))'):
            dtype = [('f0', object), ('f1', object)]
            data = [
                np.array([
                    (1, 'a'),
                    (2, None), (None, None), (None, 'd'),
                    (5, 'e')
                ], dtype=dtype)
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(inserted[0], data[0])
            self.assertEqual(inserted[0].dtype, dtype)

    def test_nested_tuple_with_common_types(self):
        columns = 'a Tuple(String, Tuple(Int32, String), String)'
        dtype = [
            ('f0', '<U5'),
            ('f1', np.dtype([('f0', np.int32), ('f1', '<U1')])),
            ('f2', '<U4')
        ]
        data = [
            np.array([
                ('one', (1, 'a'), 'two'),
                ('three', (2, 'b'), 'four'),
            ], dtype=dtype)
        ]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(inserted[0], data[0])
            self.assertEqual(inserted[0].dtype, dtype)
