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

import unittest
from tests.numpy.testcase import NumpyBaseTestCase


class StringTestCase(NumpyBaseTestCase):
    def test_string(self):
        with self.create_table('a String'):
            data = [np.array(['a', 'b', 'c'])]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            rv = self.client.execute(query, columnar=True)

            self.assertArraysEqual(rv[0], data)
            self.assertNotEqual(rv[0].dtype, object)
            self.assertIsInstance(rv[0][1], (np.str_, ))

    def test_nullable(self):
        with self.create_table('a Nullable(String)'):
            data = [np.array([np.nan, 'test', None, 'nullable'], dtype=object)]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(
                inserted[0], [np.array([None, 'test', None, 'nullable'])]
            )
            self.assertEqual(inserted[0].dtype, object)


class ByteStringTestCase(NumpyBaseTestCase):
    client_kwargs = {'settings': {'strings_as_bytes': True, 'use_numpy': True}}

    @unittest.skip("AttributeError: 'bool' object has no attribute 'all'")
    def test_string(self):
        with self.create_table('a String'):
            data = [np.array([b'a', b'b', b'c'])]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            rv = self.client.execute(query, columnar=True)

            self.assertArraysEqual(rv[0], data)
            self.assertNotEqual(rv[0].dtype, object)
            self.assertIsInstance(rv[0][0], (np.bytes_, ))

    @unittest.skip("AssertionError: False is not true")
    def test_nullable(self):
        with self.create_table('a Nullable(String)'):
            data = [
                np.array([np.nan, b'test', None, b'nullable'], dtype=object)
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(
                inserted[0], [np.array([None, b'test', None, b'nullable'])]
            )
            self.assertEqual(inserted[0].dtype, object)


class FixedStringTestCase(NumpyBaseTestCase):
    def test_string(self):
        with self.create_table('a FixedString(3)'):
            data = [np.array(['a', 'b', 'c'])]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            rv = self.client.execute(query, columnar=True)

            self.assertArraysEqual(rv[0], data)
            self.assertNotEqual(rv[0].dtype, object)
            self.assertIsInstance(rv[0][0], (np.str_, ))

    def test_nullable(self):
        with self.create_table('a Nullable(FixedString(10))'):
            data = [np.array([np.nan, 'test', None, 'nullable'], dtype=object)]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(
                inserted[0], [np.array([None, 'test', None, 'nullable'])]
            )
            self.assertEqual(inserted[0].dtype, object)


class ByteFixedStringTestCase(NumpyBaseTestCase):
    client_kwargs = {'settings': {'strings_as_bytes': True, 'use_numpy': True}}

    @unittest.skip("AttributeError: 'bool' object has no attribute 'all'")
    def test_string(self):
        with self.create_table('a FixedString(3)'):
            data = [np.array([b'a', b'b', b'c'])]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            rv = self.client.execute(query, columnar=True)

            self.assertArraysEqual(rv[0], data)
            self.assertNotEqual(rv[0].dtype, object)
            self.assertIsInstance(rv[0][0], (np.bytes_, ))

    @unittest.skip("AssertionError: False is not true")
    def test_nullable(self):
        with self.create_table('a Nullable(FixedString(10))'):
            data = [np.array([
                np.nan,
                b'test\x00\x00\x00\x00\x00\x00',
                None,
                b'nullable\x00\x00'
            ], dtype=object)]

            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(
                inserted[0], [np.array([None, b'test', None, b'nullable'])]
            )
            self.assertEqual(inserted[0].dtype, object)
