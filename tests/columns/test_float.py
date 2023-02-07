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

import math

from tests.testcase import BaseTestCase
from bytehouse_driver import errors


class FloatTestCase(BaseTestCase):
    def test_chop_to_type(self):
        with self.create_table('a Float32, b Float64'):
            data = [
                (3.4028235e38, 3.4028235e38),
                (3.4028235e39, 3.4028235e39),
                (-3.4028235e39, 3.4028235e39),
                (1, 2)
            ]

            with self.assertRaises(errors.TypeMismatchError) as e:
                self.client.execute(
                    'INSERT INTO test (a, b) VALUES', data
                )

            self.assertIn('Column a', str(e.exception))

    def test_simple(self):
        with self.create_table('a Float32, b Float64'):
            data = [
                (3.4028235e38, 3.4028235e38),
                (3.4028235e39, 3.4028235e39),
                (-3.4028235e39, 3.4028235e39),
                (1, 2)
            ]
            self.client.execute(
                'INSERT INTO test (a, b) VALUES', data, types_check=True
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (3.4028234663852886e+38, 3.4028235e38),
                (float('inf'), 3.4028235e39),
                (-float('inf'), 3.4028235e39),
                (1, 2)
            ])

    def test_nullable(self):
        with self.create_table('a Nullable(Float32)'):
            data = [(None, ), (0.5, ), (None, ), (1.5, )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nan(self):
        with self.create_table('a Float32'):
            data = [(float('nan'), ), (0.5, )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(len(inserted), 2)
            self.assertTrue(math.isnan(inserted[0][0]))
            self.assertEqual(inserted[1][0], 0.5)
