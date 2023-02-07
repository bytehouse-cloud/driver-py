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


class MapTestCase(BaseTestCase):
    def test_simple(self):
        with self.create_table('a Map(String, UInt64)'):
            data = [
                ({},),
                ({'key1': 1},),
                ({'key1': 2, 'key2': 20},),
                ({'key1': 3, 'key2': 30, 'key3': 50},)
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)
            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_array(self):
        columns = 'a Map(String, Array(UInt64))'
        with self.create_table(columns):
            data = [
                ({'key1': []},),
                ({'key2': [1, 2, 3]},),
                ({'key3': [1, 1, 1, 1]},)
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)
            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
