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
from tests.testcase import BaseTestCase


class ExternalTablesTestCase(BaseTestCase):
    @unittest.skip("no database table pair provided")
    def test_select(self):
        tables = [{
            'name': 'test',
            'structure': [('x', 'Int32'), ('y', 'Array(Int32)')],
            'data': [
                {'x': 100, 'y': [2, 4, 6, 8]},
                {'x': 500, 'y': [1, 3, 5, 7]},
            ]
        }]
        rv = self.client.execute('SELECT * FROM test', external_tables=tables)
        self.assertEqual(rv, [(100, [2, 4, 6, 8]), (500, [1, 3, 5, 7])])

    @unittest.skip("no database table pair provided")
    def test_send_empty_table(self):
        tables = [{
            'name': 'test',
            'structure': [('x', 'Int32')],
            'data': []
        }]
        rv = self.client.execute('SELECT * FROM test', external_tables=tables)
        self.assertEqual(rv, [])

    def test_send_empty_table_structure(self):
        tables = [{
            'name': 'test',
            'structure': [],
            'data': []
        }]
        with self.assertRaises(ValueError) as e:
            self.client.execute('SELECT * FROM test', external_tables=tables)

        self.assertIn('Empty table "test" structure', str(e.exception))
