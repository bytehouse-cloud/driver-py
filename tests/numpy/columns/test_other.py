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

from parameterized import parameterized

from bytehouse_driver import errors
from bytehouse_driver.columns.service import get_column_by_spec
from bytehouse_driver.context import Context

from tests.numpy.testcase import NumpyBaseTestCase


class OtherColumnsTestCase(NumpyBaseTestCase):
    def get_column(self, spec):
        ctx = Context()
        ctx.client_settings = {'strings_as_bytes': False, 'use_numpy': True}
        return get_column_by_spec(spec, {'context': ctx})

    @parameterized.expand([
        ("Enum8('hello' = 1, 'world' = 2)", ),
        ('Decimal(8, 4)', ),
        ('Array(String)', ),
        ('Tuple(String)', ),
        ('SimpleAggregateFunction(any, Int32)', ),
        ('Map(String, String)', ),
        ('Array(LowCardinality(String))', )
    ])
    def test_generic_type(self, spec):
        col = self.get_column(spec)
        self.assertIsNotNone(col)

    def test_get_unknown_column(self):
        with self.assertRaises(errors.UnknownTypeError) as e:
            self.get_column('Unicorn')

        self.assertIn('Unicorn', str(e.exception))
