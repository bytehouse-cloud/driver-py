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


class IntervalTestCase(BaseTestCase):
    def test_all(self):
        interval = [
            ('YEAR', 1),
            ('MONTH', 2),
            ('WEEK', 3),
            ('DAY', 4),
            ('HOUR', 5),
            ('MINUTE', 6),
            ('SECOND', 7)
        ]
        columns = ', '.join(['INTERVAL {} {}'.format(v, k)
                             for k, v in interval])
        query = 'SELECT {}'.format(columns)

        client_result = self.client.execute(query)
        self.assertEqual(client_result, [(1, 2, 3, 4, 5, 6, 7)])
