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


class UUIDTestCase(BaseTestCase):
    def test_simple(self):
        with self.create_table('a UUID'):
            data = [
                (UUID('c0fcbba9-0752-44ed-a5d6-4dfb4342b89d'), ),
                ('2efcead4-ff55-4db5-bdb4-6b36a308d8e0', )
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (UUID('c0fcbba9-0752-44ed-a5d6-4dfb4342b89d'), ),
                (UUID('2efcead4-ff55-4db5-bdb4-6b36a308d8e0'), )
            ])

    def test_type_mismatch(self):
        data = [(62457709573696417404743346296141175008, )]
        with self.create_table('a UUID'):
            with self.assertRaises(errors.TypeMismatchError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data, types_check=True
                )
            with self.assertRaises(AttributeError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

    def test_bad_uuid(self):
        data = [('a', )]
        with self.create_table('a UUID'):
            with self.assertRaises(errors.CannotParseUuidError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

    def test_nullable(self):
        with self.create_table('a Nullable(UUID)'):
            data = [(UUID('2efcead4-ff55-4db5-bdb4-6b36a308d8e0'), ), (None, )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
