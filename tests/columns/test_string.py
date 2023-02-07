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

# coding=utf-8
from __future__ import unicode_literals

from tests.testcase import BaseTestCase
from bytehouse_driver import errors


class StringTestCase(BaseTestCase):
    def test_unicode(self):
        data = [('яндекс', )]
        with self.create_table('a String'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_non_utf(self):
        data = [('яндекс'.encode('koi8-r'), )]
        with self.create_table('a String'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_null_byte_in_the_middle(self):
        data = [('a\x00b', )]
        with self.create_table('a String'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nullable(self):
        with self.create_table('a Nullable(String)'):
            data = [(None, ), ('test', ), (None, ), ('nullable', )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_buffer_reader(self):
        data = [('a' * 300, )] * 300
        with self.create_table('a String'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_compressed_client(self):
        with self.created_client(compression=True) as client:
            data = [('a' * 300, )]
            with self.create_table('a String'):
                client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

                query = 'SELECT * FROM test'

                inserted = client.execute(query)
                self.assertEqual(inserted, data)

    def test_custom_encoding(self):
        settings = {'strings_encoding': 'cp1251'}

        data = [(('яндекс'), ), (('test'), )]
        with self.create_table('a String'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, settings=settings
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query, settings=settings)
            self.assertEqual(inserted, data)
            self.assertIsInstance(inserted[0][0], str)
            self.assertIsInstance(inserted[1][0], str)

    def test_not_supported_types(self):
        datas = [
            [(bytearray(b'asd'), )],
            [(123, )]
        ]
        with self.create_table('a String'):
            for data in datas:
                with self.assertRaises(errors.TypeMismatchError) as e:
                    self.client.execute(
                        'INSERT INTO test (a) VALUES', data,
                        types_check=True
                    )

                self.assertIn('for column "a"', str(e.exception))

                with self.assertRaises(AttributeError):
                    self.client.execute(
                        'INSERT INTO test (a) VALUES', data
                    )


class ByteStringTestCase(BaseTestCase):
    client_kwargs = {'settings': {'strings_as_bytes': True}}

    def test_not_decoded(self):
        data = [
            (bytes('яндекс'.encode('cp1251')), ),
            (bytes('test'.encode('cp1251')), ),
        ]
        with self.create_table('a String'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
            self.assertIsInstance(inserted[0][0], bytes)
            self.assertIsInstance(inserted[1][0], bytes)

    def test_nullable(self):
        with self.create_table('a Nullable(String)'):
            data = [(None, ), (b'test', ), (None, ), (b'nullable', )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_not_supported_types(self):
        datas = [
            [('asd', )],
            [(bytearray(b'asd'), )],
            [(123, )]
        ]
        with self.create_table('a String'):
            for data in datas:
                with self.assertRaises(errors.TypeMismatchError) as e:
                    self.client.execute(
                        'INSERT INTO test (a) VALUES', data,
                        types_check=True
                    )

                self.assertIn('for column "a"', str(e.exception))

                with self.assertRaises(ValueError) as e:
                    self.client.execute(
                        'INSERT INTO test (a) VALUES', data
                    )

                self.assertIn('bytes object expected', str(e.exception))
