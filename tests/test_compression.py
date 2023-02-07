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
from datetime import date, datetime
from unittest import TestCase

from bytehouse_driver import errors
from bytehouse_driver.client import Client
from bytehouse_driver.compression import get_compressor_cls
from bytehouse_driver.compression.lz4 import Compressor
from .testcase import BaseTestCase, file_config


class BaseCompressionTestCase(BaseTestCase):
    compression = False
    supported_compressions = file_config.get('db', 'compression').split(',')

    def _create_client(self):
        settings = None
        if self.compression:
            # Set server compression method explicitly
            # By default server sends blocks compressed by LZ4.
            method = self.compression
            settings = {'network_compression_method': method}

        return Client(
            self.host, self.port, self.database, self.user, self.password,
            compression=self.compression, settings=settings
        )

    def setUp(self):
        super(BaseCompressionTestCase, self).setUp()
        supported = (
                self.compression is False or
                self.compression in self.supported_compressions
        )

        if not supported:
            self.skipTest(
                'Compression {} is not supported'.format(self.compression)
            )

    def run_simple(self):
        with self.create_table('a Date, b DateTime'):
            data = [(date(2012, 10, 25), datetime(2012, 10, 25, 14, 7, 19))]
            self.client.execute(
                'INSERT INTO test (a, b) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test(self):
        if self.compression is False:
            return

        self.run_simple()


@unittest.skip("EOFError: Unexpected EOF while reading bytes")
class LZ4ReadWriteTestCase(BaseCompressionTestCase):
    compression = 'lz4'


@unittest.skip("EOFError: Unexpected EOF while reading bytes")
class LZ4HCReadWriteTestCase(BaseCompressionTestCase):
    compression = 'lz4hc'


@unittest.skip("EOFError: Unexpected EOF while reading bytes")
class ZSTDReadWriteTestCase(BaseCompressionTestCase):
    compression = 'zstd'


class MiscCompressionTestCase(TestCase):
    def test_default_compression(self):
        client = Client('localhost', compression=True)
        self.assertEqual(client.connection.compressor_cls, Compressor)

    def test_unknown_compressor(self):
        with self.assertRaises(errors.UnknownCompressionMethod) as e:
            get_compressor_cls('hello')

        self.assertEqual(
            e.exception.code, errors.ErrorCodes.UNKNOWN_COMPRESSION_METHOD
        )


@unittest.skip("EOFError: Unexpected EOF while reading bytes")
class ReadByBlocksTestCase(BaseCompressionTestCase):
    compression = 'lz4'

    def test(self):
        with self.create_table('a Int32'):
            data = [(x % 200,) for x in range(1000000)]

            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
