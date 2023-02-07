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

from __future__ import unicode_literals

from bytehouse_driver import errors
from ipaddress import IPv6Address, IPv4Address

from tests.testcase import BaseTestCase


class IPv4TestCase(BaseTestCase):
    def test_simple(self):
        with self.create_table('a IPv4'):
            data = [
                (IPv4Address("10.0.0.1"),),
                (IPv4Address("192.168.253.42"),)
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (IPv4Address("10.0.0.1"),),
                (IPv4Address("192.168.253.42"),)
            ])

    def test_from_int(self):
        with self.create_table('a IPv4'):
            data = [
                (167772161,),
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, types_check=True
            )

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (IPv4Address("10.0.0.1"),),
            ])

    def test_from_str(self):
        with self.create_table('a IPv4'):
            data = [
                ("10.0.0.1",),
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, types_check=True
            )

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (IPv4Address("10.0.0.1"),),
            ])

    def test_type_mismatch(self):
        data = [(1025.2147,)]
        with self.create_table('a IPv4'):
            with self.assertRaises(errors.TypeMismatchError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data, types_check=True
                )

    def test_bad_ipv4(self):
        data = [('985.512.12.0',)]
        with self.create_table('a IPv4'):
            with self.assertRaises(errors.CannotParseDomainError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

    def test_bad_ipv4_with_type_check(self):
        data = [('985.512.12.0',)]
        with self.create_table('a IPv4'):
            with self.assertRaises(errors.TypeMismatchError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data, types_check=True
                )

    def test_nullable(self):
        with self.create_table('a Nullable(IPv4)'):
            data = [(IPv4Address('10.10.10.10'),), (None,)]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)


class IPv6TestCase(BaseTestCase):
    required_server_version = (19, 3, 3)

    def test_simple(self):
        with self.create_table('a IPv6'):
            data = [
                (IPv6Address('79f4:e698:45de:a59b:2765:28e3:8d3a:35ae'),),
                (IPv6Address('a22:cc64:cf47:1653:4976:3c0c:ff8d:417c'),),
                (IPv6Address('12ff:0000:0000:0000:0000:0000:0000:0001'),)
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (IPv6Address('79f4:e698:45de:a59b:2765:28e3:8d3a:35ae'),),
                (IPv6Address('a22:cc64:cf47:1653:4976:3c0c:ff8d:417c'),),
                (IPv6Address('12ff::1'),)
            ])

    def test_from_str(self):
        with self.create_table('a IPv6'):
            data = [
                ('79f4:e698:45de:a59b:2765:28e3:8d3a:35ae',),
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, types_check=True
            )

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (IPv6Address('79f4:e698:45de:a59b:2765:28e3:8d3a:35ae'),),
            ])

    def test_from_bytes(self):
        with self.create_table('a IPv6'):
            data = [
                (b"y\xf4\xe6\x98E\xde\xa5\x9b'e(\xe3\x8d:5\xae",),
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, types_check=True
            )

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (IPv6Address('79f4:e698:45de:a59b:2765:28e3:8d3a:35ae'),),
            ])

    def test_type_mismatch(self):
        data = [(1025.2147,)]
        with self.create_table('a IPv6'):
            with self.assertRaises(errors.TypeMismatchError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data, types_check=True
                )

    def test_bad_ipv6(self):
        data = [("ghjk:e698:45de:a59b:2765:28e3:8d3a:zzzz",)]
        with self.create_table('a IPv6'):
            with self.assertRaises(errors.CannotParseDomainError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

    def test_bad_ipv6_with_type_check(self):
        data = [("ghjk:e698:45de:a59b:2765:28e3:8d3a:zzzz",)]
        with self.create_table('a IPv6'):
            with self.assertRaises(errors.TypeMismatchError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data, types_check=True
                )

    def test_nullable(self):
        with self.create_table('a Nullable(IPv6)'):
            data = [
                (IPv6Address('79f4:e698:45de:a59b:2765:28e3:8d3a:35ae'),),
                (None,)]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
