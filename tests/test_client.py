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

import ssl
import unittest

from bytehouse_driver import Client
from bytehouse_driver.compression.lz4 import Compressor as LZ4Compressor
from bytehouse_driver.compression.lz4hc import Compressor as LZHC4Compressor
from bytehouse_driver.compression.zstd import Compressor as ZSTDCompressor
from bytehouse_driver.protocol import Compression
from tests.numpy.util import check_numpy
from tests.testcase import TestCase, BaseTestCase


class ClientFromUrlTestCase(TestCase):
    def assertHostsEqual(self, client, another, msg=None):
        self.assertEqual(list(client.connection.hosts), another, msg=msg)

    @unittest.skip("default secure=true breaking this case")
    def test_simple(self):
        c = Client.from_url('bytehouse://host')

        self.assertHostsEqual(c, [('host', 9000)])
        self.assertEqual(c.connection.database, '')

        c = Client.from_url('bytehouse://host/db')

        self.assertHostsEqual(c, [('host', 9000)])
        self.assertEqual(c.connection.database, 'db')

    def test_credentials(self):
        c = Client.from_url('bytehouse://host/db')

        self.assertEqual(c.connection.user, 'default')
        self.assertEqual(c.connection.password, '')

        c = Client.from_url('bytehouse://admin:secure@host/db')

        self.assertEqual(c.connection.user, 'admin')
        self.assertEqual(c.connection.password, 'secure')

        c = Client.from_url('bytehouse://user:@host/db')

        self.assertEqual(c.connection.user, 'user')
        self.assertEqual(c.connection.password, '')

    def test_credentials_unquoting(self):
        c = Client.from_url('bytehouse://ad%3Amin:se%2Fcure@host/db')

        self.assertEqual(c.connection.user, 'ad:min')
        self.assertEqual(c.connection.password, 'se/cure')

    @unittest.skip("default secure=true breaking this case")
    def test_schema(self):
        c = Client.from_url('bytehouse://host')
        self.assertFalse(c.connection.secure_socket)

        c = Client.from_url('bytehouses://host')
        self.assertTrue(c.connection.secure_socket)

        c = Client.from_url('test://host')
        self.assertFalse(c.connection.secure_socket)

    @unittest.skip("default secure=true breaking this case")
    def test_port(self):
        c = Client.from_url('bytehouse://host')
        self.assertHostsEqual(c, [('host', 9000)])

        c = Client.from_url('bytehouses://host')
        self.assertHostsEqual(c, [('host', 9440)])

        c = Client.from_url('bytehouses://host:1234')
        self.assertHostsEqual(c, [('host', 1234)])

    def test_secure(self):
        c = Client.from_url('bytehouse://host?secure=n')
        self.assertHostsEqual(c, [('host', 9000)])
        self.assertFalse(c.connection.secure_socket)

        c = Client.from_url('bytehouse://host?secure=y')
        self.assertHostsEqual(c, [('host', 9440)])
        self.assertTrue(c.connection.secure_socket)

        c = Client.from_url('bytehouse://host:1234?secure=y')
        self.assertHostsEqual(c, [('host', 1234)])
        self.assertTrue(c.connection.secure_socket)

        with self.assertRaises(ValueError):
            Client.from_url('bytehouse://host:1234?secure=nonono')

    def test_compression(self):
        c = Client.from_url('bytehouse://host?compression=n')
        self.assertEqual(c.connection.compression, Compression.DISABLED)
        self.assertIsNone(c.connection.compressor_cls)

        c = Client.from_url('bytehouse://host?compression=y')
        self.assertEqual(c.connection.compression, Compression.ENABLED)
        self.assertIs(c.connection.compressor_cls, LZ4Compressor)

        c = Client.from_url('bytehouse://host?compression=lz4')
        self.assertEqual(c.connection.compression, Compression.ENABLED)
        self.assertIs(c.connection.compressor_cls, LZ4Compressor)

        c = Client.from_url('bytehouse://host?compression=lz4hc')
        self.assertEqual(c.connection.compression, Compression.ENABLED)
        self.assertIs(c.connection.compressor_cls, LZHC4Compressor)

        c = Client.from_url('bytehouse://host?compression=zstd')
        self.assertEqual(c.connection.compression, Compression.ENABLED)
        self.assertIs(c.connection.compressor_cls, ZSTDCompressor)

        with self.assertRaises(ValueError):
            Client.from_url('bytehouse://host:1234?compression=custom')

    def test_client_name(self):
        c = Client.from_url('bytehouse://host?client_name=native')
        self.assertEqual(c.connection.client_name, 'ByteHouse native')

    def test_timeouts(self):
        with self.assertRaises(ValueError):
            Client.from_url('bytehouse://host?connect_timeout=test')

        c = Client.from_url('bytehouse://host?connect_timeout=1.2')
        self.assertEqual(c.connection.connect_timeout, 1.2)

        c = Client.from_url('bytehouse://host?send_receive_timeout=1.2')
        self.assertEqual(c.connection.send_receive_timeout, 1.2)

        c = Client.from_url('bytehouse://host?sync_request_timeout=1.2')
        self.assertEqual(c.connection.sync_request_timeout, 1.2)

    def test_compress_block_size(self):
        with self.assertRaises(ValueError):
            Client.from_url('bytehouse://host?compress_block_size=test')

        c = Client.from_url('bytehouse://host?compress_block_size=100500')
        # compression is not set
        self.assertIsNone(c.connection.compress_block_size)

        c = Client.from_url(
            'bytehouse://host?'
            'compress_block_size=100500&'
            'compression=1'
        )
        self.assertEqual(c.connection.compress_block_size, 100500)

    def test_settings(self):
        c = Client.from_url(
            'bytehouse://host?'
            'send_logs_level=trace&'
            'max_block_size=123'
        )
        self.assertEqual(c.settings, {
            'send_logs_level': 'trace',
            'max_block_size': '123'
        })

    def test_ssl(self):
        c = Client.from_url(
            'bytehouses://host?'
            'verify=false&'
            'ssl_version=PROTOCOL_SSLv23&'
            'ca_certs=/tmp/certs&'
            'ciphers=HIGH:-aNULL:-eNULL:-PSK:RC4-SHA:RC4-MD5'
        )
        self.assertEqual(c.connection.ssl_options, {
            'ssl_version': ssl.PROTOCOL_SSLv23,
            'ca_certs': '/tmp/certs',
            'ciphers': 'HIGH:-aNULL:-eNULL:-PSK:RC4-SHA:RC4-MD5'
        })

    def test_ssl_key_cert(self):
        base_url = (
            'bytehouses://host?'
            'verify=true&'
            'ssl_version=PROTOCOL_SSLv23&'
            'ca_certs=/tmp/certs&'
            'ciphers=HIGH:-aNULL:-eNULL:-PSK:RC4-SHA:RC4-MD5&'
        )
        base_expected = {
            'ssl_version': ssl.PROTOCOL_SSLv23,
            'ca_certs': '/tmp/certs',
            'ciphers': 'HIGH:-aNULL:-eNULL:-PSK:RC4-SHA:RC4-MD5'
        }

        c = Client.from_url(
            base_url +
            'keyfile=/tmp/client.key&'
            'certfile=/tmp/client.cert'
        )
        expected = base_expected.copy()
        expected.update({
            'keyfile': '/tmp/client.key',
            'certfile': '/tmp/client.cert'
        })
        self.assertEqual(c.connection.ssl_options, expected)

        c = Client.from_url(
            base_url +
            'certfile=/tmp/client.cert'
        )
        expected = base_expected.copy()
        expected.update({
            'certfile': '/tmp/client.cert'
        })
        self.assertEqual(c.connection.ssl_options, expected)

    @unittest.skip("default secure=true breaking this case")
    def test_alt_hosts(self):
        c = Client.from_url('bytehouse://host?alt_hosts=host2:1234')
        self.assertHostsEqual(c, [('host', 9000), ('host2', 1234)])

        c = Client.from_url('bytehouse://host?alt_hosts=host2')
        self.assertHostsEqual(c, [('host', 9000), ('host2', 9000)])

    def test_parameters_cast(self):
        c = Client.from_url('bytehouse://host?insert_block_size=123')
        self.assertEqual(
            c.connection.context.client_settings['insert_block_size'], 123
        )

    def test_settings_is_important(self):
        c = Client.from_url('bytehouse://host?settings_is_important=1')
        self.assertEqual(c.connection.settings_is_important, True)

        with self.assertRaises(ValueError):
            c = Client.from_url('bytehouse://host?settings_is_important=2')
            self.assertEqual(c.connection.settings_is_important, True)

        c = Client.from_url('bytehouse://host?settings_is_important=0')
        self.assertEqual(c.connection.settings_is_important, False)

    @check_numpy
    def test_use_numpy(self):
        c = Client.from_url('bytehouse://host?use_numpy=true')
        self.assertTrue(c.connection.context.client_settings['use_numpy'])

    def test_opentelemetry(self):
        c = Client.from_url(
            'bytehouse://host?opentelemetry_traceparent='
            '00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-00'
        )
        self.assertEqual(
            c.connection.context.client_settings['opentelemetry_traceparent'],
            '00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-00'
        )
        self.assertEqual(
            c.connection.context.client_settings['opentelemetry_tracestate'],
            ''
        )

        c = Client.from_url(
            'bytehouse://host?opentelemetry_traceparent='
            '00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-00&'
            'opentelemetry_tracestate=state'
        )
        self.assertEqual(
            c.connection.context.client_settings['opentelemetry_traceparent'],
            '00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-00'
        )
        self.assertEqual(
            c.connection.context.client_settings['opentelemetry_tracestate'],
            'state'
        )

    def test_quota_key(self):
        c = Client.from_url('bytehouse://host?quota_key=myquota')
        self.assertEqual(
            c.connection.context.client_settings['quota_key'], 'myquota'
        )

        c = Client.from_url('bytehouse://host')
        self.assertEqual(
            c.connection.context.client_settings['quota_key'], ''
        )

    def test_round_robin(self):
        c = Client.from_url('bytehouse://host?alt_hosts=host2')
        self.assertEqual(len(c.connections), 0)

        c = Client.from_url(
            'bytehouse://host?round_robin=true&alt_hosts=host2'
        )
        self.assertEqual(len(c.connections), 1)


class ByteHouseURITestCase(BaseTestCase):
    def test_bytehouse_uri_password(self):
        account_url_without_database = 'bytehouse://{}:{}/?account={}&user={}&password={}'.format(
            self.host, self.port, self.account, self.user, self.password
        )
        account_url_with_database = 'bytehouse://{}:{}/{}?account={}&user={}&password={}'.format(
            self.host, self.port, self.database, self.account, self.user, self.password
        )
        account_url_with_database_key = 'bytehouse://{}:{}/?account={}&user={}&password={}&database={}'.format(
            self.host, self.port, self.account, self.user, self.password, self.database
        )

        urls = [
            account_url_without_database,
            account_url_with_database,
            account_url_with_database_key,
        ]

        for url in urls:
            c = Client.from_url(url)
            result = c.execute("SELECT 1")
            self.assertEqual(result, [(1,)])

    def test_bytehouse_uri_api_key(self):
        url_with_api_key = 'bytehouse://{}:{}/?user=bytehouse&password={}'.format(
            self.host, self.port, self.api_key
        )
        url_with_api_key_prefix = 'bytehouse://bytehouse:{}@{}:{}/'.format(
            self.api_key, self.host, self.port
        )

        urls = [
            url_with_api_key,
            url_with_api_key_prefix
        ]

        for url in urls:
            c = Client.from_url(url)
            result = c.execute("SELECT 1")
            self.assertEqual(result, [(1,)])

    def test_bytehouse_uri_region(self):
        url_with_region_account_password = 'bytehouse:///?region={}&account={}&user={}&password={}'.format(
            self.region, self.account, self.user, self.password
        )
        url_with_region_api_key = 'bytehouse:///?region={}&user=bytehouse&password={}'.format(
            self.region, self.api_key
        )

        urls = [
            url_with_region_account_password,
            url_with_region_api_key,
        ]

        for url in urls:
            c = Client.from_url(url)
            result = c.execute("SELECT 1")
            self.assertEqual(result, [(1,)])

    def test_bytehouse_client_region(self):
        c = Client(
            region=self.region,
            user="bytehouse",
            password=self.api_key
        )
        result = c.execute("SELECT 1")
        self.assertEqual(result, [(1,)])

        c = Client(
            region=self.region,
            account=self.account,
            user=self.user,
            password=self.password
        )
        result = c.execute("SELECT 1")
        self.assertEqual(result, [(1,)])
