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
from tests.util import capture_logging


class OpenTelemetryTestCase(BaseTestCase):
    required_server_version = (20, 11, 2)

    @unittest.skip("Assertion error: 'OpenTelemetry' not found")
    def test_server_logs(self):
        tracestate = 'tracestate'
        traceparent = '00-1af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'

        settings = {
            'opentelemetry_tracestate': tracestate,
            'opentelemetry_traceparent': traceparent

        }
        with self.created_client(settings=settings) as client:
            with capture_logging('bytehouse_driver.log', 'INFO') as buffer:
                settings = {'send_logs_level': 'trace'}
                query = 'SELECT 1'
                client.execute(query, settings=settings)
                value = buffer.getvalue()
                self.assertIn('OpenTelemetry', value)

                # ByteHouse 22.2+ use big-endian:
                # https://github.com/ByteHouse/ByteHouse/pull/33723
                if self.server_version >= (22, 2):
                    tp = '8448eb211c80319c1af7651916cd43dd'
                else:
                    tp = '1af7651916cd43dd8448eb211c80319c'
                self.assertIn(tp, value)

    @unittest.skip("Assertion error: 'OpenTelemetry' not found")
    def test_no_tracestate(self):
        traceparent = '00-1af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'

        settings = {
            'opentelemetry_traceparent': traceparent

        }
        with self.created_client(settings=settings) as client:
            with capture_logging('bytehouse_driver.log', 'INFO') as buffer:
                settings = {'send_logs_level': 'trace'}
                query = 'SELECT 1'
                client.execute(query, settings=settings)
                value = buffer.getvalue()
                self.assertIn('OpenTelemetry', value)
                # ByteHouse 22.2+ use big-endian:
                # https://github.com/ByteHouse/ByteHouse/pull/33723
                if self.server_version >= (22, 2):
                    tp = '8448eb211c80319c1af7651916cd43dd'
                else:
                    tp = '1af7651916cd43dd8448eb211c80319c'
                self.assertIn(tp, value)

    def test_bad_traceparent(self):
        settings = {'opentelemetry_traceparent': 'bad'}
        with self.created_client(settings=settings) as client:
            with self.assertRaises(ValueError) as e:
                client.execute('SELECT 1')

            self.assertEqual(
                str(e.exception),
                'unexpected length 3, expected 55'
            )

        traceparent = '00-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-yyyyyyyyyyyyyyyy-01'
        settings = {'opentelemetry_traceparent': traceparent}
        with self.created_client(settings=settings) as client:
            with self.assertRaises(ValueError) as e:
                client.execute('SELECT 1')

            self.assertEqual(
                str(e.exception),
                'Malformed traceparant header: {}'.format(traceparent)
            )

    def test_bad_traceparent_version(self):
        settings = {
            'opentelemetry_traceparent':
                '01-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01'
        }
        with self.created_client(settings=settings) as client:
            with self.assertRaises(ValueError) as e:
                client.execute('SELECT 1')

            self.assertEqual(
                str(e.exception),
                'unexpected version 01, expected 00'
            )
