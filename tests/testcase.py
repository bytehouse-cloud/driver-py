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

import configparser
from contextlib import contextmanager
from unittest import TestCase

from bytehouse_driver.client import Client
from tests import log

file_config = configparser.ConfigParser()
file_config.read(['setup.cfg'])

log.configure(file_config.get('log', 'level'))


class BaseTestCase(TestCase):
    host = file_config.get('db', 'host')
    port = file_config.getint('db', 'port')
    database = file_config.get('db', 'database')
    account = file_config.get('db', 'account')
    user = file_config.get('db', 'user')
    password = file_config.get('db', 'password')
    api_key = file_config.get('db', 'api_key')
    region = file_config.get('db', 'region')

    additional_settings = None
    client = None
    client_kwargs = None

    def _create_client(self, **kwargs):
        client_kwargs = {
            'port': self.port,
            'database': self.database,
            'account': self.account,
            'user': self.user,
            'password': self.password,
            'secure': True
        }
        client_kwargs.update(kwargs)
        if self.additional_settings:
            client_kwargs.update(self.additional_settings)
        return Client(self.host, **client_kwargs)

    def created_client(self, **kwargs):
        return self._create_client(**kwargs)

    @classmethod
    def setUpClass(cls):
        setup_client = Client(
            host=cls.host,
            port=cls.port,
            account=cls.account,
            user=cls.user,
            password=cls.password
        )
        setup_client.execute(
            'DROP DATABASE IF EXISTS {}'.format(cls.database)
        )
        setup_client.execute('CREATE DATABASE {}'.format(cls.database))

        super(BaseTestCase, cls).setUpClass()

    def setUp(self):
        super(BaseTestCase, self).setUp()

        client_kwargs = self.client_kwargs or {}
        self.client = self._create_client(**client_kwargs)

    def tearDown(self):
        self.client.disconnect()
        super(BaseTestCase, self).tearDown()

    @classmethod
    def tearDownClass(cls):
        setup_client = Client(
            host=cls.host,
            port=cls.port,
            account=cls.account,
            user=cls.user,
            password=cls.password,
            database=cls.database
        )
        setup_client.execute('DROP DATABASE {}'.format(cls.database))
        super(BaseTestCase, cls).tearDownClass()

    @contextmanager
    def create_table(self, columns, **kwargs):
        self.client.execute(
            'CREATE TABLE test ({}) ''ENGINE = CnchMergeTree() Order by tuple()'.format(columns),
            **kwargs
        )
        try:
            yield
        except Exception:
            raise
        finally:
            self.client.execute('DROP TABLE test')
