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

from .connection import Connection
from .errors import (
    Warning, Error, DataError, DatabaseError, ProgrammingError, IntegrityError,
    InterfaceError, InternalError, NotSupportedError, OperationalError
)
from .. import defines

apilevel = '2.0'

threadsafety = 2

paramstyle = 'pyformat'


def connect(dsn=None, host=None,
            user=defines.DEFAULT_USER, password=defines.DEFAULT_PASSWORD,
            port=defines.DEFAULT_PORT, database=defines.DEFAULT_DATABASE,
            **kwargs):
    """
    Create a new database connection.

    The connection can be specified via DSN:

        ``conn = connect("bytehouse://localhost/test?param1=value1&...")``

    or using database and credentials arguments:

        ``conn = connect(database="test", user="default", password="default",
        host="localhost", **kwargs)``

    The basic connection parameters are:

    - *host*: host with running ByteHouse server.
    - *port*: port ByteHouse server is bound to.
    - *database*: database connect to.
    - *user*: database user.
    - *password*: user's password.

    See defaults in :data:`~bytehouse_driver.connection.Connection`
    constructor.

    DSN or host is required.

    Any other keyword parameter will be passed to the underlying Connection
    class.

    :return: a new connection.
    """
    if 'region' in kwargs:
        host, port = defines.HostPortByRegion[kwargs['region']]
        kwargs.pop('region')

    if 'account' in kwargs and 'user' in kwargs:
        user = '{}::{}'.format(kwargs['account'], kwargs['user'])
        kwargs.pop('account')

    if dsn is None and host is None:
        raise ValueError('host or dsn is required')

    return Connection(dsn=dsn, user=user, password=password, host=host,
                      port=port, database=database, **kwargs)


__all__ = [
    'connect',
    'Warning', 'Error', 'DataError', 'DatabaseError', 'ProgrammingError',
    'IntegrityError', 'InterfaceError', 'InternalError', 'NotSupportedError',
    'OperationalError'
]
