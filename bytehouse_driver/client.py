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

import re
import ssl
import string
from collections import deque
from contextlib import contextmanager
from time import time, sleep
import types
from urllib.parse import urlparse, parse_qs, unquote

from . import errors, defines
from .block import ColumnOrientedBlock, RowOrientedBlock
from .connection import Connection
from .log import log_block
from .protocol import ServerPacketTypes
from .result import (
    IterQueryResult, ProgressQueryResult, QueryResult, QueryInfo
)
from .util.escape import escape_params
from .util.helpers import column_chunks, chunks, asbool

import logging

logger = logging.getLogger(__name__)


class Client(object):
    """
    Client for communication with the ByteHouse server.
    Single connection is established per each connected instance of the client.

    :param settings: Dictionary of settings that passed to every query (except
                     for the client settings, see below). Defaults to ``None``
                     (no additional settings).
    :param \\**kwargs: All other args are passed to the
                       :py:class:`~bytehouse_driver.connection.Connection`
                       constructor.

    The following keys when passed in ``settings`` are used for configuring the
    client itself:

        * ``insert_block_size`` -- chunk size to split rows for ``INSERT``.
          Defaults to ``1048576``.
        * ``strings_as_bytes`` -- turns off string column encoding/decoding.
        * ``strings_encoding`` -- specifies string encoding. UTF-8 by default.
        * ``use_numpy`` -- Use NumPy for columns reading.
        * ``opentelemetry_traceparent`` -- OpenTelemetry traceparent header as
                           described by W3C Trace Context recommendation.
        * ``opentelemetry_tracestate`` -- OpenTelemetry tracestate header as
                           described by W3C Trace Context recommendation.
        * ``quota_key`` -- A string to differentiate quotas when the user have
                           keyed quotas configured on server.
        * ``input_format_null_as_default`` -- Initialize null fields with
                           default values if data type of this field is not
                           nullable. Does not work for NumPy. Default: False.
        * ``round_robin`` -- If ``alt_hosts`` are provided the query will be
                           executed on host picked with round-robin algorithm.
    """

    available_client_settings = (
        'insert_block_size',  # TODO: rename to max_insert_block_size
        'strings_as_bytes',
        'strings_encoding',
        'use_numpy',
        'opentelemetry_traceparent',
        'opentelemetry_tracestate',
        'quota_key',
        'input_format_null_as_default'
    )

    def __init__(self, *args, **kwargs):
        self.settings = (kwargs.pop('settings', None) or {}).copy()

        # Default 'secure' = True
        if 'secure' not in kwargs:
            kwargs['secure'] = True

        if 'account' in kwargs and kwargs['account'] is None:
            kwargs.pop('account')

        if 'region' in kwargs and kwargs['region'] is None:
            kwargs.pop('region')

        if 'host' in kwargs and kwargs['host'] is 'localhost':
            kwargs.pop('host')

        if 'account' in kwargs and 'user' in kwargs:
            kwargs['user'] = '{}::{}'.format(kwargs['account'], kwargs['user'])
            kwargs.pop('account')

        if 'region' in kwargs:
            region_host, region_port = defines.HostPortByRegion[kwargs['region']]
            args = (region_host, )
            kwargs['port'] = region_port
            kwargs.pop('region')

        self.client_settings = {
            'insert_block_size': int(self.settings.pop(
                'insert_block_size', defines.DEFAULT_INSERT_BLOCK_SIZE,
            )),
            'strings_as_bytes': self.settings.pop(
                'strings_as_bytes', False
            ),
            'strings_encoding': self.settings.pop(
                'strings_encoding', defines.STRINGS_ENCODING
            ),
            'use_numpy': self.settings.pop(
                'use_numpy', False
            ),
            'opentelemetry_traceparent': self.settings.pop(
                'opentelemetry_traceparent', None
            ),
            'opentelemetry_tracestate': self.settings.pop(
                'opentelemetry_tracestate', ''
            ),
            'quota_key': self.settings.pop(
                'quota_key', ''
            ),
            'input_format_null_as_default': self.settings.pop(
                'input_format_null_as_default', False
            )
        }

        if self.client_settings['use_numpy']:
            try:
                from .numpy.result import (
                    NumpyIterQueryResult, NumpyProgressQueryResult,
                    NumpyQueryResult
                )
                self.query_result_cls = NumpyQueryResult
                self.iter_query_result_cls = NumpyIterQueryResult
                self.progress_query_result_cls = NumpyProgressQueryResult
            except ImportError:
                raise RuntimeError('Extras for NumPy must be installed')
        else:
            self.query_result_cls = QueryResult
            self.iter_query_result_cls = IterQueryResult
            self.progress_query_result_cls = ProgressQueryResult

        vw = kwargs.pop('vw', None)
        round_robin = kwargs.pop('round_robin', False)
        self.connections = deque([Connection(*args, **kwargs)])

        if round_robin and 'alt_hosts' in kwargs:
            alt_hosts = kwargs.pop('alt_hosts')
            for host in alt_hosts.split(','):
                url = urlparse('bytehouse://' + host)

                connection_kwargs = kwargs.copy()
                if len(args) > 2:
                    # port as positional argument
                    connection_args = (url.hostname, url.port) + args[2:]
                else:
                    # port as keyword argument
                    connection_args = (url.hostname, ) + args[1:]
                    connection_kwargs['port'] = url.port

                connection = Connection(*connection_args, **connection_kwargs)
                self.connections.append(connection)

        self.connection = self.get_connection()
        self.reset_last_query()
        self.set_warehouse(vw)
        super(Client, self).__init__()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def get_connection(self):
        if hasattr(self, 'connection'):
            self.connections.append(self.connection)

        connection = self.connections.popleft()

        connection.context.settings = self.settings
        connection.context.client_settings = self.client_settings
        return connection

    def disconnect(self):
        self.disconnect_connection()
        for connection in self.connections:
            connection.disconnect()

    def disconnect_connection(self):
        """
        Disconnects from the server.
        """
        self.connection.disconnect()
        self.reset_last_query()

    def reset_last_query(self):
        self.last_query = None

    def set_warehouse(self, vw):
        if vw == None:
            default_settings = self.execute("SHOW DEFAULT SETTINGS")
            vw = default_settings[0][4]
            if vw is None or len(vw) < 1:
                raise Exception("No default virtual warehouse selected")
        else:
            if type(vw) != str:
                raise Exception("Name of virtual warehouse should be a string")
            try:
                self.execute("SET WAREHOUSE {}".format(vw))
            except Exception as e:
                logger.warn("Failed to resume warehouse {}".format(vw))
                raise e

        if self.is_warehouse_up(vw):
            return
        logger.info("Resuming warehouse %s", vw)
        try:
            self.execute("RESUME WAREHOUSE {}".format(vw))
        except:
            logger.warn("Error from server while resuming warehouse", vw)

        start_time = time()
        warehouse_started = False
        # allowing 10 seconds to resume the vw
        while (time() - start_time) < 10:
            if self.is_warehouse_up(vw):
                warehouse_started = True
                break
        if not warehouse_started:
            logger.info("Cannot turn on warehouse")

    def is_warehouse_up(self, warehouse_name):
        logger.info("Checking warehouse status %s", warehouse_name)
        warehouses = self.execute("SHOW WAREHOUSES")
        for warehouse in warehouses:
            if warehouse[1] == warehouse_name and warehouse[6] == "up":
                return True
        return False

    def receive_result(self, with_column_types=False, progress=False,
                       columnar=False):

        gen = self.packet_generator()

        if progress:
            return self.progress_query_result_cls(
                gen, with_column_types=with_column_types, columnar=columnar
            )

        else:
            result = self.query_result_cls(
                gen, with_column_types=with_column_types, columnar=columnar
            )
            return result.get_result()

    def iter_receive_result(self, with_column_types=False):
        gen = self.packet_generator()

        result = self.iter_query_result_cls(
            gen, with_column_types=with_column_types
        )

        for rows in result:
            for row in rows:
                yield row

    def packet_generator(self):
        while True:
            try:
                packet = self.receive_packet()
                if not packet:
                    break

                if packet is True:
                    continue

                yield packet

            except (Exception, KeyboardInterrupt):
                self.disconnect()
                raise

    def receive_packet(self):
        packet = self.connection.receive_packet()

        if packet.type == ServerPacketTypes.EXCEPTION:
            raise packet.exception

        elif packet.type == ServerPacketTypes.PROGRESS:
            self.last_query.store_progress(packet.progress)
            return packet

        elif packet.type == ServerPacketTypes.END_OF_STREAM:
            return False

        elif packet.type == ServerPacketTypes.DATA:
            return packet

        elif packet.type == ServerPacketTypes.TOTALS:
            return packet

        elif packet.type == ServerPacketTypes.EXTREMES:
            return packet

        elif packet.type == ServerPacketTypes.PROFILE_INFO:
            self.last_query.store_profile(packet.profile_info)
            return True

        else:
            return True

    def make_query_settings(self, settings):
        settings = dict(settings or {})

        # Pick client-related settings.
        client_settings = self.client_settings.copy()
        for key in self.available_client_settings:
            if key in settings:
                client_settings[key] = settings.pop(key)

        self.connection.context.client_settings = client_settings

        # The rest of settings are ByteHouse-related.
        query_settings = self.settings.copy()
        query_settings.update(settings)
        self.connection.context.settings = query_settings

    def track_current_database(self, query):
        query = query.strip('; ')
        if query.lower().startswith('use '):
            self.connection.database = query[4:].strip()

    def establish_connection(self, settings):
        num_connections = len(self.connections)
        if hasattr(self, 'connection'):
            num_connections += 1

        for i in range(num_connections):
            try:
                self.connection = self.get_connection()
                self.make_query_settings(settings)
                self.connection.force_connect()
                self.last_query = QueryInfo()

            except (errors.SocketTimeoutError, errors.NetworkError):
                if i < num_connections - 1:
                    continue
                raise

            return

    @contextmanager
    def disconnect_on_error(self, query, settings):
        try:
            self.establish_connection(settings)

            yield

            self.track_current_database(query)

        except (Exception, KeyboardInterrupt):
            self.disconnect()
            raise

    def execute(self, query, params=None, with_column_types=False,
                external_tables=None, query_id=None, settings=None,
                types_check=False, columnar=False):
        """
        Executes query.

        Establishes new connection if it wasn't established yet.
        After query execution connection remains intact for next queries.
        If connection can't be reused it will be closed and new connection will
        be created.

        :param query: query that will be send to server.
        :param params: substitution parameters for SELECT queries and data for
                       INSERT queries. Data for INSERT can be `list`, `tuple`
                       or :data:`~types.GeneratorType`.
                       Defaults to ``None`` (no parameters  or data).
        :param with_column_types: if specified column names and types will be
                                  returned alongside with result.
                                  Defaults to ``False``.
        :param external_tables: external tables to send.
                                Defaults to ``None`` (no external tables).
        :param query_id: the query identifier. If no query id specified
                         ByteHouse server will generate it.
        :param settings: dictionary of query settings.
                         Defaults to ``None`` (no additional settings).
        :param types_check: enables type checking of data for INSERT queries.
                            Causes additional overhead. Defaults to ``False``.
        :param columnar: if specified the result of the SELECT query will be
                         returned in column-oriented form.
                         It also allows to INSERT data in columnar form.
                         Defaults to ``False`` (row-like form).

        :return: * number of inserted rows for INSERT queries with data.
                   Returning rows count from INSERT FROM SELECT is not
                   supported.
                 * if `with_column_types=False`: `list` of `tuples` with
                   rows/columns.
                 * if `with_column_types=True`: `tuple` of 2 elements:
                    * The first element is `list` of `tuples` with
                      rows/columns.
                    * The second element information is about columns: names
                      and types.
        """

        start_time = time()

        with self.disconnect_on_error(query, settings):
            # INSERT queries can use list/tuple/generator of list/tuples/dicts.
            # For SELECT parameters can be passed in only in dict right now.
            is_insert = isinstance(params, (list, tuple, types.GeneratorType))

            if is_insert:
                rv = self.process_insert_query(
                    query, params, external_tables=external_tables,
                    query_id=query_id, types_check=types_check,
                    columnar=columnar
                )
            else:
                rv = self.process_ordinary_query(
                    query, params=params, with_column_types=with_column_types,
                    external_tables=external_tables,
                    query_id=query_id, types_check=types_check,
                    columnar=columnar
                )
            self.last_query.store_elapsed(time() - start_time)
            return rv

    def execute_with_progress(
            self, query, params=None, with_column_types=False,
            external_tables=None, query_id=None, settings=None,
            types_check=False, columnar=False):
        """
        Executes SELECT query with progress information.
        See, :ref:`execute-with-progress`.

        :param query: query that will be send to server.
        :param params: substitution parameters for SELECT queries and data for
                       INSERT queries. Data for INSERT can be `list`, `tuple`
                       or :data:`~types.GeneratorType`.
                       Defaults to ``None`` (no parameters  or data).
        :param with_column_types: if specified column names and types will be
                                  returned alongside with result.
                                  Defaults to ``False``.
        :param external_tables: external tables to send.
                                Defaults to ``None`` (no external tables).
        :param query_id: the query identifier. If no query id specified
                         ByteHouse server will generate it.
        :param settings: dictionary of query settings.
                         Defaults to ``None`` (no additional settings).
        :param types_check: enables type checking of data for INSERT queries.
                            Causes additional overhead. Defaults to ``False``.
        :param columnar: if specified the result will be returned in
                         column-oriented form.
                         Defaults to ``False`` (row-like form).
        :return: :ref:`progress-query-result` proxy.
        """

        with self.disconnect_on_error(query, settings):
            return self.process_ordinary_query_with_progress(
                query, params=params, with_column_types=with_column_types,
                external_tables=external_tables, query_id=query_id,
                types_check=types_check, columnar=columnar
            )

    def execute_iter(
            self, query, params=None, with_column_types=False,
            external_tables=None, query_id=None, settings=None,
            types_check=False, chunk_size=1):
        """

        Executes SELECT query with results streaming. See, :ref:`execute-iter`.

        :param query: query that will be send to server.
        :param params: substitution parameters for SELECT queries and data for
                       INSERT queries. Data for INSERT can be `list`, `tuple`
                       or :data:`~types.GeneratorType`.
                       Defaults to ``None`` (no parameters  or data).
        :param with_column_types: if specified column names and types will be
                                  returned alongside with result.
                                  Defaults to ``False``.
        :param external_tables: external tables to send.
                                Defaults to ``None`` (no external tables).
        :param query_id: the query identifier. If no query id specified
                         ByteHouse server will generate it.
        :param settings: dictionary of query settings.
                         Defaults to ``None`` (no additional settings).
        :param types_check: enables type checking of data for INSERT queries.
                            Causes additional overhead. Defaults to ``False``.
        :param chunk_size: chunk query results.
        :return: :ref:`iter-query-result` proxy.
        """
        with self.disconnect_on_error(query, settings):
            rv = self.iter_process_ordinary_query(
                query, params=params, with_column_types=with_column_types,
                external_tables=external_tables,
                query_id=query_id, types_check=types_check
            )
            return chunks(rv, chunk_size) if chunk_size > 1 else rv

    def query_dataframe(
            self, query, params=None, external_tables=None, query_id=None,
            settings=None):
        """

        Queries DataFrame with specified SELECT query.

        :param query: query that will be send to server.
        :param params: substitution parameters.
                       Defaults to ``None`` (no parameters  or data).
        :param external_tables: external tables to send.
                                Defaults to ``None`` (no external tables).
        :param query_id: the query identifier. If no query id specified
                         ByteHouse server will generate it.
        :param settings: dictionary of query settings.
                         Defaults to ``None`` (no additional settings).
        :return: pandas DataFrame.
        """

        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError('Extras for NumPy must be installed')

        data, columns = self.execute(
            query, columnar=True, with_column_types=True, params=params,
            external_tables=external_tables, query_id=query_id,
            settings=settings
        )

        columns = [re.sub(r'\W', '_', name) for name, type_ in columns]
        return pd.DataFrame(
            {col: d for d, col in zip(data, columns)}, columns=columns
        )

    def insert_dataframe(
            self, query, dataframe, external_tables=None, query_id=None,
            settings=None):
        """

        Inserts pandas DataFrame with specified query.

        :param query: query that will be send to server.
        :param dataframe: pandas DataFrame.
        :param external_tables: external tables to send.
                                Defaults to ``None`` (no external tables).
        :param query_id: the query identifier. If no query id specified
                         ByteHouse server will generate it.
        :param settings: dictionary of query settings.
                         Defaults to ``None`` (no additional settings).
        :return: number of inserted rows.
        """

        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError('Extras for NumPy must be installed')

        start_time = time()

        with self.disconnect_on_error(query, settings):
            self.connection.send_query(query, query_id=query_id)
            self.connection.send_external_tables(external_tables)

            sample_block = self.receive_sample_block()
            rv = None
            if sample_block:
                columns = [x[0] for x in sample_block.columns_with_types]
                # raise if any columns are missing from the dataframe
                diff = set(columns) - set(dataframe.columns)
                if len(diff):
                    msg = "DataFrame missing required columns: {}"
                    raise ValueError(msg.format(list(diff)))

                data = [dataframe[column].values for column in columns]
                rv = self.send_data(sample_block, data, columnar=True)
                self.receive_end_of_query()

            self.last_query.store_elapsed(time() - start_time)
            return rv

    def process_ordinary_query_with_progress(
            self, query, params=None, with_column_types=False,
            external_tables=None, query_id=None,
            types_check=False, columnar=False):

        if params is not None:
            query = self.substitute_params(
                query, params, self.connection.context
            )

        self.connection.send_query(query, query_id=query_id)
        self.connection.send_external_tables(external_tables,
                                             types_check=types_check)
        return self.receive_result(with_column_types=with_column_types,
                                   progress=True, columnar=columnar)

    def process_ordinary_query(
            self, query, params=None, with_column_types=False,
            external_tables=None, query_id=None,
            types_check=False, columnar=False):

        if params is not None:
            query = self.substitute_params(
                query, params, self.connection.context
            )

        self.connection.send_query(query, query_id=query_id)
        self.connection.send_external_tables(external_tables,
                                             types_check=types_check)
        return self.receive_result(with_column_types=with_column_types,
                                   columnar=columnar)

    def iter_process_ordinary_query(
            self, query, params=None, with_column_types=False,
            external_tables=None, query_id=None,
            types_check=False):

        if params is not None:
            query = self.substitute_params(
                query, params, self.connection.context
            )

        self.connection.send_query(query, query_id=query_id)
        self.connection.send_external_tables(external_tables,
                                             types_check=types_check)
        return self.iter_receive_result(with_column_types=with_column_types)

    def process_insert_query(self, query_without_data, data,
                             external_tables=None, query_id=None,
                             types_check=False, columnar=False):
        self.connection.send_query(query_without_data, query_id=query_id)
        self.connection.send_external_tables(external_tables,
                                             types_check=types_check)

        sample_block = self.receive_sample_block()
        if sample_block:
            rv = self.send_data(sample_block, data,
                                types_check=types_check, columnar=columnar)
            self.receive_end_of_query()
            return rv

    def receive_sample_block(self):
        while True:
            packet = self.connection.receive_packet()

            if packet.type == ServerPacketTypes.DATA:
                return packet.block

            elif packet.type == ServerPacketTypes.EXCEPTION:
                raise packet.exception

            elif packet.type == ServerPacketTypes.LOG:
                log_block(packet.block)

            elif packet.type == ServerPacketTypes.TABLE_COLUMNS:
                pass

            else:
                message = self.connection.unexpected_packet_message(
                    'Data, Exception, Log or TableColumns', packet.type
                )
                raise errors.UnexpectedPacketFromServerError(message)

    def send_data(self, sample_block, data, types_check=False, columnar=False):
        inserted_rows = 0

        client_settings = self.connection.context.client_settings
        block_cls = ColumnOrientedBlock if columnar else RowOrientedBlock

        if client_settings['use_numpy']:
            try:
                from .numpy.helpers import column_chunks as numpy_column_chunks

                if columnar:
                    slicer = numpy_column_chunks
                else:
                    raise ValueError(
                        'NumPy inserts is only allowed with columnar=True'
                    )

            except ImportError:
                raise RuntimeError('Extras for NumPy must be installed')

        else:
            slicer = column_chunks if columnar else chunks

        for chunk in slicer(data, client_settings['insert_block_size']):
            block = block_cls(sample_block.columns_with_types, chunk,
                              types_check=types_check)
            self.connection.send_data(block)
            inserted_rows += block.num_rows

        # Empty block means end of data.
        self.connection.send_data(block_cls())
        return inserted_rows

    def receive_end_of_query(self):
        while True:
            packet = self.connection.receive_packet()

            if packet.type == ServerPacketTypes.END_OF_STREAM:
                break

            elif packet.type == ServerPacketTypes.PROGRESS:
                continue

            elif packet.type == ServerPacketTypes.EXCEPTION:
                raise packet.exception

            elif packet.type == ServerPacketTypes.LOG:
                log_block(packet.block)

            elif packet.type == ServerPacketTypes.TABLE_COLUMNS:
                pass

            elif packet.type == ServerPacketTypes.PROFILE_EVENTS:
                pass

            else:
                message = self.connection.unexpected_packet_message(
                    'Exception, EndOfStream or Log', packet.type
                )
                raise errors.UnexpectedPacketFromServerError(message)

    def cancel(self, with_column_types=False):
        # TODO: Add warning if already cancelled.
        self.connection.send_cancel()
        # Client must still read until END_OF_STREAM packet.
        return self.receive_result(with_column_types=with_column_types)

    def substitute_params(self, query, params, context):
        """
        Substitutes parameters into a provided query.

        For example::

            client = Client(...)

            substituted_query = client.substitute_params(
                query='SELECT 1234, %(foo)s',
                params={'foo': 'bar'},
                context=client.connection.context
            )

            # prints: SELECT 1234, 'bar'
            print(substituted_query)
        """
        if not isinstance(params, dict):
            raise ValueError('Parameters are expected in dict form')

        escaped = escape_params(params, context)
        return query % escaped

    @classmethod
    def from_url(cls, url):
        """
        Return a client configured from the given URL.

        For example::

            bytehouse://[user:password]@localhost:9000/default
            bytehouses://[user:password]@localhost:9440/default

        Three URL schemes are supported:
            bytehouse:// creates a normal TCP socket connection
            bytehouses:// creates a SSL wrapped TCP socket connection

        Any additional querystring arguments will be passed along to
        the Connection class's initializer.
        """
        # TODO: Fix sqlalchemy uri contains %2F
        url = url.replace("%2F", "")
        url = urlparse(url)

        settings = {}
        kwargs = {}

        host = url.hostname

        if url.port is not None:
            kwargs['port'] = url.port

        path = url.path.replace('/', '', 1)
        if path:
            kwargs['database'] = path

        if url.username is not None:
            kwargs['user'] = unquote(url.username)

        if url.password is not None:
            kwargs['password'] = unquote(url.password)

        if url.scheme == 'bytehouses':
            kwargs['secure'] = True

        compression_algs = {'lz4', 'lz4hc', 'zstd'}
        timeouts = {
            'connect_timeout',
            'send_receive_timeout',
            'sync_request_timeout'
        }

        # Default 'secure' = True
        if 'secure' not in kwargs:
            kwargs['secure'] = True

        for name, value in parse_qs(url.query).items():
            if not value or not len(value):
                continue

            value = value[0]

            if name == 'compression':
                value = value.lower()
                if value in compression_algs:
                    kwargs[name] = value
                else:
                    kwargs[name] = asbool(value)

            elif name == 'secure':
                kwargs[name] = asbool(value)

            elif name == 'use_numpy':
                settings[name] = asbool(value)

            elif name == 'round_robin':
                kwargs[name] = asbool(value)

            elif name == 'client_name':
                kwargs[name] = value

            elif name in timeouts:
                kwargs[name] = float(value)

            elif name == 'compress_block_size':
                kwargs[name] = int(value)

            elif name == 'settings_is_important':
                kwargs[name] = asbool(value)

            # ssl
            elif name == 'verify':
                kwargs[name] = asbool(value)
            elif name == 'ssl_version':
                kwargs[name] = getattr(ssl, value)
            elif name in ['ca_certs', 'ciphers', 'keyfile', 'certfile',
                          'server_hostname']:
                kwargs[name] = value
            elif name == 'alt_hosts':
                kwargs['alt_hosts'] = value
            elif name == 'account':
                kwargs['account'] = value
            elif name == 'user':
                kwargs['user'] = value
            elif name == 'password':
                kwargs['password'] = value
            elif name == 'region':
                kwargs['region'] = value.upper()
            elif name == 'database':
                kwargs['database'] = value
            else:
                settings[name] = value

        if settings:
            kwargs['settings'] = settings

        return cls(host, **kwargs)
