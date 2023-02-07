from bytehouse_driver import Client, connect
from unittest import TestCase
from enum import IntEnum
from datetime import date, datetime
from ipaddress import IPv6Address, IPv4Address
from decimal import Decimal
from uuid import UUID
import configparser


class DocumentationTestCase(TestCase):
    file_config = configparser.ConfigParser()
    file_config.read(['setup.cfg'])

    host = file_config.get('db', 'host')
    port = file_config.getint('db', 'port')
    database = file_config.get('db', 'database')
    account = file_config.get('db', 'account')
    user = file_config.get('db', 'user')
    password = file_config.get('db', 'password')
    api_key = file_config.get('db', 'api_key')
    region = file_config.get('db', 'region')

    def test_sql(self):
        client = Client(
            region=self.region,
            account=self.account,
            user=self.user,
            password=self.password
        )
        # DDL Query
        client.execute("DROP DATABASE IF EXISTS demo_db")
        client.execute("CREATE DATABASE demo_db")
        client.execute("CREATE TABLE demo_db.demo_tb (id INT) ENGINE=CnchMergeTree() ORDER BY tuple()")

        # DML Query
        client.execute("INSERT INTO demo_db.demo_tb VALUES", [[1], [2], [3]])

        # DQL Query
        result_set = client.execute("SELECT * FROM demo_db.demo_tb")
        last = 0
        for result in result_set:
            assert result[0] == last+1
            last = last+1

    def test_int_types(self):
        client = Client(
            region=self.region,
            account=self.account,
            user=self.user,
            password=self.password
        )
        client.execute("DROP DATABASE IF EXISTS demo_db")
        client.execute("CREATE DATABASE demo_db")
        client.execute("CREATE TABLE demo_db.demo_tb (a Int8, b Int16, c Int32, d Int64, e UInt8, f UInt16, "
                       "g UInt32, h UInt64) ENGINE=CnchMergeTree() ORDER BY tuple()")
        data = [
            (-10, -300, -123581321, -123581321345589144,
             10, 300, 123581321, 123581321345589144)
        ]
        client.execute("INSERT INTO demo_db.demo_tb VALUES", data)
        query = 'SELECT * FROM demo_db.demo_tb'

        inserted = client.execute(query)
        self.assertEqual(inserted, data)

    def test_float_types(self):
        client = Client(
            region=self.region,
            account=self.account,
            user=self.user,
            password=self.password
        )
        client.execute("DROP DATABASE IF EXISTS demo_db")
        client.execute("CREATE DATABASE demo_db")
        client.execute("CREATE TABLE demo_db.demo_tb (a Float32, b Float64) ENGINE=CnchMergeTree() ORDER BY tuple()")
        data = [
            (3.4028235e38, 3.4028235e38),
            (3.4028235e39, 3.4028235e39),
            (-3.4028235e39, 3.4028235e39),
            (1, 2)
        ]
        client.execute("INSERT INTO demo_db.demo_tb VALUES", data, types_check=True)
        query = 'SELECT * FROM demo_db.demo_tb'

        inserted = client.execute(query)
        self.assertEqual(inserted, [
            (3.4028234663852886e+38, 3.4028235e38),
            (float('inf'), 3.4028235e39),
            (-float('inf'), 3.4028235e39),
            (1, 2)
        ])

    def test_string(self):
        client = Client(
            region=self.region,
            account=self.account,
            user=self.user,
            password=self.password
        )
        client.execute("DROP DATABASE IF EXISTS demo_db")
        client.execute("CREATE DATABASE demo_db")
        client.execute("CREATE TABLE demo_db.demo_tb (a String) ENGINE=CnchMergeTree() ORDER BY tuple()")
        data = [('axdfgrt', )]
        client.execute("INSERT INTO demo_db.demo_tb VALUES", data)
        query = 'SELECT * FROM demo_db.demo_tb'

        inserted = client.execute(query)
        self.assertEqual(inserted, data)

    def test_nullable(self):
        client = Client(
            region=self.region,
            account=self.account,
            user=self.user,
            password=self.password
        )
        client.execute("DROP DATABASE IF EXISTS demo_db")
        client.execute("CREATE DATABASE demo_db")
        client.execute("CREATE TABLE demo_db.demo_tb (a Nullable(Int32)) ENGINE=CnchMergeTree() ORDER BY tuple()")
        data = [(3, ), (None, ), (2, )]
        client.execute("INSERT INTO demo_db.demo_tb VALUES", data)
        query = 'SELECT * FROM demo_db.demo_tb'

        inserted = client.execute(query)
        self.assertEqual(inserted, data)

    def test_datetime(self):
        client = Client(
            region=self.region,
            account=self.account,
            user=self.user,
            password=self.password
        )
        client.execute("DROP DATABASE IF EXISTS demo_db")
        client.execute("CREATE DATABASE demo_db")
        client.execute("CREATE TABLE demo_db.demo_tb (a DateTime) ENGINE=CnchMergeTree() ORDER BY tuple()")
        data = [(datetime(2015, 6, 6, 12, 30, 54), ), (1530211034,)]
        client.execute("INSERT INTO demo_db.demo_tb VALUES", data)
        query = 'SELECT * FROM demo_db.demo_tb'

        inserted = client.execute(query)
        delta = 8 * 3600
        expected = [
            (datetime(2015, 6, 6, 12, 30, 54), ),
            (datetime.fromtimestamp(1530211034 - delta), ),
        ]
        self.assertEqual(inserted, expected)

    def test_date(self):
        client = Client(
            region=self.region,
            account=self.account,
            user=self.user,
            password=self.password
        )
        client.execute("DROP DATABASE IF EXISTS demo_db")
        client.execute("CREATE DATABASE demo_db")
        client.execute("CREATE TABLE demo_db.demo_tb (a Date) ENGINE=CnchMergeTree() ORDER BY tuple()")
        data = [(date(1970, 1, 1),), (datetime(2015, 6, 6, 12, 30, 54),)]
        client.execute("INSERT INTO demo_db.demo_tb VALUES", data)
        query = 'SELECT * FROM demo_db.demo_tb'

        inserted = client.execute(query)
        expected = [
            (date(1970, 1, 1),),
            (date(2015, 6, 6),)
        ]
        self.assertEqual(inserted, expected)

    def test_fixed_string(self):
        client = Client(
            region=self.region,
            account=self.account,
            user=self.user,
            password=self.password
        )
        client.execute("DROP DATABASE IF EXISTS demo_db")
        client.execute("CREATE DATABASE demo_db")
        client.execute("CREATE TABLE demo_db.demo_tb (a FixedString(4)) ENGINE=CnchMergeTree() ORDER BY tuple()")
        data = [('a', ), ('bb', ), ('ccc', ), ('dddd', ), ('я', )]
        client.execute("INSERT INTO demo_db.demo_tb VALUES", data)
        query = 'SELECT * FROM demo_db.demo_tb'

        inserted = client.execute(query)
        self.assertEqual(inserted, data)

    def test_array(self):
        client = Client(
            region=self.region,
            account=self.account,
            user=self.user,
            password=self.password
        )
        client.execute("DROP DATABASE IF EXISTS demo_db")
        client.execute("CREATE DATABASE demo_db")
        client.execute("CREATE TABLE demo_db.demo_tb (a Array(Int32)) ENGINE=CnchMergeTree() ORDER BY tuple()")
        data = [([], ), ([100, 500], )]
        client.execute("INSERT INTO demo_db.demo_tb VALUES", data)
        query = 'SELECT * FROM demo_db.demo_tb'

        inserted = client.execute(query)
        self.assertEqual(inserted, data)

    def test_enum(self):
        class A(IntEnum):
            hello = -1
            world = 2

        class B(IntEnum):
            foo = -300
            bar = 300

        client = Client(
            region=self.region,
            account=self.account,
            user=self.user,
            password=self.password
        )
        client.execute("DROP DATABASE IF EXISTS demo_db")
        client.execute("CREATE DATABASE demo_db")
        client.execute("CREATE TABLE demo_db.demo_tb (a Enum8('hello' = -1, 'world' = 2), "
                       "b Enum16('foo' = -300, 'bar' = 300)) ENGINE=CnchMergeTree() ORDER BY tuple()")
        data = [(A.hello, B.bar), (A.world, B.foo), (-1, 300), (2, -300)]
        client.execute("INSERT INTO demo_db.demo_tb VALUES", data)
        query = 'SELECT * FROM demo_db.demo_tb'

        inserted = client.execute(query)
        self.assertEqual(
            inserted, [
                ('hello', 'bar'), ('world', 'foo'),
                ('hello', 'bar'), ('world', 'foo')
            ]
        )

    def test_decimal(self):
        client = Client(
            region=self.region,
            account=self.account,
            user=self.user,
            password=self.password
        )
        client.execute("DROP DATABASE IF EXISTS demo_db")
        client.execute("CREATE DATABASE demo_db")
        client.execute("CREATE TABLE demo_db.demo_tb (a Decimal(9, 5)) ENGINE=CnchMergeTree() ORDER BY tuple()")
        data = [(Decimal('300.42'),), (300.42,), (-300,)]
        client.execute("INSERT INTO demo_db.demo_tb VALUES", data, types_check=True)
        query = 'SELECT * FROM demo_db.demo_tb'

        inserted = client.execute(query)
        self.assertEqual(inserted, [
            (Decimal('300.42'),),
            (Decimal('300.42'),),
            (Decimal('-300'),)
        ])

    def test_ip_types(self):
        client = Client(
            region=self.region,
            account=self.account,
            user=self.user,
            password=self.password
        )
        client.execute("DROP DATABASE IF EXISTS demo_db")
        client.execute("CREATE DATABASE demo_db")
        client.execute("CREATE TABLE demo_db.demo_tb (a IPv4, b IPv6) ENGINE=CnchMergeTree() ORDER BY tuple()")
        data = [
            (IPv4Address("10.0.0.1"), IPv6Address('79f4:e698:45de:a59b:2765:28e3:8d3a:35ae'),),
        ]
        client.execute("INSERT INTO demo_db.demo_tb (a, b) VALUES", data)
        query = 'SELECT * FROM demo_db.demo_tb'

        inserted = client.execute(query)
        self.assertEqual(inserted, data)

    def test_map(self):
        client = Client(
            region=self.region,
            account=self.account,
            user=self.user,
            password=self.password
        )
        client.execute("DROP DATABASE IF EXISTS demo_db")
        client.execute("CREATE DATABASE demo_db")
        client.execute("CREATE TABLE demo_db.demo_tb (a Map(String, UInt64)) ENGINE=CnchMergeTree() ORDER BY tuple()")
        data = [
            ({},),
            ({'key1': 1},),
            ({'key1': 2, 'key2': 20},),
            ({'key1': 3, 'key2': 30, 'key3': 50},)
        ]
        client.execute("INSERT INTO demo_db.demo_tb VALUES", data)
        query = 'SELECT * FROM demo_db.demo_tb'

        inserted = client.execute(query)
        self.assertEqual(inserted, data)

    def test_low_cardinality(self):
        client = Client(
            region=self.region,
            account=self.account,
            user=self.user,
            password=self.password
        )
        client.execute("DROP DATABASE IF EXISTS demo_db")
        client.execute("CREATE DATABASE demo_db")
        client.execute("CREATE TABLE demo_db.demo_tb (a LowCardinality(UInt8)) ENGINE=CnchMergeTree() ORDER BY tuple()")
        data = [(x,) for x in range(255)]
        client.execute("INSERT INTO demo_db.demo_tb VALUES", data)
        query = 'SELECT * FROM demo_db.demo_tb'

        inserted = client.execute(query)
        self.assertEqual(inserted, data)

    def test_uuid(self):
        client = Client(
            region=self.region,
            account=self.account,
            user=self.user,
            password=self.password
        )
        client.execute("DROP DATABASE IF EXISTS demo_db")
        client.execute("CREATE DATABASE demo_db")
        client.execute("CREATE TABLE demo_db.demo_tb (a UUID) ENGINE=CnchMergeTree() ORDER BY tuple()")
        data = [
            (UUID('c0fcbba9-0752-44ed-a5d6-4dfb4342b89d'),),
            ('2efcead4-ff55-4db5-bdb4-6b36a308d8e0',)
        ]
        client.execute("INSERT INTO demo_db.demo_tb VALUES", data)
        query = 'SELECT * FROM demo_db.demo_tb'

        inserted = client.execute(query)
        self.assertEqual(inserted, [
            (UUID('c0fcbba9-0752-44ed-a5d6-4dfb4342b89d'),),
            (UUID('2efcead4-ff55-4db5-bdb4-6b36a308d8e0'),)
        ])

    def test_dbapi(self):
        kwargs = {}
        kwargs.setdefault('region', self.region)
        kwargs.setdefault('account', self.account)
        kwargs.setdefault('user', self.user)
        kwargs.setdefault('password', self.password)

        connection = connect(**kwargs)
        cursor = connection.cursor()

        cursor.execute("DROP TABLE IF EXISTS cursor_tb")
        cursor.execute("CREATE TABLE cursor_tb (id INT) ENGINE=CnchMergeTree() ORDER BY tuple()")

        cursor.executemany("INSERT INTO cursor_tb (id) VALUES", [{'id': 100}])

        result_set = cursor.execute("SELECT * FROM cursor_tb")
        self.assertIsNone(result_set)
        self.assertEqual(cursor.fetchall(), [(100,)])

        connection.close()


