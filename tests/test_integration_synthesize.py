"""Synthetic rows on real servers, where every catalog and every driver's
types differ. Run with `pytest -m integration`.
"""
import importlib
import uuid

import pytest

from bauta.database import Database
from bauta.synthesize import synthesizeTable
from servers import SERVERS

pytestmark = pytest.mark.integration

# Per server: timestamp, boolean-ish, text, uuid-ish column types.
TYPES = {
    'mysql': ('DATETIME', 'BOOLEAN', 'TEXT', 'CHAR(36)'),
    'mariadb': ('DATETIME', 'BOOLEAN', 'TEXT', 'UUID'),
    'postgresql': ('TIMESTAMP', 'BOOLEAN', 'TEXT', 'UUID'),
    'oracle': ('TIMESTAMP', 'NUMBER(1)', 'CLOB', 'VARCHAR2(36)'),
    'mssql': ('DATETIME2', 'BIT', 'NVARCHAR(MAX)', 'UNIQUEIDENTIFIER'),
    }


@pytest.fixture(params=sorted(SERVERS))
def server(request):
    driver, settings = SERVERS[request.param]
    try:
        importlib.import_module(driver)
        database = Database(connectionSettings=settings)
    except Exception as error:
        pytest.skip('{} is not available ({})'.format(request.param, error))

    yield request.param, database

    database.close()


def test_a_related_schema_fills_with_valid_rows(server):
    name, database = server
    timestamp, flag, text, identifier = TYPES[name]
    suffix = uuid.uuid4().hex[:6]
    customers, products, orders, items = ('{}_{}'.format(table, suffix) for table in ('cust', 'prod', 'ord', 'item'))
    statements = [
        'CREATE TABLE {} (id INT PRIMARY KEY, email VARCHAR(60) NOT NULL, first_name VARCHAR(20), birth_date DATE, '
        'balance DECIMAL(10,2), active {}, notes {}, created_at {}, token {})'.format(customers, flag, text, timestamp, identifier),
        'CREATE TABLE {} (sku VARCHAR(12) PRIMARY KEY, price DECIMAL(8,2) NOT NULL)'.format(products),
        'CREATE TABLE {0} (id INT PRIMARY KEY, customer_id INT NOT NULL, CONSTRAINT fk_{0} FOREIGN KEY (customer_id) REFERENCES {1}(id))'.format(
            orders, customers),
        'CREATE TABLE {0} (order_id INT NOT NULL, line INT NOT NULL, sku VARCHAR(12) NOT NULL, PRIMARY KEY (order_id, line), '
        'CONSTRAINT fk1_{0} FOREIGN KEY (order_id) REFERENCES {1}(id), CONSTRAINT fk2_{0} FOREIGN KEY (sku) REFERENCES {2}(sku))'.format(
            items, orders, products),
        ]
    for statement in statements:
        database.alter(statement)

    try:
        counts = [synthesizeTable(database, table, rows) for table, rows in ((customers, 40), (products, 6), (orders, 80), (items, 150))]

        assert counts == [40, 6, 80, 150]
        assert database.query('SELECT count(*) FROM {} WHERE customer_id NOT IN (SELECT id FROM {})'.format(orders, customers)) == [(0,)]
        assert database.query('SELECT count(DISTINCT email) FROM {}'.format(customers)) == [(40,)]
        assert database.query('SELECT count(*) FROM {} WHERE sku NOT IN (SELECT sku FROM {})'.format(items, products)) == [(0,)]

        # And again, continuing after the keys already there.
        assert synthesizeTable(database, customers, 10) == 10
        assert database.query('SELECT max(id) FROM {}'.format(customers))[0][0] == 50
    finally:
        for table in (items, orders, products, customers):
            database.alter('DROP TABLE {}'.format(table))
