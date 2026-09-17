"""Synthetic rows, checked against a real SQLite schema: keys unique, foreign
keys resolvable, values within their columns, and the same seed repeatable.
"""
import datetime
import decimal
import re
import sqlite3

import pytest

from understudy_data.configuration import DatabaseConnectionConfig
from understudy_data.database import Database
from understudy_data.synthesize import SynthesisError, planTable, synthesizeTable

SCHEMA = '''
CREATE TABLE customers (id INTEGER PRIMARY KEY, email VARCHAR(40) NOT NULL, first_name VARCHAR(8), phone VARCHAR(20),
                        birth_date DATE, balance DECIMAL(8,2), active BOOLEAN, notes TEXT, zip_code INTEGER, code CHAR(3),
                        created_at TIMESTAMP, token BLOB);
CREATE TABLE products (sku VARCHAR(10) PRIMARY KEY, name VARCHAR(30), price NUMERIC(8,2) NOT NULL);
CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INT NOT NULL REFERENCES customers(id), total REAL);
CREATE TABLE order_items (order_id INT REFERENCES orders(id), line INT, sku VARCHAR(10) NOT NULL REFERENCES products(sku),
                          PRIMARY KEY (order_id, line));
CREATE TABLE tags (order_id INT REFERENCES orders(id), sku VARCHAR(10) REFERENCES products(sku), PRIMARY KEY (order_id, sku));
CREATE TABLE employees (id INTEGER PRIMARY KEY, manager_id INT REFERENCES employees(id), full_name VARCHAR(40));
CREATE TABLE bosses (id INTEGER PRIMARY KEY, boss_id INT NOT NULL REFERENCES bosses(id));
'''


@pytest.fixture
def database(tmp_path):
    path = tmp_path / 'synthetic.db'
    connection = sqlite3.connect(path)
    connection.executescript(SCHEMA)
    connection.close()

    with Database(DatabaseConnectionConfig(type='sqlite', database=str(path))) as opened:
        yield opened


def _fill(database, *tables, seed=0):
    return [synthesizeTable(database, table, rows, seed=seed) for table, rows in tables]


def test_a_schema_fills_parents_first_with_every_foreign_key_resolvable(database):
    counts = _fill(database, ('customers', 50), ('products', 5), ('orders', 200), ('order_items', 300), ('employees', 10))

    assert counts == [50, 5, 200, 300, 10]
    database.cursor.execute('PRAGMA foreign_key_check')
    assert database.cursor.fetchall() == []
    assert database.query('SELECT count(*) FROM orders WHERE customer_id NOT IN (SELECT id FROM customers)') == [(0,)]


def test_values_fit_their_columns(database):
    _fill(database, ('customers', 200))

    rows = database.query('SELECT id, email, first_name, phone, birth_date, balance, active, zip_code, code, created_at, token FROM customers')

    assert [row[0] for row in rows] == list(range(1, 201))
    assert len({row[1] for row in rows}) == 200
    assert all(re.fullmatch(r'u[0-9a-f]+@example\.test', row[1]) and len(row[1]) <= 40 for row in rows)
    assert all(row[2] is None or len(row[2]) <= 8 for row in rows)
    assert all(row[3] is None or re.fullmatch(r'\+\d \d{3} \d{3} \d{4}', row[3]) for row in rows)
    assert all(row[4] is None or datetime.date(1940, 1, 1) <= datetime.date.fromisoformat(row[4]) <= datetime.date(2005, 1, 1) for row in rows)
    assert all(row[5] is None or abs(decimal.Decimal(str(row[5]))) < 10 ** 6 for row in rows)
    assert {row[6] for row in rows} <= {0, 1, None}
    assert all(row[8] is None or re.fullmatch('[A-Z]{3}', row[8]) for row in rows)
    assert any(row[9] is None for row in rows) and any(row[9] is not None for row in rows)
    assert all(row[10] is None or len(row[10]) == 16 for row in rows)


def test_a_second_run_continues_after_the_existing_keys(database):
    _fill(database, ('customers', 5), ('products', 3))
    _fill(database, ('customers', 5), ('products', 3))

    assert [row[0] for row in database.query('SELECT id FROM customers ORDER BY id')] == list(range(1, 11))
    assert sorted(row[0] for row in database.query('SELECT sku FROM products')) == ['S1', 'S2', 'S3', 'S4', 'S5', 'S6']


def test_the_same_seed_makes_the_same_rows(tmp_path):
    def build(name, seed):
        path = tmp_path / name
        connection = sqlite3.connect(path)
        connection.executescript(SCHEMA)
        connection.close()
        with Database(DatabaseConnectionConfig(type='sqlite', database=str(path))) as opened:
            _fill(opened, ('customers', 20), ('orders', 30), seed=seed)
            return opened.query('SELECT * FROM customers'), opened.query('SELECT * FROM orders')

    assert build('a.db', 7) == build('b.db', 7)
    assert build('c.db', 7) != build('d.db', 8)


def test_a_table_keyed_only_by_foreign_keys_gets_no_more_rows_than_its_parents_allow(database):
    _fill(database, ('customers', 3), ('products', 2), ('orders', 3))

    assert synthesizeTable(database, 'tags', 100) == 6
    assert database.query('SELECT count(DISTINCT order_id || sku) FROM tags') == [(6,)]


def test_a_nullable_self_reference_is_left_null(database):
    _fill(database, ('employees', 5))

    assert database.query('SELECT count(*) FROM employees WHERE manager_id IS NULL') == [(5,)]


@pytest.mark.parametrize('table,message', [
    ('orders', 'references customers, which has no rows; fill customers first'),
    ('bosses', 'references itself through NOT NULL'),
    ('nowhere', 'table nowhere was not found'),
    ])
def test_what_cannot_be_filled_is_refused(database, table, message):
    with pytest.raises(SynthesisError, match=message):
        synthesizeTable(database, table, 5)


def test_the_plan_says_what_each_column_gets(database):
    _fill(database, ('customers', 1))

    _, makeRow, plans, available = planTable(database, 'orders', 10)
    described = {plan.column: (plan.source, plan.description) for plan in plans}

    assert available == 10
    assert described['id'] == ('primary key', 'sequential, from 1')
    assert described['customer_id'] == ('foreign key', 'an existing customers key')
    assert described['total'] == ('type', 'a number, sometimes NULL')
    assert makeRow(0)[1] == 1


def test_fixed_width_text_keys_stay_distinct_across_runs(database):
    """Padding after the number made row 0, 9 and 99 all S1000."""
    from understudy_data.synthesize import _textKeys

    keys = _textKeys(0, 5, fixed=True)
    assert [keys(row) for row in (0, 9, 99, 999)] == ['S0001', 'S0010', 'S0100', 'S1000']

    database.alter('CREATE TABLE codes (code CHAR(5) PRIMARY KEY, "rank" INTEGER)')
    assert synthesizeTable(database, 'codes', 150) == 150
    assert synthesizeTable(database, 'codes', 150) == 150
    assert database.query('SELECT count(DISTINCT code), min(code), max(code) FROM codes') == [(300, 'S0001', 'S0300')]


def test_an_integer_key_named_with_a_reserved_word_continues(database):
    database.alter('CREATE TABLE ranks ("order" INTEGER PRIMARY KEY, label TEXT)')

    _fill(database, ('ranks', 3), ('ranks', 3))

    assert [row[0] for row in database.query('SELECT "order" FROM ranks ORDER BY 1')] == list(range(1, 7))
