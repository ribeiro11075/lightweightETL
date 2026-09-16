"""Subset planning, checked the way that matters: by running the generated
queries against a real SQLite schema and confirming every copied foreign key
points at a copied row.
"""
import sqlite3

import pytest

from lightweight_etl.databaseDialects import ForeignKey
from lightweight_etl.subset import SubsetError, parseIgnore, planSubset

SCHEMA = '''
CREATE TABLE regions (id INT PRIMARY KEY, name TEXT);
CREATE TABLE customers (id INT PRIMARY KEY, region_id INT REFERENCES regions(id), tier TEXT);
CREATE TABLE suppliers (id INT PRIMARY KEY);
CREATE TABLE products (id INT PRIMARY KEY, supplier_id INT REFERENCES suppliers(id));
CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT REFERENCES customers(id));
CREATE TABLE order_items (order_id INT REFERENCES orders(id), line INT, product_id INT REFERENCES products(id), PRIMARY KEY (order_id, line));
CREATE TABLE shipments (order_id INT, line INT, carrier TEXT, FOREIGN KEY (order_id, line) REFERENCES order_items(order_id, line));
CREATE TABLE unrelated (id INT PRIMARY KEY);
'''


def foreignKeysOf(connection):
    from lightweight_etl.databaseDialects import SQLiteDialect

    return SQLiteDialect().foreignKeys(connection.cursor())


@pytest.fixture
def shop(tmp_path):
    connection = sqlite3.connect(str(tmp_path / 'shop.db'))
    connection.executescript(SCHEMA)
    connection.executemany('INSERT INTO regions VALUES (?, ?)', [(1, 'eu'), (2, 'us'), (3, 'apac')])
    connection.executemany('INSERT INTO customers VALUES (?, ?, ?)', [(index, index % 3 + 1, 'gold' if index % 4 == 0 else 'basic') for index in range(1, 21)])
    connection.executemany('INSERT INTO suppliers VALUES (?)', [(index,) for index in range(1, 6)])
    connection.executemany('INSERT INTO products VALUES (?, ?)', [(index, index % 5 + 1) for index in range(1, 11)])
    connection.executemany('INSERT INTO orders VALUES (?, ?)', [(index, index % 20 + 1) for index in range(1, 61)])
    connection.executemany('INSERT INTO order_items VALUES (?, ?, ?)', [(order, line, order % 10 + 1) for order in range(1, 61) for line in (1, 2)])
    connection.executemany('INSERT INTO shipments VALUES (?, ?, ?)', [(order, 1, 'ups') for order in range(1, 61, 3)])
    connection.executemany('INSERT INTO unrelated VALUES (?)', [(1,)])
    connection.commit()

    yield connection

    connection.close()


def selected(connection, plan):
    return {table: connection.execute(plan.queries[table]).fetchall() for table in plan.tables}


def assertReferentiallyComplete(connection, plan):
    """Every foreign key of every copied row resolves to a copied row."""
    rows = selected(connection, plan)
    columnsOf = {table: [row[1] for row in connection.execute('PRAGMA table_info({})'.format(table))] for table in plan.tables}

    for foreignKey in foreignKeysOf(connection):
        if foreignKey.table not in rows or foreignKey in plan.ignored:
            continue
        childIndexes = [columnsOf[foreignKey.table].index(column) for column in foreignKey.columns]
        parentIndexes = [columnsOf[foreignKey.referencedTable].index(column) for column in foreignKey.referencedColumns]
        parentKeys = {tuple(row[index] for index in parentIndexes) for row in rows.get(foreignKey.referencedTable, [])}
        for row in rows[foreignKey.table]:
            key = tuple(row[index] for index in childIndexes)
            if None not in key:
                assert key in parentKeys, '{} row {} references a {} row outside the subset'.format(foreignKey.table, row, foreignKey.referencedTable)


def test_a_subset_follows_children_down_and_parents_up(shop):
    plan = planSubset(foreignKeysOf(shop), root='customers', where="tier = 'gold'")
    rows = selected(shop, plan)

    assert set(plan.tables) == {'regions', 'customers', 'suppliers', 'products', 'orders', 'order_items', 'shipments'}
    assert {row[0] for row in rows['customers']} == {4, 8, 12, 16, 20}
    assert {row[1] for row in rows['orders']} == {4, 8, 12, 16, 20}
    assert len(rows['order_items']) == 2 * len(rows['orders'])
    assert {row[0] for row in rows['products']} == {2, 4, 6, 8, 10}
    assertReferentiallyComplete(shop, plan)


def test_tables_load_parents_first(shop):
    plan = planSubset(foreignKeysOf(shop), root='customers', where="tier = 'gold'")

    position = {table: index for index, table in enumerate(plan.tables)}
    for table, parents in plan.parents.items():
        for parent in parents:
            assert position[parent] < position[table]

    assert plan.parents['order_items'] == ['orders', 'products']


def test_a_subset_rooted_mid_graph_pulls_in_its_parents(shop):
    plan = planSubset(foreignKeysOf(shop), root='orders', where='id <= 3')
    rows = selected(shop, plan)

    assert {row[0] for row in rows['orders']} == {1, 2, 3}
    assert {row[0] for row in rows['customers']} == {2, 3, 4}
    assertReferentiallyComplete(shop, plan)


def test_without_children_only_the_root_and_its_parents_are_copied(shop):
    plan = planSubset(foreignKeysOf(shop), root='orders', where='id <= 3', followChildren=False)

    assert set(plan.tables) == {'regions', 'customers', 'orders'}
    assertReferentiallyComplete(shop, plan)


def test_composite_foreign_keys_are_followed(shop):
    plan = planSubset(foreignKeysOf(shop), root='shipments', where="carrier = 'ups'")
    rows = selected(shop, plan)

    assert len(rows['order_items']) == len(rows['shipments'])
    assertReferentiallyComplete(shop, plan)


def test_table_names_match_case_insensitively(shop):
    plan = planSubset(foreignKeysOf(shop), root='CUSTOMERS', where="tier = 'gold'")

    assert 'customers' in plan.tables


def test_a_root_with_no_foreign_keys_is_copied_alone(shop):
    plan = planSubset(foreignKeysOf(shop), root='unrelated', where='1 = 1')

    assert plan.tables == ['unrelated']
    assert selected(shop, plan) == {'unrelated': [(1,)]}


def test_a_cycle_is_reported_with_how_to_break_it():
    foreignKeys = [
        ForeignKey('employees', ('manager_id',), 'employees', ('id',), 'fk_manager'),
        ForeignKey('employees', ('department_id',), 'departments', ('id',), 'fk_department'),
        ]

    with pytest.raises(SubsetError, match=r'cycle \(employees -> employees\).*--ignore-foreign-key'):
        planSubset(foreignKeys, root='departments', where='1 = 1')


def test_ignoring_a_foreign_key_breaks_a_cycle():
    foreignKeys = [
        ForeignKey('a', ('b_id',), 'b', ('id',), 'fk1'),
        ForeignKey('b', ('a_id',), 'a', ('id',), 'fk2'),
        ]

    with pytest.raises(SubsetError, match='cycle'):
        planSubset(foreignKeys, root='a', where='1 = 1')

    plan = planSubset(foreignKeys, root='a', where='1 = 1', ignore=['b.a_id'])

    assert plan.tables == ['b', 'a']
    assert [foreignKey.name for foreignKey in plan.ignored] == ['fk2']


def test_ignore_entries_must_name_a_column():
    with pytest.raises(SubsetError, match='table.column'):
        parseIgnore(['orders'])

    assert parseIgnore(['Sales.Orders.customer_id']) == {('SALES.ORDERS', 'CUSTOMER_ID')}


def test_no_scope_reuses_an_alias_from_a_scope_enclosing_it(shop):
    """A shared subquery may appear twice side by side, reusing its aliases --
    that's harmless. Reuse inside an *enclosing* scope would shadow the outer
    alias and silently correlate against the wrong table.
    """
    import re

    plan = planSubset(foreignKeysOf(shop), root='customers', where="tier = 'gold'")

    for query in plan.queries.values():
        enclosing = []
        depth = 0
        for token in re.findall(r'\) s\d+|\(|\)|FROM \w+ t\d+', query):
            if token.startswith(')'):
                depth -= 1
                enclosing = [(level, alias) for level, alias in enclosing if level <= depth]
                if token != ')':
                    alias = token.split()[1]
                    assert alias not in {alias for _, alias in enclosing}
                    enclosing.append((depth, alias))
            elif token == '(':
                depth += 1
            else:
                alias = token.split()[2]
                assert alias not in {alias for _, alias in enclosing}
                enclosing.append((depth, alias))
