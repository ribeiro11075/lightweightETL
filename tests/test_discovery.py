"""Policy proposals: what a column is taken for, and the YAML they render to."""
import datetime
import decimal
import sqlite3

import pytest
import yaml

from lightweight_etl.configuration import Configuration, DatabaseConnectionConfig, DatabaseType, DataJobsFile, expandEnvironmentVariables
from lightweight_etl.database import Database
from lightweight_etl.databaseDialects import ColumnCategory, ForeignKey
from lightweight_etl.discovery import JobDraft, keyReferences, proposeTable, renderJobs, suggestColumn

TEXT = ColumnCategory.TEXT
NUMBER = ColumnCategory.NUMBER
DATE = ColumnCategory.DATE


def strategyFor(column, values=(), category=None, table='things', keyReference=None):
    return suggestColumn(table, column, category, list(values), keyReference).policy['strategy']


@pytest.mark.parametrize('column,expected', [
    ('email', 'email'),
    ('contactEmail', 'email'),
    ('EMAIL_ADDRESS', 'email'),
    ('first_name', 'fakeFirstName'),
    ('LastName', 'fakeLastName'),
    ('phone_number', 'digits'),
    ('ssn', 'key'),
    ('password_hash', 'hash'),
    ('street_address', 'fakeStreetAddress'),
    ('ip_address', 'hash'),
    ('city', 'fakeCity'),
    ('notes', 'null'),
    ('company_name', 'fakeCompany'),
    ('gender', 'shuffle'),
    ('status', 'keep'),
    ])
def test_names_suggest_strategies(column, expected):
    assert strategyFor(column) == expected


def test_a_name_is_not_trusted_over_the_columns_type():
    """`place_of_birth` is text, not a date; `token_count` is not a credential."""
    assert strategyFor('place_of_birth', ['Lisbon', 'Porto'], TEXT) != 'dateShift'
    assert strategyFor('token_count', [1, 2, 3], NUMBER) != 'hash'
    assert strategyFor('birth_date', [datetime.date(1990, 1, 1)], DATE) == 'dateShift'
    assert strategyFor('birth_date', ['1990-01-01']) == 'dateShift'


def test_salary_is_masked_as_a_number_only_when_it_is_one():
    assert strategyFor('salary', [decimal.Decimal('100.00')], NUMBER) == 'number'
    assert strategyFor('salary', ['confidential'], TEXT) == 'keep'


def test_a_bare_name_column_is_a_person_only_in_a_table_of_people():
    assert strategyFor('name', ['Ann'], TEXT, table='customers') == 'fakeName'
    assert strategyFor('name', ['Widget'], TEXT, table='products') == 'keep'


@pytest.mark.parametrize('values,expected', [
    (['a@b.com', 'c@d.org', 'e@f.net'], 'email'),
    (['123-45-6789', '987-65-4321'], 'key'),
    (['4111 1111 1111 1111', '5500-0000-0000-0004'], 'digits'),
    (['10.0.0.1', '192.168.1.20'], 'hash'),
    (['+1 (555) 010-9999', '555.010.1234'], 'digits'),
    (['x' * 100, 'y' * 90], 'null'),
    (['2026-01-01', '2026-02-03'], 'keep'),
    (['red', 'green'], 'keep'),
    ])
def test_sampled_values_suggest_strategies(values, expected):
    assert strategyFor('col', values, TEXT) == expected


def test_a_mostly_matching_sample_is_enough():
    values = ['a{}@b.com'.format(index) for index in range(9)] + ['not an email']

    assert strategyFor('contact', values, TEXT) == 'email'


def test_a_numeric_key_is_kept_and_a_text_key_is_masked_one_to_one():
    assert strategyFor('id', [1, 2], NUMBER, keyReference=('customers', True)) == 'keep'

    suggestion = suggestColumn('customers', 'code', TEXT, ['AB1'], ('customers', False))
    assert suggestion.policy == {'strategy': 'key', 'domain': 'customers'}


def test_key_references_give_both_ends_of_a_foreign_key_the_same_domain():
    foreignKeys = [ForeignKey('ORDERS', ('CUSTOMER_CODE',), 'CUSTOMERS', ('CODE',), 'fk1')]
    primaryKeys = {'CUSTOMERS': ['CODE'], 'ORDERS': ['ID']}

    parent = keyReferences('customers', ['code', 'name'], ['CODE'], foreignKeys, primaryKeys)
    child = keyReferences('orders', ['id', 'customer_code'], ['ID'], foreignKeys, primaryKeys)

    assert parent == {'code': 'customers'}
    assert child == {'id': 'orders', 'customer_code': 'customers'}


def test_a_referenced_column_that_is_not_the_primary_key_gets_a_qualified_domain():
    foreignKeys = [ForeignKey('logins', ('user_email',), 'users', ('email',), 'fk1')]
    primaryKeys = {'USERS': ['id']}

    assert keyReferences('users', ['id', 'email'], ['id'], foreignKeys, primaryKeys)['email'] == 'users.email'
    assert keyReferences('logins', ['user_email'], [], foreignKeys, primaryKeys) == {'user_email': 'users.email'}


@pytest.fixture
def sampleDatabase(tmp_path):
    connection = sqlite3.connect(str(tmp_path / 'sample.db'))
    connection.executescript('''
        CREATE TABLE customers (code TEXT PRIMARY KEY, email TEXT, notes TEXT, joined TEXT);
        CREATE TABLE orders (id INT PRIMARY KEY, customer_code TEXT REFERENCES customers(code), total NUMERIC);
        ''')
    connection.executemany('INSERT INTO customers VALUES (?, ?, ?, ?)',
                           [('C{:03d}'.format(index), 'person{}@corp.com'.format(index), None, '2026-01-01') for index in range(20)])
    connection.executemany('INSERT INTO orders VALUES (?, ?, ?)', [(index, 'C{:03d}'.format(index % 20), 9.5) for index in range(40)])
    connection.commit()
    connection.close()

    settings = DatabaseConnectionConfig(type=DatabaseType.SQLITE, database=str(tmp_path / 'sample.db'))
    with Database(connectionSettings=settings) as database:
        yield database


def test_propose_table_reads_the_schema_and_a_sample(sampleDatabase):
    customers = {suggestion.column: suggestion.policy for suggestion in proposeTable(sampleDatabase, 'customers', sampleSize=5).columns}
    orders = {suggestion.column: suggestion.policy for suggestion in proposeTable(sampleDatabase, 'orders', sampleSize=5).columns}

    assert customers['code'] == {'strategy': 'key', 'domain': 'customers'}
    assert customers['email'] == {'strategy': 'email'}
    assert customers['notes'] == {'strategy': 'null'}
    assert customers['joined'] == {'strategy': 'keep'}
    assert orders['customer_code'] == customers['code']
    assert orders['id'] == {'strategy': 'keep'}


def test_rendered_jobs_are_valid_configuration_and_carry_their_reasons(sampleDatabase, monkeypatch):
    drafts = [
        JobDraft('customers', 'SELECT * FROM customers', [], proposeTable(sampleDatabase, 'customers')),
        JobDraft('orders', 'SELECT *\nFROM orders\nWHERE total > 0', ['customers'], proposeTable(sampleDatabase, 'orders')),
        ]

    text = renderJobs(drafts, 'prod', 'staging', ['a heading'], keyVariable='TEST_MASKING_KEY', chunkSize=100)

    assert text.startswith('# a heading\n')
    assert '# name suggests an email address' in text
    assert 'person1@corp.com' not in text

    monkeypatch.setenv('TEST_MASKING_KEY', 'k' * 32)
    jobsFile = Configuration.validateJobConfiguration(expandEnvironmentVariables(yaml.safe_load(text)), DataJobsFile)

    orders = jobsFile.jobs['maskOrders']
    assert orders.predecessors == ['maskCustomers']
    assert orders.sourceQuery == 'SELECT *\nFROM orders\nWHERE total > 0'
    assert orders.insertStrategy.value == 'upsert'
    assert orders.chunkSize == 100
    assert jobsFile.jobs['maskCustomers'].masking.columns['notes'] == {'strategy': 'null'}


def test_rendered_jobs_mask_in_place_through_a_swap(sampleDatabase, monkeypatch):
    text = renderJobs([JobDraft('customers', 'SELECT * FROM customers', [], proposeTable(sampleDatabase, 'customers'))], 'prod', 'prod', [])

    monkeypatch.setenv('MASKING_KEY', 'k' * 32)
    job = Configuration.validateJobConfiguration(expandEnvironmentVariables(yaml.safe_load(text)), DataJobsFile).jobs['maskCustomers']

    assert job.insertStrategy.value == 'swap'
    assert job.targetTableStage == 'customers_masked_stage'
    assert job.targetTableFinal == 'customers'


@pytest.mark.parametrize('column,expected', [
    ('customerEmail', 'name suggests an email address'),
    ('SSN', 'name suggests a government identifier; key keeps it unique and shaped'),
    ('created_at', None),
    ('status', None),
    ])
def test_personal_data_hint_reads_the_name_alone(column, expected):
    from lightweight_etl.discovery import personalDataHint

    assert personalDataHint(column) == expected
