"""Masking, foreign-key discovery and subsetting against real servers.

The parts of the masking work that touch dialect-specific ground: each
database's foreign-key catalog (queried differently on every one), the subset
queries' nested EXISTS form (which SQL Server and Oracle are pickiest about),
and the Python types each driver hands the masking strategies -- Decimal from
NUMERIC, float from Oracle's scaled NUMBER, datetime from Oracle's DATE.

Parametrized over the five servers in docker-compose.yml; any that isn't
reachable, or whose driver isn't installed, is skipped with a reason. Run with
`pytest -m integration`.
"""
import datetime
import decimal
import importlib
import uuid

import pytest

from understudy_data.configuration import Configuration, DataJobsFile
from understudy_data.database import Database
from understudy_data.dependencyGraph import JobStatus
from understudy_data.discovery import proposeTable
from understudy_data.memory import FileMemory
from understudy_data.runner import runDataJobs
from understudy_data.subset import planSubset
from servers import SERVERS

pytestmark = pytest.mark.integration

KEY = 'an-integration-masking-key'


@pytest.fixture(params=sorted(SERVERS))
def server(request):
    driver, settings = SERVERS[request.param]

    try:
        importlib.import_module(driver)
        database = Database(connectionSettings=settings)
    except Exception as error:
        pytest.skip('{} is not available ({})'.format(request.param, error))

    yield settings, database

    database.close()


@pytest.fixture
def schema(server):
    """customers <- orders, plus a composite key: bins <- placements."""
    settings, database = server
    suffix = uuid.uuid4().hex[:6]
    names = {name: '{}_{}'.format(name, suffix) for name in ('customers', 'orders', 'bins', 'placements', 'customers_copy', 'orders_copy')}

    statements = [
        'CREATE TABLE {customers} (id INT PRIMARY KEY, email VARCHAR(100), tier VARCHAR(10), balance NUMERIC(12,2), born DATE)',
        'CREATE TABLE {orders} (id INT PRIMARY KEY, customer_id INT, CONSTRAINT fk_{orders} FOREIGN KEY (customer_id) REFERENCES {customers}(id))',
        'CREATE TABLE {bins} (aisle INT, shelf INT, PRIMARY KEY (aisle, shelf))',
        'CREATE TABLE {placements} (id INT PRIMARY KEY, aisle INT, shelf INT, '
        'CONSTRAINT fk_{placements} FOREIGN KEY (aisle, shelf) REFERENCES {bins}(aisle, shelf))',
        'CREATE TABLE {customers_copy} (id INT PRIMARY KEY, email VARCHAR(100), tier VARCHAR(10), balance NUMERIC(12,2), born DATE)',
        'CREATE TABLE {orders_copy} (id INT PRIMARY KEY, customer_id INT)',
        ]
    for statement in statements:
        database.alter(statement.format(**names))

    database.insert(table=names['customers'], data=[
        (index, 'person{}@corp.com'.format(index), 'gold' if index % 3 == 0 else 'basic', decimal.Decimal('{}.25'.format(index * 10)),
         datetime.date(1980 + index, 1 + index % 12, 1 + index % 28))
        for index in range(1, 13)], chunkSize=50)
    database.insert(table=names['orders'], data=[(100 + index, index % 12 + 1) for index in range(30)], chunkSize=50)
    database.insert(table=names['bins'], data=[(1, 1), (1, 2), (2, 1)], chunkSize=50)
    database.insert(table=names['placements'], data=[(1, 1, 2), (2, 2, 1)], chunkSize=50)

    yield settings, database, names

    for name in ('placements', 'bins', 'orders', 'customers', 'orders_copy', 'customers_copy'):
        database.alter('DROP TABLE IF EXISTS {}'.format(names[name]))


def test_foreign_keys_are_listed_including_composite_ones(schema):
    _, database, names = schema

    foreignKeys = {foreignKey.table.lower(): foreignKey for foreignKey in database.getForeignKeys()
                   if foreignKey.table.lower() in (names['orders'], names['placements'])}

    orders = foreignKeys[names['orders']]
    assert orders.referencedTable.lower() == names['customers']
    assert [column.lower() for column in orders.columns] == ['customer_id']
    assert [column.lower() for column in orders.referencedColumns] == ['id']

    placements = foreignKeys[names['placements']]
    assert [column.lower() for column in placements.columns] == ['aisle', 'shelf']
    assert [column.lower() for column in placements.referencedColumns] == ['aisle', 'shelf']


def test_subset_queries_run_and_are_referentially_complete(schema):
    _, database, names = schema

    plan = planSubset(database.getForeignKeys(), root=names['customers'], where="tier = 'gold'")
    tables = {table.lower(): table for table in plan.tables}

    assert set(tables) == {names['customers'], names['orders']}

    customers = database.query(plan.queries[tables[names['customers']]])
    orders = database.query(plan.queries[tables[names['orders']]])

    assert sorted(row[0] for row in customers) == [3, 6, 9, 12]
    assert orders and {row[1] for row in orders} <= {row[0] for row in customers}


def test_composite_subset_queries_run(schema):
    _, database, names = schema

    plan = planSubset(database.getForeignKeys(), root=names['placements'], where='id = 1')
    tables = {table.lower(): table for table in plan.tables}

    assert database.query(plan.queries[tables[names['bins']]]) == [(1, 2)]


def test_a_masked_job_keeps_types_and_references(schema, tmp_path):
    settings, database, names = schema
    customerPolicy = {
        'id': {'strategy': 'key', 'domain': 'customer'},
        'email': 'email',
        'tier': 'keep',
        'balance': {'strategy': 'number', 'decimals': 2},
        'born': {'strategy': 'dateShift', 'maxDays': 20},
        }
    orderPolicy = {'id': 'keep', 'customer_id': {'strategy': 'key', 'domain': 'customer'}}

    def job(source, target, policy, **extra):
        definition = {'active': True, 'sourceDatabase': 'db', 'targetDatabase': 'db', 'sourceQuery': 'SELECT * FROM {}'.format(source),
                      'targetTableFinal': target, 'insertStrategy': 'upsert', 'chunkSize': 5, 'masking': {'key': KEY, 'columns': policy}}
        definition.update(extra)
        return definition

    jobsFile = Configuration.validateJobConfiguration({'workers': 1, 'jobs': {
        'maskCustomers': job(names['customers'], names['customers_copy'], customerPolicy),
        'maskOrders': job(names['orders'], names['orders_copy'], orderPolicy, predecessors=['maskCustomers']),
        }}, DataJobsFile)

    result = runDataJobs(jobsFile=jobsFile, databaseConfiguration={'db': settings}, logFile=tmp_path / 'jobs.log',
                         memory=FileMemory(memoryFile=tmp_path / 'memory.yaml'))

    assert result.succeeded, [outcome.error for outcome in result.outcomes if outcome.status != JobStatus.COMPLETED]

    joined = database.query('SELECT count(*) FROM {} o JOIN {} c ON c.id = o.customer_id'.format(names['orders_copy'], names['customers_copy']))
    assert joined[0][0] == 30

    original = database.query('SELECT id, email, balance, born FROM {} ORDER BY id'.format(names['customers']))
    masked = database.query('SELECT id, email, balance, born FROM {} ORDER BY id'.format(names['customers_copy']))

    assert len(masked) == 12
    assert [row[0] for row in masked] != [row[0] for row in original]
    assert all(row[1].endswith('@example.test') for row in masked)
    assert sorted(float(row[2]) for row in masked) != sorted(float(row[2]) for row in original)
    assert all(round(float(row[2]), 2) == float(row[2]) for row in masked)
    assert sorted(row[3] for row in masked) != sorted(row[3] for row in original)


def test_discovery_reads_real_column_types(schema):
    _, database, names = schema

    proposal = {suggestion.column.lower(): suggestion.policy for suggestion in proposeTable(database, names['customers']).columns}

    assert proposal['email'] == {'strategy': 'email'}
    assert proposal['id']['strategy'] == 'keep'
    assert proposal['tier'] == {'strategy': 'keep'}

    orders = {suggestion.column.lower(): suggestion.policy for suggestion in proposeTable(database, names['orders']).columns}
    assert orders['customer_id']['strategy'] == 'keep'


def test_the_deepest_subset_allowed_runs_on_every_server(server):
    """A 16-table chain, the deepest planSubset accepts. Nested selections
    once made PostgreSQL run out of memory planning a 12-table chain and
    restart; without MATERIALIZED it still took minutes. Every server has to
    answer every query, quickly, with only the rows that belong.
    """
    import time

    settings, database = server
    suffix = uuid.uuid4().hex[:5]
    names = ['c{}_{}'.format(level, suffix) for level in range(16)]

    try:
        database.alter('CREATE TABLE {} (id INT PRIMARY KEY, parent_id INT)'.format(names[0]))
        database.insert(table=names[0], data=[(index, None) for index in range(20)])
        for level in range(1, len(names)):
            database.alter('CREATE TABLE {0} (id INT PRIMARY KEY, parent_id INT, CONSTRAINT fk_{0} FOREIGN KEY (parent_id) REFERENCES {1}(id))'.format(
                names[level], names[level - 1]))
            database.insert(table=names[level], data=[(index, (index * 7) % 20) for index in range(20)])

        foreignKeys = [foreignKey for foreignKey in database.getForeignKeys() if foreignKey.table.lower().endswith(suffix)]
        plan = planSubset(foreignKeys, root=names[0], where='id < 5', materialize=database.dialect.supportsMaterializedSelections())

        started = time.time()
        counts = [len(database.query(plan.queries[table])) for table in plan.tables]

        assert counts == [5] * len(names)
        assert time.time() - started < 60
    finally:
        for name in reversed(names):
            try:
                database.alter('DROP TABLE {}'.format(name))
            except Exception:
                database.connection.rollback()


def _maskedJob(source, target, policy):
    return Configuration.validateJobConfiguration({'workers': 1, 'jobs': {'mask': {
        'active': True, 'sourceDatabase': 'db', 'targetDatabase': 'db', 'sourceQuery': 'SELECT * FROM {}'.format(source),
        'targetTableFinal': target, 'insertStrategy': 'upsert', 'chunkSize': 5, 'masking': {'key': KEY, 'columns': policy}}}}, DataJobsFile)


def test_checking_a_masked_query_leaves_the_connection_usable(schema):
    """Reading one row and closing the rest unread used to leave MySQL and
    MariaDB connections refusing the close itself, so --dry-run and
    audit --connect called every such job uncheckable.
    """
    from understudy_data.cli import _sourceQueryColumns

    settings, database, names = schema
    jobsFile = _maskedJob(names['customers'], names['customers_copy'], {'id': 'keep'})

    columns = _sourceQueryColumns(jobsFile.jobs['mask'], {'db': settings})

    assert [column.lower() for column in columns] == ['id', 'email', 'tier', 'balance', 'born']


def test_a_policy_that_misses_a_column_fails_once_and_names_it(schema, tmp_path):
    """The MaskingError used to be replaced by the stream's InternalError on
    MySQL, retried as if temporary, and the column never named.
    """
    settings, database, names = schema
    jobsFile = _maskedJob(names['customers'], names['customers_copy'], {'id': 'keep', 'email': 'email', 'tier': 'keep', 'balance': 'null'})
    jobsFile.jobs['mask'].retries = 2
    jobsFile.jobs['mask'].retryDelaySeconds = 0

    result = runDataJobs(jobsFile=jobsFile, databaseConfiguration={'db': settings}, memory=FileMemory(tmp_path / 'memory.yaml'),
                         logFile=tmp_path / 'runner.log')

    (outcome,) = result.outcomes
    assert outcome.status == JobStatus.FAILED and outcome.attempts == 1
    assert outcome.error.startswith('MaskingError:') and 'born' in outcome.error.lower()
    assert database.query('SELECT count(*) FROM {}'.format(names['customers_copy']))[0][0] == 0
