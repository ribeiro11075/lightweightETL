"""Masked data jobs through runDataJobs against real SQLite files.

Not marked `integration`, for the same reason test_integration_sqlite.py isn't:
there's no server to be missing. The same properties are checked against the
network databases in test_integration_masking.py.
"""
import pytest

from understudy_data.configuration import Configuration, DatabaseConnectionConfig, DatabaseType, DataJobsFile
from understudy_data.database import Database
from understudy_data.dependencyGraph import JobStatus
from understudy_data.memory import FileMemory
from understudy_data.runner import runDataJobs

KEY = 'an-end-to-end-masking-key'

CUSTOMERS = [(index, 'Person{}@Corp.com'.format(index), '+1 (555) 010-{:04d}'.format(index), '1990-01-{:02d}'.format(index % 28 + 1),
              'private note {}'.format(index)) for index in range(1, 21)]
ORDERS = [(100 + index, index % 20 + 1, '{}.50'.format(index)) for index in range(40)]

CUSTOMER_POLICY = {
    'id': {'strategy': 'key', 'domain': 'customer'},
    'email': 'email',
    'phone': 'digits',
    'birth_date': {'strategy': 'dateShift', 'maxDays': 10},
    'notes': 'null',
    }
ORDER_POLICY = {
    'id': 'keep',
    'customer_id': {'strategy': 'key', 'domain': 'customer'},
    'amount': 'keep',
    }


@pytest.fixture
def databases(tmp_path):
    settings = {
        'prod': DatabaseConnectionConfig(type=DatabaseType.SQLITE, database=str(tmp_path / 'prod.db')),
        'staging': DatabaseConnectionConfig(type=DatabaseType.SQLITE, database=str(tmp_path / 'staging.db')),
        }

    for alias in settings:
        with Database(connectionSettings=settings[alias]) as database:
            for table in ('customers', 'customers_stage'):
                database.alter('CREATE TABLE {} (id INT PRIMARY KEY, email TEXT, phone TEXT, birth_date TEXT, notes TEXT)'.format(table))
            database.alter('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount TEXT)')

    with Database(connectionSettings=settings['prod']) as database:
        database.insert(table='customers', data=CUSTOMERS, chunkSize=100)
        database.insert(table='orders', data=ORDERS, chunkSize=100)

    return settings


def job(table, policy, **overrides):
    definition = {
        'active': True, 'sourceDatabase': 'prod', 'sourceQuery': 'SELECT * FROM {}'.format(table), 'targetDatabase': 'staging',
        'targetTableFinal': table, 'insertStrategy': 'upsert', 'chunkSize': 7, 'masking': {'key': KEY, 'columns': policy},
        }
    definition.update(overrides)
    return definition


def run(databases, tmp_path, jobs):
    jobsFile = Configuration.validateJobConfiguration({'workers': 2, 'jobs': jobs}, DataJobsFile)
    result = runDataJobs(jobsFile=jobsFile, databaseConfiguration=databases, logFile=tmp_path / 'jobs.log',
                         memory=FileMemory(memoryFile=tmp_path / 'memory.yaml'))
    return jobsFile, result


def rows(settings, query):
    with Database(connectionSettings=settings) as database:
        return database.query(query)


def test_masked_tables_still_join(databases, tmp_path):
    _, result = run(databases, tmp_path, {
        'maskCustomers': job('customers', CUSTOMER_POLICY),
        'maskOrders': job('orders', ORDER_POLICY, predecessors=['maskCustomers']),
        })

    assert result.succeeded, result.outcomes
    joined = rows(databases['staging'], 'SELECT count(*) FROM orders o JOIN customers c ON c.id = o.customer_id')
    assert joined == [(40,)]
    assert rows(databases['staging'], 'SELECT id FROM customers ORDER BY id') != [(index,) for index in range(1, 21)]


def test_masked_values_no_longer_carry_the_originals(databases, tmp_path):
    _, result = run(databases, tmp_path, {'maskCustomers': job('customers', CUSTOMER_POLICY)})

    assert result.succeeded, result.outcomes
    masked = rows(databases['staging'], 'SELECT id, email, phone, birth_date, notes FROM customers')
    originals = {value for row in CUSTOMERS for value in row[1:]}

    assert len(masked) == 20
    for _, email, phone, birthDate, notes in masked:
        assert email.endswith('@example.test')
        assert len(phone) == len('+1 (555) 010-0000')
        assert len(birthDate) == 10
        assert notes is None
        # A shifted date may coincide with another person's real one; only the
        # identifying columns must never reproduce an original value.
        assert not {email, phone} & originals


def test_masking_is_reproducible(databases, tmp_path):
    jobs = {'maskCustomers': job('customers', CUSTOMER_POLICY)}

    run(databases, tmp_path, jobs)
    first = rows(databases['staging'], 'SELECT * FROM customers ORDER BY id')

    with Database(connectionSettings=databases['staging']) as database:
        database.truncate('customers')

    run(databases, tmp_path, jobs)

    assert rows(databases['staging'], 'SELECT * FROM customers ORDER BY id') == first


def test_an_uncovered_column_fails_the_job_before_anything_is_written(databases, tmp_path):
    """The default that matters: a new production column never reaches a copy
    unmasked. Nothing is written -- not even to a stage table -- and the error
    is not retried, since a second attempt can only fail the same way.
    """
    policy = dict(CUSTOMER_POLICY)
    del policy['notes']

    _, result = run(databases, tmp_path, {'maskCustomers': job('customers', policy, retries=3, retryDelaySeconds=5,
                                                               insertStrategy='swap', targetTableStage='customers_stage')})

    [outcome] = result.outcomes
    assert outcome.status == JobStatus.FAILED
    assert 'MaskingError' in outcome.error and 'notes' in outcome.error
    assert outcome.attempts == 1
    assert rows(databases['staging'], 'SELECT count(*) FROM customers') == [(0,)]
    assert rows(databases['staging'], 'SELECT count(*) FROM customers_stage') == [(0,)]


def test_a_value_a_strategy_cannot_mask_fails_the_job_without_leaking_it(databases, tmp_path):
    policy = dict(CUSTOMER_POLICY, email='number')

    _, result = run(databases, tmp_path, {'maskCustomers': job('customers', policy)})

    [outcome] = result.outcomes
    assert outcome.status == JobStatus.FAILED
    assert 'email' in outcome.error
    assert '@Corp.com' not in outcome.error
    assert '@Corp.com' not in (tmp_path / 'jobs.log').read_text()


def test_masking_in_place_swaps_through_a_stage_table(databases, tmp_path):
    inPlace = job('customers', CUSTOMER_POLICY, targetDatabase='prod', insertStrategy='swap', targetTableStage='customers_stage')

    _, result = run(databases, tmp_path, {'maskInPlace': inPlace})

    assert result.succeeded, result.outcomes
    emails = [row[0] for row in rows(databases['prod'], 'SELECT email FROM customers')]
    assert len(emails) == 20
    assert all(email.endswith('@example.test') for email in emails)


def test_the_manifest_describes_what_was_applied(databases, tmp_path):
    jobsFile, result = run(databases, tmp_path, {
        'maskCustomers': job('customers', CUSTOMER_POLICY),
        'maskOrders': job('orders', dict(ORDER_POLICY, surprise='keep'), predecessors=['maskCustomers']),
        'plainCopy': {key: value for key, value in job('orders', ORDER_POLICY).items() if key != 'masking'},
        })

    manifest = result.maskingManifest(jobsFile.jobs)
    byJob = {entry['job']: entry for entry in manifest['jobs']}

    assert set(byJob) == {'maskCustomers', 'maskOrders'}
    assert byJob['maskCustomers']['status'] == 'completed'
    assert byJob['maskCustomers']['rowCount'] == 20
    assert byJob['maskCustomers']['targetTable'] == 'customers'
    assert [(column['column'], column['strategy'], column['domain']) for column in byJob['maskCustomers']['columns']] == [
        ('id', 'key', 'customer'), ('email', 'email', 'email'), ('phone', 'digits', 'phone'),
        ('birth_date', 'dateShift', 'birth_date'), ('notes', 'null', None)]
    assert byJob['maskOrders']['status'] == 'failed'
    assert byJob['maskOrders']['columns'] == []
    assert byJob['maskCustomers']['keyFingerprint'] == byJob['maskOrders']['keyFingerprint']
    assert KEY not in str(manifest)


def test_masking_composes_with_an_incremental_load(databases, tmp_path):
    """The watermark is read from the raw rows, so masking the watermark column
    itself can't move the next run's starting point.
    """
    policy = dict(CUSTOMER_POLICY, birth_date={'strategy': 'dateShift', 'maxDays': 400})
    incremental = job('customers', policy, sourceQuery='SELECT * FROM customers WHERE birth_date > {{ watermark }}',
                      watermarkColumn='birth_date', watermarkInitial='1900-01-01')

    run(databases, tmp_path, {'maskCustomers': incremental})

    assert FileMemory(memoryFile=tmp_path / 'memory.yaml').readWatermarks() == {'maskCustomers': '1990-01-21'}
