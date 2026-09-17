"""Proves the actual headline feature works: extracting from one database dialect
and loading into a *different* one within a single job. Every other integration
test uses the same database for both source and target, for simplicity -- this is
the one test that genuinely exercises sourceDatabase and targetDatabase pointing
at different servers of different types in the same _executeDataJob call, with a
real sourceQueryColumnTransforms entry applied in between.

Requires both a MySQL and a PostgreSQL server reachable at the settings below
(see docker-compose.yml: `docker compose up -d mysql postgresql`) and both
drivers importable. Skipped automatically, with a clear reason, if either isn't
available. Excluded from the default `pytest` run -- run with `pytest -m integration`.
"""
import uuid

import pytest

pytest.importorskip('mysql.connector', reason='mysql-connector-python is not installed (pip install -e ".[mysql]")')
pytest.importorskip('psycopg2', reason='psycopg2 is not installed (pip install psycopg2-binary, or pip install -e ".[postgresql]")')

from understudy_data.configuration import Configuration, DatabaseConnectionConfig, DatabaseType, DataJobsFile
from understudy_data.database import Database
from understudy_data.memory import FileMemory
from understudy_data.runner import runDataJobs

pytestmark = pytest.mark.integration

MYSQL_SETTINGS = DatabaseConnectionConfig(
    type=DatabaseType.MYSQL, user='root', password='root', database='understudy_test', host='127.0.0.1', port=3307,
    )
POSTGRESQL_SETTINGS = DatabaseConnectionConfig(
    type=DatabaseType.POSTGRESQL, user='postgres', password='postgres', database='understudy_test', host='127.0.0.1', port=5433,
    )


@pytest.fixture
def mysqlDatabase():
    try:
        database = Database(connectionSettings=MYSQL_SETTINGS)
    except Exception as error:
        pytest.skip(f'no live mysql server reachable at {MYSQL_SETTINGS.host}:{MYSQL_SETTINGS.port} ({error})')

    yield database

    database.close()


@pytest.fixture
def postgresqlDatabase():
    try:
        database = Database(connectionSettings=POSTGRESQL_SETTINGS)
    except Exception as error:
        pytest.skip(f'no live postgresql server reachable at {POSTGRESQL_SETTINGS.host}:{POSTGRESQL_SETTINGS.port} ({error})')

    yield database

    database.close()


@pytest.fixture
def sourceTable(mysqlDatabase):
    tableName = 'source_{}'.format(uuid.uuid4().hex[:8])

    mysqlDatabase.alter('CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR(50), amount INT)'.format(tableName))
    mysqlDatabase.insert(table=tableName, data=[(1, 'alice', 100), (2, 'bob', 200)])

    yield tableName

    mysqlDatabase.alter('DROP TABLE IF EXISTS {}'.format(tableName))


@pytest.fixture
def targetTable(postgresqlDatabase):
    tableName = 'target_{}'.format(uuid.uuid4().hex[:8])

    # amount is VARCHAR here, not INT -- the currency transform below turns it
    # into a formatted string ("$100.00") before it's loaded
    postgresqlDatabase.alter('CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR(50), amount VARCHAR(20))'.format(tableName))

    yield tableName

    postgresqlDatabase.alter('DROP TABLE IF EXISTS {}'.format(tableName))


def test_data_moves_from_mysql_to_postgresql_with_a_transform_applied(postgresqlDatabase, sourceTable, targetTable, tmp_path):
    raw = {
        'workers': 1,
        'jobs': {
            'job1': {
                'active': True, 'sourceDatabase': 'mysql', 'targetDatabase': 'postgresql', 'insertStrategy': 'upsert',
                'chunkSize': 100, 'targetTableFinal': targetTable,
                'sourceQueryColumnTransforms': {'amount': ['understudy_data.builtinTransforms:currency']},
                'sourceQuery': 'select id, name, amount from {} order by id'.format(sourceTable),
                },
            },
        }
    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)
    databaseConfiguration = {'mysql': MYSQL_SETTINGS, 'postgresql': POSTGRESQL_SETTINGS}

    runDataJobs(jobsFile=jobsFile, databaseConfiguration=databaseConfiguration, logFile=tmp_path / 'runner.log',
                memory=FileMemory(memoryFile=tmp_path / 'memory.yaml'), runForever=False)

    rows = postgresqlDatabase.query('SELECT id, name, amount FROM {} ORDER BY id'.format(targetTable))
    assert rows == [(1, 'alice', '$100.00'), (2, 'bob', '$200.00')]
