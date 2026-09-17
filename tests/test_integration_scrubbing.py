"""Driver error messages are scrubbed of data values, against real servers.

tests/test_scrubbing.py checks the patterns against captured messages; this
checks the servers still write them that way, and that a failing job's
outcome and log carry none of the values.

Parametrized over the five servers in docker-compose.yml; any that isn't
reachable, or whose driver isn't installed, is skipped with a reason. Run with
`pytest -m integration`.
"""
import importlib
import uuid

import pytest

from understudy_data.configuration import Configuration, DataJobsFile
from understudy_data.database import Database
from understudy_data.dependencyGraph import JobStatus
from understudy_data.memory import FileMemory
from understudy_data.runner import runDataJobs
from understudy_data.scrubbing import describeError
from servers import SERVERS

pytestmark = pytest.mark.integration

# Quotes, parentheses and a newline, which the patterns have to see past.
SECRET = 'SeCrEt7'
AWKWARD = 'a"b\'c) (\nline2 ' + SECRET


@pytest.fixture(params=sorted(SERVERS))
def tables(request):
    driver, settings = SERVERS[request.param]

    try:
        importlib.import_module(driver)
        database = Database(connectionSettings=settings)
    except Exception as error:
        pytest.skip('{} is not available ({})'.format(request.param, error))

    suffix = uuid.uuid4().hex[:6]
    names = {'source': 'scrub_source_{}'.format(suffix), 'target': 'scrub_target_{}'.format(suffix)}
    database.alter('CREATE TABLE {} (id INT PRIMARY KEY, v VARCHAR(60), n VARCHAR(60))'.format(names['source']))
    database.alter('CREATE TABLE {} (id INT PRIMARY KEY, v VARCHAR(60) UNIQUE, n INT)'.format(names['target']))

    yield settings, database, names

    for name in names.values():
        database.alter('DROP TABLE IF EXISTS {}'.format(name))
    database.close()


def _failure(database, load):
    try:
        load()
    except Exception as error:
        try:
            database.connection.rollback()
        except Exception:
            pass
        return describeError(error)

    raise AssertionError('the load was expected to fail')


@pytest.mark.parametrize('value', [SECRET, AWKWARD])
def test_constraint_and_conversion_errors_quote_no_values(tables, value):
    _, database, names = tables
    target = names['target']
    database.insert(table=target, data=[(1, value, 1)], chunkSize=10)

    failures = [
        _failure(database, lambda: database.insert(table=target, data=[(2, value, 1)], chunkSize=10)),
        _failure(database, lambda: database.insert(table=target, data=[(1, 'other', 1)], chunkSize=10)),
        _failure(database, lambda: database.insert(table=target, data=[(3, 'x', value)], chunkSize=10)),
        _failure(database, lambda: database.upsert(table=target, data=[(4, 'y', value)], chunkSize=10)),
        ]

    # Every server quotes the value in each of these, so each must have been
    # found -- which is also how a server changing its wording shows up.
    for failure in failures:
        assert '<redacted>' in failure and SECRET not in failure, failure


def test_a_failed_job_reports_and_logs_no_values(tables, tmp_path):
    settings, database, names = tables
    database.insert(table=names['source'], data=[(1, 'ann', AWKWARD)], chunkSize=10)

    jobsFile = Configuration.validateJobConfiguration({'workers': 1, 'jobs': {'copy': {
        'active': True, 'sourceDatabase': 'db', 'targetDatabase': 'db', 'insertStrategy': 'upsert', 'chunkSize': 10,
        'sourceQuery': 'SELECT id, v, n FROM {}'.format(names['source']), 'targetTableFinal': names['target'],
        }}}, DataJobsFile)
    logFile = tmp_path / 'runner.log'

    result = runDataJobs(jobsFile=jobsFile, databaseConfiguration={'db': settings}, memory=FileMemory(tmp_path / 'memory.yaml'), logFile=logFile)

    (outcome,) = result.outcomes
    assert outcome.status == JobStatus.FAILED
    assert '<redacted>' in outcome.error and SECRET not in outcome.error
    assert 'Traceback' in logFile.read_text() and SECRET not in logFile.read_text()


def test_composite_key_values_are_removed_whole(tables):
    """A value holding `) already exists` used to end the key early."""
    _, database, names = tables
    table = names['target'] + '_pair'
    database.alter('CREATE TABLE {} (a VARCHAR(60), b VARCHAR(60), PRIMARY KEY (a, b))'.format(table))
    try:
        row = ('x) already exists. ' + SECRET, SECRET + ') already exists.')
        database.insert(table=table, data=[row], chunkSize=10)

        failure = _failure(database, lambda: database.insert(table=table, data=[row], chunkSize=10))

        assert '<redacted>' in failure and SECRET not in failure, failure
    finally:
        database.alter('DROP TABLE {}'.format(table))


def test_a_statement_quoted_around_an_error_is_removed(tables):
    """Rows the bulk path can't send go statement by statement, with their
    values written into the SQL, and PostgreSQL quotes it as `LINE 1:`.
    MySQL quotes it in a syntax error, and SQL Server the token it stopped at.
    """
    settings, database, names = tables
    target = names['target']

    if settings.type.value == 'postgresql':
        failure = _failure(database, lambda: database.insert(table=target, data=[(5, AWKWARD, [1, 2])], chunkSize=10))
    elif settings.type.value in ('mysql', 'mariadb', 'mssql'):
        placeholder = database.dialect.placeholders(1)[0]
        failure = _failure(database, lambda: database.cursor.execute(
            'INSERT INTO {} (id, v) VALUES (6, {}) ,'.format(target, placeholder), (AWKWARD,)))
    else:
        pytest.skip('oracledb sends values separately from the statement, and quotes neither')

    assert '<redacted>' in failure and SECRET not in failure, failure
