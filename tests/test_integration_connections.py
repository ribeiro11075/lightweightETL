"""Connection options, encryption and currentSchema against real servers.

What a driver does with an option can only be seen from the server: that it
arrived, that a connection really is encrypted, that a session really resolves
names in the schema it was given. Each test asks the server.

Parametrized over the servers in docker-compose.yml where it applies; any that
isn't reachable, or whose driver isn't installed, is skipped with a reason. Run
with `pytest -m integration`.
"""
import importlib
import os
import subprocess
import sys
import textwrap
import uuid

import pytest

from understudy_data.database import Database
from servers import SERVERS

pytestmark = pytest.mark.integration


def _connect(name: str, **changes):
    driver, settings = SERVERS[name]
    try:
        importlib.import_module(driver)
        return Database(connectionSettings=settings.model_copy(update=changes))
    except ImportError as error:
        pytest.skip('{} is not available ({})'.format(name, error))


def _requireServer(name: str) -> None:
    try:
        _connect(name).close()
    except Exception as error:
        pytest.skip('{} is not available ({})'.format(name, error))


@pytest.mark.parametrize('name', ['mysql', 'mariadb'])
def test_mysql_tls_is_on_by_default_and_options_can_turn_it_off(name):
    _requireServer(name)

    with _connect(name) as database:
        assert database.isEncrypted() is True

    with _connect(name, options={'ssl_disabled': True}) as database:
        assert database.isEncrypted() is False


def test_postgresql_options_reach_libpq():
    _requireServer('postgresql')

    with _connect('postgresql', options={'application_name': 'understudy-test', 'sslmode': 'disable'}) as database:
        assert database.query("SELECT current_setting('application_name')") == [('understudy-test',)]
        assert database.isEncrypted() is False

    # The compose server has no certificate, so requiring TLS must fail
    # rather than quietly connecting in the clear.
    with pytest.raises(Exception, match='SSL'):
        _connect('postgresql', options={'sslmode': 'require'})


def test_oracle_options_reach_the_driver():
    _requireServer('oracle')

    with _connect('oracle', options={'program': 'understudy-test'}) as database:
        assert database.query("SELECT program FROM v$session WHERE sid = SYS_CONTEXT('USERENV', 'SID')") == [('understudy-test',)]
        assert database.isEncrypted() is False


def test_mssql_encrypts_when_freetds_is_configured_to(tmp_path):
    """pymssql's own `encryption` argument had no effect in testing, so the
    documented route is FreeTDS's configuration file. FreeTDS reads it when
    the process starts using it, hence a separate process.
    """
    _requireServer('mssql')

    (tmp_path / 'freetds.conf').write_text('[global]\n\tencryption = require\n')
    script = textwrap.dedent('''
        from servers import SERVERS
        from understudy_data.database import Database
        with Database(SERVERS['mssql'][1]) as database:
            print(database.isEncrypted())
        ''')
    repository = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    environment = dict(os.environ, FREETDSCONF=str(tmp_path / 'freetds.conf'),
                       PYTHONPATH=os.pathsep.join([repository, os.path.join(repository, 'tests')]))

    result = subprocess.run([sys.executable, '-c', script], env=environment, capture_output=True, text=True, timeout=120)

    assert result.stdout.strip() == 'True', result.stderr


# How to create and drop another schema, per server that supports currentSchema.
SCHEMAS = {
    'postgresql': ('CREATE SCHEMA {0}', 'DROP SCHEMA {0} CASCADE'),
    'oracle': ('CREATE USER {0} IDENTIFIED BY "Pw{0}" QUOTA UNLIMITED ON USERS', 'DROP USER {0} CASCADE'),
    }


@pytest.mark.parametrize('name', sorted(SCHEMAS))
def test_current_schema_decides_where_unqualified_names_resolve(name):
    _requireServer(name)
    schema = 'current_{}'.format(uuid.uuid4().hex[:6])
    create, drop = SCHEMAS[name]

    with _connect(name) as admin:
        admin.alter(create.format(schema))

        try:
            with _connect(name, currentSchema=schema) as database:
                database.alter('CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR(20))')
                database.upsert(table='people', data=[(1, 'Ann')])
                database.upsert(table='people', data=[(1, 'Bo')])

                assert [column.lower() for column in database.getPrimaryColumnNames('people')] == ['id']
                assert database.tableExists('people')

            assert admin.query('SELECT id, name FROM {}.people'.format(schema)) == [(1, 'Bo')]
            assert not admin.tableExists('people')
        finally:
            admin.alter(drop.format(schema))
