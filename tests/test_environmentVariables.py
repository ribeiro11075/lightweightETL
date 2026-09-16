"""Keeping credentials out of the YAML that sits beside your job definitions.

Plaintext passwords in a config file is the first thing a security review
objects to, and it was the state of this package until now.
"""
import pytest

from lightweight_etl.configuration import Configuration, ConfigurationError, expandEnvironmentVariables


def test_a_variable_is_replaced_from_the_environment(monkeypatch):
    monkeypatch.setenv('DEMO_PASSWORD', 's3cret')

    assert expandEnvironmentVariables({'password': '${DEMO_PASSWORD}'}) == {'password': 's3cret'}


def test_expansion_reaches_nested_structures(monkeypatch):
    monkeypatch.setenv('DEMO_HOST', 'db.internal')

    raw = {'aliases': [{'host': '${DEMO_HOST}'}], 'nested': {'deep': ['${DEMO_HOST}']}}

    assert expandEnvironmentVariables(raw) == {'aliases': [{'host': 'db.internal'}], 'nested': {'deep': ['db.internal']}}


def test_an_unset_variable_raises_rather_than_expanding_to_empty(monkeypatch):
    """The important safety property. An empty password fails at connect time
    with the driver's own unhelpful message; an empty host silently connects
    somewhere unintended. Refusing to start beats both.
    """
    monkeypatch.delenv('DEMO_MISSING', raising=False)

    with pytest.raises(ConfigurationError, match='DEMO_MISSING'):
        expandEnvironmentVariables({'password': '${DEMO_MISSING}'})


def test_every_missing_variable_is_named_at_once(monkeypatch):
    for name in ('DEMO_A', 'DEMO_B'):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(ConfigurationError) as excinfo:
        expandEnvironmentVariables({'a': '${DEMO_A}', 'b': '${DEMO_B}'})

    assert 'DEMO_A' in str(excinfo.value)
    assert 'DEMO_B' in str(excinfo.value)


def test_a_default_is_used_when_the_variable_is_unset(monkeypatch):
    monkeypatch.delenv('DEMO_PORT', raising=False)

    assert expandEnvironmentVariables({'port': '${DEMO_PORT:-5432}'}) == {'port': '5432'}


def test_the_environment_wins_over_a_default(monkeypatch):
    monkeypatch.setenv('DEMO_PORT', '6543')

    assert expandEnvironmentVariables({'port': '${DEMO_PORT:-5432}'}) == {'port': '6543'}


def test_an_empty_default_is_honoured_rather_than_treated_as_missing(monkeypatch):
    monkeypatch.delenv('DEMO_SCHEMA', raising=False)

    assert expandEnvironmentVariables({'schema': '${DEMO_SCHEMA:-}'}) == {'schema': ''}


def test_a_doubled_dollar_escapes_the_construct(monkeypatch):
    """sourceQuery is arbitrary SQL and may legitimately contain ${...}."""
    monkeypatch.delenv('notAVariable', raising=False)

    assert expandEnvironmentVariables('select $${notAVariable} from t') == 'select ${notAVariable} from t'


def test_postgres_dollar_quoting_is_left_alone():
    """$$body$$ is never followed by a brace, so it must pass through untouched."""
    query = "DO $$ BEGIN PERFORM 1; END $$"

    assert expandEnvironmentVariables(query) == query


def test_non_strings_pass_through_unchanged():
    raw = {'port': 5432, 'active': True, 'refresh': None, 'ratio': 1.5}

    assert expandEnvironmentVariables(raw) == raw


def test_a_whole_database_configuration_can_be_kept_out_of_the_file(monkeypatch):
    monkeypatch.setenv('PROD_PASSWORD', 'hunter2')
    monkeypatch.delenv('PROD_PORT', raising=False)

    raw = {'prod': {'type': 'postgresql', 'database': 'app', 'host': 'db.internal',
                     'user': 'etl', 'password': '${PROD_PASSWORD}', 'port': '${PROD_PORT:-5432}'}}

    configuration = Configuration.validateDatabaseConfiguration(expandEnvironmentVariables(raw))

    assert configuration['prod'].password == 'hunter2'
    assert configuration['prod'].port == 5432
