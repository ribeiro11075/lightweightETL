"""Keeping credentials out of the YAML that sits beside your job definitions.

Plaintext passwords in a config file is the first thing a security review
objects to, and it was the state of this package until now.
"""
import pytest

from understudy_data.configuration import Configuration, ConfigurationError, expandEnvironmentVariables


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

    assert configuration['prod'].plainPassword() == 'hunter2'
    assert configuration['prod'].port == 5432


def test_a_file_reference_reads_the_file_without_its_trailing_newline(tmp_path):
    """How Docker and Kubernetes mount secrets, and how the Vault agent writes them."""
    secret = tmp_path / 'db-password'
    secret.write_text('s3cret\n')

    assert expandEnvironmentVariables({'password': '${file:' + str(secret) + '}'}) == {'password': 's3cret'}


def test_an_unreadable_file_is_reported_with_the_missing_variables(tmp_path, monkeypatch):
    monkeypatch.delenv('DEMO_MISSING', raising=False)
    missing = tmp_path / 'nope'

    with pytest.raises(ConfigurationError) as excinfo:
        expandEnvironmentVariables({'a': '${file:' + str(missing) + '}', 'b': '${DEMO_MISSING}'})

    assert 'file {}'.format(missing) in str(excinfo.value)
    assert '$DEMO_MISSING' in str(excinfo.value)


def test_a_file_reference_can_be_escaped(tmp_path):
    assert expandEnvironmentVariables('$${file:/etc/passwd}') == '${file:/etc/passwd}'


def _connection(**overrides):
    from understudy_data.configuration import DatabaseConnectionConfig

    fields = dict(type='postgresql', user='u', database='d', host='h')
    fields.update(overrides)
    return DatabaseConnectionConfig(**fields)


def test_a_password_command_supplies_the_password_at_connect_time(tmp_path):
    import sys

    settings = _connection(passwordCommand=[sys.executable, '-c', 'print("token-123")'])

    assert settings.password is None
    assert settings.plainPassword() == 'token-123'


def test_a_password_command_may_be_one_string(tmp_path):
    import shlex
    import sys

    settings = _connection(passwordCommand='{} -c "print(\'tok en\')"'.format(shlex.quote(sys.executable)))

    assert settings.plainPassword() == 'tok en'


@pytest.mark.parametrize('script,message', [
    ('import sys; sys.stderr.write("denied"); sys.exit(3)', 'exited with status 3: denied'),
    ('pass', 'printed nothing'),
    ])
def test_a_failing_password_command_raises_without_revealing_output(script, message):
    import sys
    from understudy_data.configuration import PasswordCommandError

    with pytest.raises(PasswordCommandError, match=message):
        _connection(passwordCommand=[sys.executable, '-c', script]).plainPassword()


def test_a_missing_password_command_raises_a_retryable_error():
    from understudy_data.configuration import PasswordCommandError

    with pytest.raises(PasswordCommandError, match='could not run'):
        _connection(passwordCommand=['/no/such/command']).plainPassword()


def test_password_and_password_command_are_exclusive():
    with pytest.raises(ValueError, match='not both'):
        _connection(password='p', passwordCommand=['true'])


def test_a_network_database_needs_a_password_or_a_command():
    with pytest.raises(ValueError, match='a password or passwordCommand'):
        _connection()


def test_checking_options_never_runs_the_password_command():
    from understudy_data.database import DIALECTS
    from understudy_data.configuration import DatabaseType

    settings = _connection(passwordCommand=['/no/such/command'], options={'sslmode': 'require'})

    assert DIALECTS[DatabaseType.POSTGRESQL].connectArguments(settings, resolvePassword=False)['sslmode'] == 'require'
