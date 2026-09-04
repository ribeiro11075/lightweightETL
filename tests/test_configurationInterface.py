import pytest
from pydantic import ValidationError

from library.configurationInterface import (
    Configuration,
    ConfigurationError,
    DatabaseConnectionConfig,
    DataJobsFile,
    ScrambleJobsFile,
    )


def test_database_configuration_round_trips():
    raw = {
        'sourceDb': {'type': 'mysql', 'user': 'u', 'password': 'p', 'database': 'd', 'host': 'h', 'port': 3306},
        'targetDb': {'type': 'postgresql', 'user': 'u', 'password': 'p', 'database': 'd', 'host': 'h', 'port': 5432},
        }

    result = Configuration.validateDatabaseConfiguration(raw)

    assert set(result.keys()) == {'sourceDb', 'targetDb'}
    assert isinstance(result['sourceDb'], DatabaseConnectionConfig)


def test_database_configuration_rejects_unknown_type():
    with pytest.raises(ConfigurationError):
        Configuration.validateDatabaseConfiguration({'db': {'type': 'mongodb', 'user': 'u', 'password': 'p', 'database': 'd', 'host': 'h'}})


def test_sqlite_connection_needs_only_a_database_path():
    settings = DatabaseConnectionConfig(type='sqlite', database='/tmp/some.db')

    assert settings.user is None
    assert settings.password is None
    assert settings.host is None


@pytest.mark.parametrize('missingField', ['user', 'password', 'host'])
def test_non_sqlite_connections_require_network_credentials(missingField):
    kwargs = dict(type='mysql', user='u', password='p', database='d', host='h')
    kwargs[missingField] = None

    with pytest.raises(ValidationError):
        DatabaseConnectionConfig(**kwargs)


@pytest.mark.parametrize('serviceName,sid,shouldRaise', [
    ('svc', None, False),
    (None, 'sid1', False),
    ('svc', 'sid1', True),
    (None, None, True),
    ])
def test_oracle_requires_exactly_one_of_service_name_or_sid(serviceName, sid, shouldRaise):
    kwargs = dict(type='oracle', user='u', password='p', database='d', host='h', serviceName=serviceName, sid=sid)

    if shouldRaise:
        with pytest.raises(ValidationError):
            DatabaseConnectionConfig(**kwargs)
    else:
        DatabaseConnectionConfig(**kwargs)


def test_swap_strategy_requires_target_table_stage():
    raw = {
        'workers': 1,
        'jobs': {
            'job1': {
                'active': True, 'sourceDatabase': 'a', 'targetDatabase': 'b', 'insertStrategy': 'swap',
                'chunkSize': 100, 'targetTableFinal': 't', 'sourceQuery': 'select 1',
                },
            },
        }

    with pytest.raises(ConfigurationError):
        Configuration.validateJobConfiguration(raw, DataJobsFile)


def test_upsert_strategy_does_not_require_target_table_stage():
    raw = {
        'workers': 1,
        'jobs': {
            'job1': {
                'active': True, 'sourceDatabase': 'a', 'targetDatabase': 'b', 'insertStrategy': 'upsert',
                'chunkSize': 100, 'targetTableFinal': 't', 'sourceQuery': 'select 1',
                },
            },
        }

    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)
    assert jobsFile.jobs['job1'].targetTableStage is None


def test_validate_job_graph_catches_unknown_predecessor():
    raw = {'workers': 1, 'jobs': {'job1': {'active': True, 'predecessors': ['doesNotExist'], 'sourceDatabase': 'a',
                                            'targetDatabase': 'b', 'insertStrategy': 'upsert', 'chunkSize': 1,
                                            'targetTableFinal': 't', 'sourceQuery': 'select 1'}}}
    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)

    with pytest.raises(ConfigurationError, match='doesNotExist'):
        Configuration.validateJobGraph(jobsFile.jobs)


def test_validate_job_graph_catches_unknown_database_alias():
    raw = {'workers': 1, 'jobs': {'job1': {'active': True, 'sourceDatabase': 'ghost', 'targetDatabase': 'b',
                                            'insertStrategy': 'upsert', 'chunkSize': 1, 'targetTableFinal': 't',
                                            'sourceQuery': 'select 1'}}}
    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)

    with pytest.raises(ConfigurationError, match='ghost'):
        Configuration.validateJobGraph(jobsFile.jobs, databaseAliases={'b'})


def test_validate_job_graph_passes_for_valid_config():
    raw = {'workers': 1, 'jobs': {'job1': {'active': True, 'sourceDatabase': 'a', 'targetDatabase': 'b',
                                            'insertStrategy': 'upsert', 'chunkSize': 1, 'targetTableFinal': 't',
                                            'sourceQuery': 'select 1'}}}
    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)

    Configuration.validateJobGraph(jobsFile.jobs, databaseAliases={'a', 'b'})


@pytest.mark.parametrize('field,rawValue', [
    ('predecessors', [None]),
    ('preTargetAdhocQueries', None),
    ])
def test_yaml_null_list_idiom_is_treated_as_empty(field, rawValue):
    """YAML's "key:\\n-\\n" idiom parses to [None]; a bare omitted key parses to
    None. Both should become an empty list rather than a validation error.
    """
    raw = {'workers': 1, 'jobs': {'job1': {
        'active': True, 'sourceDatabase': 'a', 'targetDatabase': 'b', 'insertStrategy': 'upsert', 'chunkSize': 1,
        'targetTableFinal': 't', 'sourceQuery': 'select 1', field: rawValue,
        }}}

    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)

    assert getattr(jobsFile.jobs['job1'], field) == []


def test_cycle_sleep_seconds_defaults_and_is_configurable():
    raw = {'workers': 1, 'jobs': {}}
    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)
    assert jobsFile.cycleSleepSeconds == 0.5

    raw = {'workers': 1, 'cycleSleepSeconds': 5, 'jobs': {}}
    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)
    assert jobsFile.cycleSleepSeconds == 5


def test_default_column_values_scalar_survives_cleanup():
    raw = {'workers': 1, 'jobs': {'job1': {
        'active': True, 'database': 'a', 'table': 't', 'randomSalt': 's',
        'defaultColumnValues': {'status': 'active', 'ignored': None},
        }}}

    jobsFile = Configuration.validateJobConfiguration(raw, ScrambleJobsFile)

    assert jobsFile.jobs['job1'].defaultColumnValues == {'status': 'active'}
