import pytest
from pydantic import ValidationError

from understudy_data.configuration import (
    Configuration,
    ConfigurationError,
    DatabaseConnectionConfig,
    DataJobsFile,
    findCycle,
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


def _job(**overrides):
    fields = dict(active=True, sourceDatabase='a', sourceQuery='select 1', targetDatabase='a', targetTableFinal='t',
                  insertStrategy='upsert', chunkSize=10)
    fields.update(overrides)
    return fields


def test_a_predecessor_cycle_is_rejected_rather_than_run_forever():
    """Every job in a cycle waits for another that waits for it, so a run
    containing one could never finish. It has to fail validation instead.
    """
    raw = {'workers': 1, 'jobs': {'first': _job(predecessors=['second']), 'second': _job(predecessors=['first']), 'third': _job()}}
    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)

    with pytest.raises(ConfigurationError, match='cycle.*first -> second -> first'):
        Configuration.validateJobGraph(jobsFile.jobs, databaseAliases={'a'})


def test_a_job_that_is_its_own_predecessor_is_a_cycle():
    raw = {'workers': 1, 'jobs': {'only': _job(predecessors=['only'])}}
    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)

    with pytest.raises(ConfigurationError, match='only -> only'):
        Configuration.validateJobGraph(jobsFile.jobs)


def test_find_cycle_accepts_a_diamond_and_ignores_unknown_predecessors():
    assert findCycle({'a': [], 'b': ['a'], 'c': ['a'], 'd': ['b', 'c', 'missing']}) is None


def test_workers_must_be_positive():
    with pytest.raises(ConfigurationError, match='workers'):
        Configuration.validateJobConfiguration({'workers': 0, 'jobs': {}}, DataJobsFile)


def test_a_swap_stage_table_must_share_the_targets_schema():
    """A rename never moves a table between schemas, so the three-way rename
    would fail part-way.
    """
    raw = {'workers': 1, 'jobs': {'j': _job(insertStrategy='swap', targetTableFinal='sales.orders', targetTableStage='staging.orders')}}

    with pytest.raises(ConfigurationError, match='same schema'):
        Configuration.validateJobConfiguration(raw, DataJobsFile)


@pytest.mark.parametrize('final,stage', [('orders', 'orders_stage'), ('sales.orders', 'SALES.orders_stage')])
def test_a_swap_stage_table_in_the_same_schema_is_accepted(final, stage):
    raw = {'workers': 1, 'jobs': {'j': _job(insertStrategy='swap', targetTableFinal=final, targetTableStage=stage)}}

    Configuration.validateJobConfiguration(raw, DataJobsFile)


def test_a_password_never_appears_in_the_models_repr():
    settings = DatabaseConnectionConfig(type='postgresql', user='u', password='hunter2', database='d', host='h')

    assert 'hunter2' not in repr(settings)
    assert 'hunter2' not in str(settings.model_dump())
    assert settings.plainPassword() == 'hunter2'


def _watermarkJob(**overrides):
    fields = dict(active=True, sourceDatabase='src', targetDatabase='tgt', insertStrategy='upsert', chunkSize=100,
                  targetTableFinal='orders', sourceQuery='select id, updated_at from orders where updated_at > {{ watermark }}',
                  watermarkColumn='updated_at', watermarkInitial='1970-01-01')
    fields.update(overrides)
    return {'workers': 1, 'jobs': {'loadOrders': fields}}


def test_a_watermark_job_validates():
    jobsFile = Configuration.validateJobConfiguration(_watermarkJob(), DataJobsFile)

    assert jobsFile.jobs['loadOrders'].watermarkColumn == 'updated_at'


def test_watermark_column_without_a_placeholder_is_rejected():
    """The column and the token are two halves of one feature: a column with no
    token extracts everything, then advances a watermark nothing filtered on.
    """
    raw = _watermarkJob(sourceQuery='select id, updated_at from orders')

    with pytest.raises(ConfigurationError, match='no {{ watermark }} placeholder'):
        Configuration.validateJobConfiguration(raw, DataJobsFile)


def test_a_placeholder_without_a_watermark_column_is_rejected():
    raw = _watermarkJob(watermarkColumn=None)

    with pytest.raises(ConfigurationError, match='watermarkColumn is not set'):
        Configuration.validateJobConfiguration(raw, DataJobsFile)


def test_watermark_without_an_initial_value_is_rejected():
    raw = _watermarkJob(watermarkInitial=None)

    with pytest.raises(ConfigurationError, match='watermarkInitial is required'):
        Configuration.validateJobConfiguration(raw, DataJobsFile)


def test_watermark_with_swap_is_rejected():
    """The important one: swap replaces the target with the stage contents, so an
    incremental extract would stage only changed rows and delete everything else.
    """
    raw = _watermarkJob(insertStrategy='swap', targetTableStage='orders_stage')

    with pytest.raises(ConfigurationError, match='requires insertStrategy: upsert'):
        Configuration.validateJobConfiguration(raw, DataJobsFile)


@pytest.mark.parametrize('placeholder', ['{{ watermark }}', '{{watermark}}', '{{   watermark   }}'])
def test_placeholder_spacing_is_forgiving(placeholder):
    raw = _watermarkJob(sourceQuery='select id, updated_at from orders where updated_at > {}'.format(placeholder))

    Configuration.validateJobConfiguration(raw, DataJobsFile)


@pytest.mark.parametrize('databaseType', ['postgresql', 'oracle'])
def test_current_schema_is_accepted_where_a_session_can_switch_schema(databaseType):
    settings = DatabaseConnectionConfig(type=databaseType, user='u', password='p', database='d', host='h', serviceName='s',
                                        currentSchema='reporting')

    assert settings.currentSchema == 'reporting'


@pytest.mark.parametrize('databaseType', ['mysql', 'mariadb', 'mssql', 'sqlite'])
def test_current_schema_is_refused_where_it_cannot_be_set(databaseType):
    with pytest.raises(ConfigurationError, match='currentSchema is supported for oracle and postgresql only'):
        Configuration.validateDatabaseConfiguration({'db': {'type': databaseType, 'user': 'u', 'password': 'p', 'database': 'd', 'host': 'h',
                                                            'currentSchema': 'reporting'}})


def test_current_schema_must_be_a_plain_identifier():
    """It is written into a session statement, so nothing but a name gets through."""
    with pytest.raises(ConfigurationError, match='plain identifier'):
        Configuration.validateDatabaseConfiguration({'db': {'type': 'postgresql', 'user': 'u', 'password': 'p', 'database': 'd', 'host': 'h',
                                                            'currentSchema': 'x; drop table t'}})


def test_connection_options_are_kept_out_of_the_models_repr():
    settings = DatabaseConnectionConfig(type='oracle', user='u', password='p', database='d', host='h', serviceName='s',
                                        options={'wallet_password': 'hunter2', 'protocol': 'tcps', 'ignored': None})

    assert settings.options == {'wallet_password': 'hunter2', 'protocol': 'tcps'}
    assert 'hunter2' not in repr(settings)


@pytest.mark.parametrize('timeout', [0, -5])
def test_timeout_seconds_must_be_positive(timeout):
    with pytest.raises(ConfigurationError, match='timeoutSeconds'):
        Configuration.validateJobConfiguration({'workers': 1, 'jobs': {'j': _job(timeoutSeconds=timeout)}}, DataJobsFile)


def _validateJob(**overrides):
    return Configuration.validateJobConfiguration({'workers': 1, 'jobs': {'job': _job(**overrides)}}, DataJobsFile)


@pytest.mark.parametrize('chunkSize', [0, -1])
def test_a_chunk_size_below_one_is_rejected(chunkSize):
    """A chunk of zero rows read nothing, and a swap then replaced the target with it."""
    with pytest.raises(ConfigurationError, match='chunkSize'):
        _validateJob(chunkSize=chunkSize, insertStrategy='swap', targetTableStage='t_stage')


@pytest.mark.parametrize('strategy', ['upsert', 'swap'])
@pytest.mark.parametrize('stage', ['t', 'T'])
def test_a_stage_table_that_is_the_target_is_rejected(strategy, stage):
    """The stage table is emptied first, so it would empty the target."""
    with pytest.raises(ConfigurationError, match='different table from targetTableFinal'):
        _validateJob(insertStrategy=strategy, targetTableStage=stage)


@pytest.mark.parametrize('command', ['', '   ', 'token "unterminated', [], ['  ']])
def test_a_password_command_that_names_no_program_is_rejected(command):
    raw = {'warehouse': {'type': 'postgresql', 'database': 'w', 'host': 'h', 'user': 'u', 'passwordCommand': command}}

    with pytest.raises(ConfigurationError, match='passwordCommand'):
        Configuration.validateDatabaseConfiguration(raw)


def test_running_a_malformed_password_command_is_a_configuration_error():
    from understudy_data.configuration import runPasswordCommand

    for command in ('  ', 'echo "x'):
        with pytest.raises(ConfigurationError, match='passwordCommand'):
            runPasswordCommand(command)
