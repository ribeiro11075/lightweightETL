"""Exercises the CLI through main(argv) rather than a subprocess: the same code
path the console script takes, without paying process startup per assertion.

The exit codes are the point. For anything that schedules work the exit code is
the entire interface, and runDataJobs used to return None -- so a cron wrapping
this reported success on total failure.
"""
import sqlite3

import pytest

from lightweight_etl.cli import EXIT_BAD_CONFIGURATION, EXIT_JOBS_DID_NOT_SUCCEED, EXIT_SUCCESS, main

JOBS_YAML = """workers: 1
jobs:
  loadRows:
    active: true
    sourceDatabase: demo
    sourceQuery: SELECT id, name FROM src
    targetDatabase: demo
    targetTableFinal: tgt
    insertStrategy: upsert
    chunkSize: 2
  dependent:
    active: true
    predecessors:
    - loadRows
    sourceDatabase: demo
    sourceQuery: SELECT id, name FROM src
    targetDatabase: demo
    targetTableFinal: tgt
    insertStrategy: upsert
    chunkSize: 2
"""


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    """A complete runnable deployment: a seeded sqlite database plus the two YAML
    files the CLI's default config directory layout expects.
    """
    configuration = tmp_path / 'configuration'
    configuration.mkdir()

    connection = sqlite3.connect(str(tmp_path / 'demo.db'))
    connection.execute('CREATE TABLE src (id INT PRIMARY KEY, name TEXT)')
    connection.execute('CREATE TABLE tgt (id INT PRIMARY KEY, name TEXT)')
    connection.executemany('INSERT INTO src VALUES (?, ?)', [(index, 'name{}'.format(index)) for index in range(5)])
    connection.commit()
    connection.close()

    (configuration / 'database.yaml').write_text('demo:\n  type: sqlite\n  database: demo.db\n')
    (configuration / 'jobs.yaml').write_text(JOBS_YAML)

    monkeypatch.chdir(tmp_path)

    return tmp_path


def _targetRowCount(workspace) -> int:
    connection = sqlite3.connect(str(workspace / 'demo.db'))
    try:
        return int(connection.execute('SELECT count(*) FROM tgt').fetchone()[0])
    finally:
        connection.close()


def test_validate_passes_on_good_configuration(workspace, capsys):
    assert main(['validate', '--quiet']) == EXIT_SUCCESS
    assert 'configuration is valid' in capsys.readouterr().out


def test_validate_is_offline(workspace, monkeypatch):
    """It must not open a connection -- that's what makes it safe in CI and in a
    pre-commit hook. `run --dry-run` is the online counterpart.
    """
    def explode(*args, **kwargs):
        raise AssertionError('validate must not connect to anything')

    monkeypatch.setattr('lightweight_etl.cli.Database', explode)

    assert main(['validate', '--quiet']) == EXIT_SUCCESS


def test_validate_rejects_an_unresolvable_transformer_reference(workspace):
    """Transformers resolve inside a worker at job-run time, so a typo otherwise
    surfaces as a failed job in a log file at 3am. It's statically checkable.
    """
    jobs = workspace / 'configuration' / 'jobs.yaml'
    jobs.write_text(JOBS_YAML.replace(
        '    chunkSize: 2\n  dependent:',
        '    chunkSize: 2\n    sourceQueryColumnTransforms:\n      name:\n      - no_such_module:nope\n  dependent:', 1))

    assert main(['validate', '--quiet']) == EXIT_BAD_CONFIGURATION


def test_run_moves_rows_and_exits_zero(workspace):
    assert main(['run', '--quiet']) == EXIT_SUCCESS
    assert _targetRowCount(workspace) == 5


def test_a_failing_job_exits_nonzero(workspace):
    """The whole reason for RunResult. This used to exit 0."""
    (workspace / 'configuration' / 'jobs.yaml').write_text(JOBS_YAML.replace('FROM src', 'FROM no_such_table'))

    assert main(['run', '--quiet']) == EXIT_JOBS_DID_NOT_SUCCEED


def test_a_skipped_job_also_exits_nonzero(workspace):
    """`dependent` never runs, because its predecessor failed. It didn't error,
    but the data isn't there, so reporting success would be a lie cron acts on.
    """
    (workspace / 'configuration' / 'jobs.yaml').write_text(
        JOBS_YAML.replace('    sourceQuery: SELECT id, name FROM src\n', '    sourceQuery: SELECT id, name FROM no_such_table\n', 1))

    assert main(['run', '--quiet']) == EXIT_JOBS_DID_NOT_SUCCEED


def test_a_missing_configuration_file_is_a_usage_error(workspace):
    assert main(['run', '--quiet', '--jobs', 'nope.yaml']) == EXIT_BAD_CONFIGURATION


def test_invalid_configuration_exits_two(workspace):
    (workspace / 'bad.yaml').write_text('workers: not-a-number\n')

    assert main(['run', '--quiet', '--jobs', 'bad.yaml']) == EXIT_BAD_CONFIGURATION


def test_an_unknown_job_name_is_a_usage_error(workspace):
    assert main(['run', '--quiet', '--job', 'nosuchjob']) == EXIT_BAD_CONFIGURATION


def test_job_selection_runs_only_what_was_asked_for(workspace, caplog):
    """Running only the named job is the right default -- the use case is a fast
    iteration loop -- but a silently ignored dependency is how a --job in a cron
    causes a stale-upstream incident, so it warns and names them.
    """
    assert main(['run', '--job', 'dependent']) == EXIT_SUCCESS
    assert 'without its predecessor(s): loadRows' in caplog.text


def test_dry_run_checks_connectivity_without_moving_rows(workspace, capsys):
    assert main(['run', '--quiet', '--dry-run']) == EXIT_SUCCESS
    assert 'no rows moved' in capsys.readouterr().out
    assert _targetRowCount(workspace) == 0


def test_dry_run_reports_an_upsert_target_with_no_primary_key(workspace):
    """A real trap: without a key the column buckets come back empty and the
    generated upsert degrades rather than failing loudly.
    """
    connection = sqlite3.connect(str(workspace / 'demo.db'))
    connection.execute('CREATE TABLE keyless (id INT, name TEXT)')
    connection.commit()
    connection.close()

    (workspace / 'configuration' / 'jobs.yaml').write_text(JOBS_YAML.replace('targetTableFinal: tgt', 'targetTableFinal: keyless'))

    assert main(['run', '--quiet', '--dry-run']) == EXIT_JOBS_DID_NOT_SUCCEED


def test_dry_run_reports_a_database_it_cannot_reach(workspace):
    (workspace / 'configuration' / 'database.yaml').write_text(
        'demo:\n  type: postgresql\n  database: d\n  user: u\n  password: p\n  host: 127.0.0.1\n  port: 1\n')

    assert main(['run', '--quiet', '--dry-run']) == EXIT_JOBS_DID_NOT_SUCCEED


def test_jobs_lists_the_graph_and_which_jobs_are_due(workspace, capsys):
    assert main(['jobs', '--quiet']) == EXIT_SUCCESS

    output = capsys.readouterr().out

    assert 'loadRows' in output
    assert 'dependent' in output
    assert '2 of 2 job(s) due' in output


def test_jobs_shows_a_throttled_job_after_it_has_run(workspace, capsys):
    (workspace / 'configuration' / 'jobs.yaml').write_text(
        JOBS_YAML.replace('  loadRows:\n    active: true\n', '  loadRows:\n    active: true\n    refresh: 60\n', 1))

    main(['run', '--quiet'])
    capsys.readouterr()
    main(['jobs', '--quiet'])

    assert 'throttled' in capsys.readouterr().out


def test_the_cli_expands_environment_variables_in_configuration(workspace, monkeypatch):
    """Credentials belong in the environment, not in the file beside the jobs."""
    monkeypatch.setenv('DEMO_DB_PATH', 'demo.db')
    (workspace / 'configuration' / 'database.yaml').write_text('demo:\n  type: sqlite\n  database: ${DEMO_DB_PATH}\n')

    assert main(['run', '--quiet']) == EXIT_SUCCESS
    assert _targetRowCount(workspace) == 5


def test_an_unset_variable_stops_the_run_before_anything_happens(workspace, monkeypatch):
    monkeypatch.delenv('DEMO_DB_PATH', raising=False)
    (workspace / 'configuration' / 'database.yaml').write_text('demo:\n  type: sqlite\n  database: ${DEMO_DB_PATH}\n')

    assert main(['run', '--quiet']) == EXIT_BAD_CONFIGURATION
    assert _targetRowCount(workspace) == 0


def test_json_log_format_produces_parseable_records(workspace, capsys):
    import json

    assert main(['run', '--log-format', 'json']) == EXIT_SUCCESS

    records = [json.loads(line) for line in capsys.readouterr().err.strip().split('\n') if line.startswith('{')]
    completions = [record for record in records if record.get('status') == 'completed']

    assert completions
    assert all('job' in record and 'rowCount' in record for record in completions)
