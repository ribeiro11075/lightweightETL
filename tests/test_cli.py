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


MASKED_JOBS_YAML = """workers: 1
jobs:
  maskRows:
    active: true
    sourceDatabase: demo
    sourceQuery: SELECT id, name FROM src
    targetDatabase: demo
    targetTableFinal: tgt
    insertStrategy: upsert
    chunkSize: 2
    masking:
      key: ${MASKING_KEY}
      columns:
        id: keep
        name: hash
"""


@pytest.fixture
def maskedWorkspace(workspace, monkeypatch):
    monkeypatch.setenv('MASKING_KEY', 'a-cli-test-masking-key')
    (workspace / 'configuration' / 'jobs.yaml').write_text(MASKED_JOBS_YAML)

    return workspace


def test_run_writes_a_masking_manifest(maskedWorkspace):
    import json

    assert main(['run', '--quiet', '--manifest', 'out/manifest.json']) == EXIT_SUCCESS

    manifest = json.loads((maskedWorkspace / 'out' / 'manifest.json').read_text())
    [entry] = manifest['jobs']

    assert entry['job'] == 'maskRows'
    assert entry['status'] == 'completed'
    assert entry['rowCount'] == 5
    assert [column['strategy'] for column in entry['columns']] == ['keep', 'hash']
    assert 'a-cli-test-masking-key' not in json.dumps(manifest)


def test_a_failed_masked_run_still_writes_its_manifest(maskedWorkspace):
    import json

    (maskedWorkspace / 'configuration' / 'jobs.yaml').write_text(MASKED_JOBS_YAML.replace('        name: hash\n', ''))

    assert main(['run', '--quiet', '--manifest', 'manifest.json']) == EXIT_JOBS_DID_NOT_SUCCEED
    assert json.loads((maskedWorkspace / 'manifest.json').read_text())['jobs'][0]['status'] == 'failed'
    assert _targetRowCount(maskedWorkspace) == 0


def test_validate_rejects_a_masking_key_that_is_too_short(maskedWorkspace, monkeypatch, caplog):
    monkeypatch.setenv('MASKING_KEY', 'short')

    assert main(['validate', '--quiet']) == EXIT_BAD_CONFIGURATION
    assert 'at least 16 characters' in caplog.text


def test_dry_run_reports_a_column_the_masking_policy_does_not_cover(maskedWorkspace, caplog):
    (maskedWorkspace / 'configuration' / 'jobs.yaml').write_text(MASKED_JOBS_YAML.replace('        name: hash\n', ''))

    assert main(['run', '--quiet', '--dry-run']) == EXIT_JOBS_DID_NOT_SUCCEED
    assert 'not in the masking policy: name' in caplog.text
    assert _targetRowCount(maskedWorkspace) == 0


def test_dry_run_passes_a_complete_masking_policy(maskedWorkspace, capsys):
    assert main(['run', '--quiet', '--dry-run']) == EXIT_SUCCESS
    assert 'no rows moved' in capsys.readouterr().out


def test_scramble_warns_that_it_is_deprecated(workspace, caplog):
    (workspace / 'configuration' / 'scramble.yaml').write_text(
        'workers: 1\njobs:\n  s:\n    active: false\n    database: demo\n    table: tgt\n    randomSalt: salt\n')

    with pytest.warns(DeprecationWarning):
        assert main(['scramble', '--quiet']) == EXIT_SUCCESS

    assert 'deprecated' in caplog.text


@pytest.fixture
def schemaWorkspace(workspace):
    connection = sqlite3.connect(str(workspace / 'demo.db'))
    connection.executescript('''
        CREATE TABLE customers (id INT PRIMARY KEY, email TEXT, tier TEXT);
        CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT REFERENCES customers(id), total INT);
        INSERT INTO customers VALUES (1, 'a@corp.com', 'gold'), (2, 'b@corp.com', 'basic');
        INSERT INTO orders VALUES (10, 1, 5), (11, 2, 7);
        ''')
    connection.commit()
    connection.close()

    (workspace / 'configuration' / 'database.yaml').write_text(
        'demo:\n  type: sqlite\n  database: demo.db\ncopy:\n  type: sqlite\n  database: copy.db\n')

    return workspace


def test_discover_proposes_a_policy_without_printing_values(schemaWorkspace, capsys):
    assert main(['discover', '--quiet', '--database', 'demo', '--table', 'customers', '--table', 'orders', '--target', 'copy']) == EXIT_SUCCESS

    output = capsys.readouterr().out

    assert 'email: {strategy: email}  # name suggests an email address' in output
    assert 'maskOrders:\n    active: true\n    predecessors:\n    - maskCustomers' in output
    assert 'a@corp.com' not in output


def test_discover_refuses_to_overwrite_a_file(schemaWorkspace):
    (schemaWorkspace / 'proposal.yaml').write_text('reviewed\n')

    assert main(['discover', '--quiet', '--database', 'demo', '--table', 'customers', '--output', 'proposal.yaml']) == EXIT_BAD_CONFIGURATION
    assert (schemaWorkspace / 'proposal.yaml').read_text() == 'reviewed\n'


def test_discover_rejects_an_unknown_alias(schemaWorkspace):
    assert main(['discover', '--quiet', '--database', 'nope', '--table', 'customers']) == EXIT_BAD_CONFIGURATION


def test_subset_generates_jobs_that_run(schemaWorkspace, monkeypatch):
    """The whole loop: generate a masked subset, then run what was generated."""
    monkeypatch.setenv('MASKING_KEY', 'a-cli-test-masking-key')
    copy = sqlite3.connect(str(schemaWorkspace / 'copy.db'))
    copy.executescript('''
        CREATE TABLE customers (id INT PRIMARY KEY, email TEXT, tier TEXT);
        CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT REFERENCES customers(id), total INT);
        ''')
    copy.close()

    assert main(['subset', '--quiet', '--database', 'demo', '--target', 'copy', '--root', 'customers',
                 '--where', "tier = 'gold'", '--mask', '--output', 'subset/jobs.yaml']) == EXIT_SUCCESS
    (schemaWorkspace / 'subset' / 'database.yaml').write_text((schemaWorkspace / 'configuration' / 'database.yaml').read_text())

    assert main(['run', '--quiet', '--config', 'subset']) == EXIT_SUCCESS

    copy = sqlite3.connect(str(schemaWorkspace / 'copy.db'))
    try:
        assert copy.execute('SELECT id, tier FROM customers').fetchall() == [(1, 'gold')]
        assert copy.execute('SELECT email FROM customers').fetchone()[0].endswith('@example.test')
        assert copy.execute('SELECT id, customer_id FROM orders').fetchall() == [(10, 1)]
    finally:
        copy.close()


def test_subset_refuses_to_load_over_its_own_source(schemaWorkspace):
    assert main(['subset', '--quiet', '--database', 'demo', '--target', 'demo', '--root', 'customers', '--where', '1 = 1']) == EXIT_BAD_CONFIGURATION


def test_subset_reports_a_cycle_as_a_usage_error(schemaWorkspace, caplog):
    connection = sqlite3.connect(str(schemaWorkspace / 'demo.db'))
    connection.execute('CREATE TABLE employees (id INT PRIMARY KEY, manager_id INT REFERENCES employees(id))')
    connection.commit()
    connection.close()

    assert main(['subset', '--quiet', '--database', 'demo', '--target', 'copy', '--root', 'employees', '--where', '1 = 1']) == EXIT_BAD_CONFIGURATION
    assert '--ignore-foreign-key' in caplog.text

    assert main(['subset', '--quiet', '--database', 'demo', '--target', 'copy', '--root', 'employees', '--where', '1 = 1',
                 '--ignore-foreign-key', 'employees.manager_id', '--output', 'employees.yaml']) == EXIT_SUCCESS


def test_schema_prints_ddl_for_the_target(schemaWorkspace, capsys):
    assert main(['schema', '--quiet', '--database', 'demo', '--target', 'copy', '--table', 'orders', '--related']) == EXIT_SUCCESS

    output = capsys.readouterr().out

    assert output.startswith('-- Generated by `lightweight-etl schema`')
    assert output.index('CREATE TABLE customers') < output.index('CREATE TABLE orders')
    assert 'REFERENCES customers (id)' in output


def test_schema_apply_creates_missing_tables_and_leaves_existing_ones(schemaWorkspace, capsys):
    copy = sqlite3.connect(str(schemaWorkspace / 'copy.db'))
    copy.execute('CREATE TABLE customers (id INT PRIMARY KEY, marker TEXT)')
    copy.commit()
    copy.close()

    assert main(['schema', '--quiet', '--database', 'demo', '--target', 'copy', '--table', 'customers', '--related', '--apply']) == EXIT_SUCCESS
    assert '1 table(s) created, 1 already existed' in capsys.readouterr().out

    copy = sqlite3.connect(str(schemaWorkspace / 'copy.db'))
    try:
        assert [row[1] for row in copy.execute('PRAGMA table_info(customers)')] == ['id', 'marker']
        assert [row[1] for row in copy.execute('PRAGMA table_info(orders)')] == ['id', 'customer_id', 'total']
    finally:
        copy.close()


def test_schema_against_the_source_itself_creates_only_stage_tables(schemaWorkspace):
    assert main(['schema', '--quiet', '--database', 'demo', '--target', 'demo', '--table', 'customers']) == EXIT_BAD_CONFIGURATION
    assert main(['schema', '--quiet', '--database', 'demo', '--target', 'demo', '--table', 'customers',
                 '--stage-suffix', '_masked_stage', '--apply']) == EXIT_SUCCESS

    connection = sqlite3.connect(str(schemaWorkspace / 'demo.db'))
    try:
        assert connection.execute("SELECT count(*) FROM sqlite_master WHERE name = 'customers_masked_stage'").fetchone() == (1,)
    finally:
        connection.close()


def test_schema_reports_a_missing_table_as_a_usage_error(schemaWorkspace):
    assert main(['schema', '--quiet', '--database', 'demo', '--target', 'copy', '--table', 'nope']) == EXIT_BAD_CONFIGURATION


def test_clear_needs_yes_and_dry_run_changes_nothing(workspace, capsys):
    main(['run', '--quiet'])

    assert main(['clear', '--quiet']) == EXIT_BAD_CONFIGURATION
    assert main(['clear', '--quiet', '--dry-run']) == EXIT_SUCCESS
    assert 'demo: would empty, in order: tgt' in capsys.readouterr().out
    assert _targetRowCount(workspace) == 5


def test_clear_empties_targets_and_a_forced_run_refills_them(workspace, capsys):
    (workspace / 'configuration' / 'jobs.yaml').write_text(
        JOBS_YAML.replace('  loadRows:\n    active: true\n', '  loadRows:\n    active: true\n    refresh: 60\n', 1))
    main(['run', '--quiet'])

    assert main(['clear', '--quiet', '--yes']) == EXIT_SUCCESS
    assert 'run the jobs with --force' in capsys.readouterr().out.lower()
    assert _targetRowCount(workspace) == 0

    assert main(['run', '--quiet', '--force']) == EXIT_SUCCESS
    assert _targetRowCount(workspace) == 5


def test_clear_refuses_the_target_of_an_incremental_job(workspace, caplog):
    incremental = JOBS_YAML.replace('SELECT id, name FROM src\n    targetDatabase: demo\n    targetTableFinal: tgt\n    insertStrategy: upsert\n    chunkSize: 2\n  dependent:',
                                    'SELECT id, name FROM src WHERE id > {{ watermark }}\n    watermarkColumn: id\n    watermarkInitial: 0\n'
                                    '    targetDatabase: demo\n    targetTableFinal: tgt\n    insertStrategy: upsert\n    chunkSize: 2\n  dependent:')
    (workspace / 'configuration' / 'jobs.yaml').write_text(incremental)

    assert main(['clear', '--quiet', '--yes']) == EXIT_BAD_CONFIGURATION
    assert 'loadRows' in caplog.text

    assert main(['clear', '--quiet', '--yes', '--job', 'dependent']) == EXIT_SUCCESS


def test_clear_rolls_back_and_exits_nonzero_when_a_delete_is_refused(schemaWorkspace, caplog):
    """A refused DELETE rolls the whole clear back. SQLite only enforces
    foreign keys when a connection asks, so a trigger stands in for one here;
    tests/test_integration_schema.py covers real foreign keys on every server.
    """
    (schemaWorkspace / 'configuration' / 'jobs.yaml').write_text(JOBS_YAML.replace('targetTableFinal: tgt', 'targetTableFinal: customers'))
    connection = sqlite3.connect(str(schemaWorkspace / 'demo.db'))
    connection.execute('CREATE TRIGGER keep_customers BEFORE DELETE ON customers BEGIN SELECT RAISE(ABORT, \'customers are referenced\'); END')
    connection.commit()
    connection.close()

    assert main(['clear', '--quiet', '--yes']) == EXIT_JOBS_DID_NOT_SUCCEED
    assert 'nothing was cleared' in caplog.text
