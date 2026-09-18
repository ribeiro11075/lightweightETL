"""Exercises the CLI through main(argv) rather than a subprocess: the same code
path the console script takes, without paying process startup per assertion.

The exit codes are the point. For anything that schedules work the exit code is
the entire interface, and runDataJobs used to return None -- so a cron wrapping
this reported success on total failure.
"""
import sqlite3

import pytest

from bauta.cli import EXIT_BAD_CONFIGURATION, EXIT_JOBS_DID_NOT_SUCCEED, EXIT_SUCCESS, main

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


def test_an_unexpected_error_is_logged_scrubbed_and_exits_1(workspace, monkeypatch):
    import bauta.cli as cli

    def failing(arguments, log):
        raise RuntimeError('duplicate key value\nDETAIL:  Key (email)=(ann@corp.com) already exists.\n')

    monkeypatch.setattr(cli, '_commandValidate', failing)

    assert main(['validate', '--quiet', '--log', 'bauta.log']) == EXIT_JOBS_DID_NOT_SUCCEED
    logged = (workspace / 'bauta.log').read_text()
    assert 'Failed: RuntimeError: duplicate key value' in logged and 'Traceback' in logged
    assert 'Key (email)=(<redacted>)' in logged and 'ann@corp.com' not in logged


def test_validate_passes_on_good_configuration(workspace, capsys):
    assert main(['validate', '--quiet']) == EXIT_SUCCESS
    assert 'configuration is valid' in capsys.readouterr().out


def test_validate_is_offline(workspace, monkeypatch):
    """It must not open a connection -- that's what makes it safe in CI and in a
    pre-commit hook. `run --dry-run` is the online counterpart.
    """
    def explode(*args, **kwargs):
        raise AssertionError('validate must not connect to anything')

    monkeypatch.setattr('bauta.cli.Database', explode)

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


@pytest.mark.parametrize('command', [['subset', '--root', 'customers', '--where', '1=1'], ['discover', '--table', 'customers']])
def test_generated_jobs_need_a_chunk_size_of_at_least_one(schemaWorkspace, command):
    with pytest.raises(SystemExit) as raised:
        main(command + ['--quiet', '--database', 'demo', '--target', 'copy', '--chunk-size', '0'])

    assert raised.value.code == EXIT_BAD_CONFIGURATION


def test_discover_rejects_an_unknown_alias(schemaWorkspace):
    assert main(['discover', '--quiet', '--database', 'nope', '--table', 'customers']) == EXIT_BAD_CONFIGURATION


def test_audit_connect_flags_a_reference_masked_unlike_its_key(schemaWorkspace, capsys):
    """The foreign key is only in the source; the copy declares none."""
    (schemaWorkspace / 'configuration' / 'jobs.yaml').write_text("""workers: 1
jobs:
  maskCustomers:
    active: true
    sourceDatabase: demo
    sourceQuery: SELECT id, email, tier FROM customers
    targetDatabase: copy
    targetTableFinal: customers
    targetColumns: [id, email, tier]
    insertStrategy: upsert
    chunkSize: 10
    masking:
      key: an-audit-cli-masking-key
      columns: {id: {strategy: key, domain: customer}, email: email, tier: keep}
  maskOrders:
    active: true
    sourceDatabase: demo
    sourceQuery: SELECT id, customer_id, total FROM orders
    targetDatabase: copy
    targetTableFinal: orders
    targetColumns: [id, customer_id, total]
    insertStrategy: upsert
    chunkSize: 10
    masking:
      key: an-audit-cli-masking-key
      columns: {id: keep, customer_id: key, total: keep}
""")

    assert main(['audit', '--quiet', '--connect']) == EXIT_SUCCESS
    assert main(['audit', '--quiet', '--connect', '--strict']) == EXIT_JOBS_DID_NOT_SUCCEED

    out = capsys.readouterr().out
    assert 'maskOrders: in copy, orders.customer_id is masked with key in domain customer_id' in out
    assert 'but customers.id, which it references, is masked with key in domain customer' in out


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

    assert output.startswith('-- Generated by `bauta schema`')
    assert output.index('CREATE TABLE customers') < output.index('CREATE TABLE orders')
    assert 'REFERENCES customers ("id")' in output


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


def test_run_keeps_its_memory_beside_the_configuration(workspace):
    """Not in the working directory, which differs between a cron entry, a
    shell and a container -- losing the file silently resets every watermark.
    """
    assert main(['run', '--quiet']) == EXIT_SUCCESS

    assert (workspace / 'configuration' / 'memory.yaml').exists()
    assert not (workspace / 'memory.yaml').exists()


def test_the_jobs_file_says_where_run_state_lives(workspace, monkeypatch):
    """`memory` is relative to jobs.yaml, not to the working directory, so a run
    from anywhere finds the same file -- and nothing lands in configuration/.
    """
    (workspace / 'configuration' / 'jobs.yaml').write_text('memory: ../transaction/memory.yaml\n' + JOBS_YAML)
    (workspace / 'configuration' / 'database.yaml').write_text('demo:\n  type: sqlite\n  database: {}\n'.format(workspace / 'demo.db'))
    elsewhere = workspace / 'elsewhere'
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    assert main(['run', '--quiet', '--config', str(workspace / 'configuration')]) == EXIT_SUCCESS

    assert 'loadRows' in (workspace / 'transaction' / 'memory.yaml').read_text()
    assert (workspace / 'transaction' / 'memory.yaml.run.lock').exists()
    assert {path.name for path in (workspace / 'configuration').iterdir()} == {'jobs.yaml', 'database.yaml'}
    assert not any(elsewhere.iterdir())


def test_memory_on_the_command_line_wins_over_the_jobs_file(workspace):
    (workspace / 'configuration' / 'jobs.yaml').write_text('memory: ../transaction/memory.yaml\n' + JOBS_YAML)

    assert main(['run', '--quiet', '--memory', 'chosen.yaml']) == EXIT_SUCCESS

    assert (workspace / 'chosen.yaml').exists()
    assert not (workspace / 'transaction').exists()


def test_a_memory_file_in_the_working_directory_is_not_used(workspace):
    """Only the configuration decides where run state lives."""
    (workspace / 'memory.yaml').write_text('lastRun: {}\n')

    assert main(['run', '--quiet']) == EXIT_SUCCESS

    assert (workspace / 'memory.yaml').read_text() == 'lastRun: {}\n'
    assert (workspace / 'configuration' / 'memory.yaml').exists()


def test_validate_says_where_run_state_lives(workspace, capsys):
    (workspace / 'configuration' / 'jobs.yaml').write_text('memory: ../transaction/memory.yaml\n' + JOBS_YAML)

    assert main(['validate', '--quiet']) == EXIT_SUCCESS

    assert 'run state: transaction/memory.yaml' in capsys.readouterr().out


def test_a_second_run_sharing_the_memory_file_refuses_to_start(workspace, caplog):
    """Overlapping runs -- a cron interval shorter than a slow run -- would run
    the same jobs at once, with two swaps renaming the same tables.
    """
    from bauta.memory import exclusiveRun

    with exclusiveRun(workspace / 'configuration' / 'memory.yaml.run.lock'):
        assert main(['run', '--quiet']) == EXIT_JOBS_DID_NOT_SUCCEED

    assert 'another run is already using' in caplog.text
    assert _targetRowCount(workspace) == 0

    assert main(['run', '--quiet']) == EXIT_SUCCESS


def test_an_interrupted_run_exits_130(workspace, monkeypatch):
    from bauta.cli import EXIT_INTERRUPTED
    from bauta.runner import RunResult

    monkeypatch.setattr('bauta.cli.runDataJobs', lambda **kwargs: RunResult(outcomes=[], interrupted=True))

    assert main(['run', '--quiet']) == EXIT_INTERRUPTED


def test_workers_must_be_a_positive_number(workspace, capsys):
    with pytest.raises(SystemExit):
        main(['run', '--quiet', '--workers', '0'])

    assert 'must be at least 1' in capsys.readouterr().err


AUDIT_JOBS_YAML = """workers: 1
jobs:
  maskRows:
    active: true
    sourceDatabase: demo
    sourceQuery: SELECT id, name AS email FROM src
    targetDatabase: demo
    targetTableFinal: tgt
    insertStrategy: upsert
    chunkSize: 2
    masking:
      key: an-audit-cli-masking-key
      columns:
        id: keep
        email: keep
"""


def test_audit_reports_offline_and_passes_without_errors(workspace, capsys, monkeypatch):
    def explode(*args, **kwargs):
        raise AssertionError('audit without --connect must not connect')

    monkeypatch.setattr('bauta.cli.Database', explode)
    (workspace / 'configuration' / 'jobs.yaml').write_text(AUDIT_JOBS_YAML)

    assert main(['audit', '--quiet']) == EXIT_SUCCESS

    out = capsys.readouterr().out
    assert 'as declared (run with --connect to resolve)' in out
    assert 'column email is kept unmasked' in out


def test_audit_strict_fails_on_warnings(workspace):
    (workspace / 'configuration' / 'jobs.yaml').write_text(AUDIT_JOBS_YAML)

    assert main(['audit', '--quiet', '--strict']) == EXIT_JOBS_DID_NOT_SUCCEED


def test_audit_connect_resolves_columns_and_writes_json(workspace):
    import json

    (workspace / 'configuration' / 'jobs.yaml').write_text(AUDIT_JOBS_YAML.replace('        email: keep\n', '').replace(
        '        id: keep\n', '        id: keep\n      defaultStrategy: "null"\n'))

    assert main(['audit', '--quiet', '--connect', '--format', 'json', '--output', 'audit.json']) == EXIT_SUCCESS

    report = json.loads((workspace / 'audit.json').read_text())
    (job,) = report['jobs']
    assert job['columnsResolved']
    assert [(column['column'], column['source']) for column in job['columns']] == [('id', 'column'), ('email', 'defaultStrategy')]
    assert report['connections'] == {}


def test_audit_connect_fails_on_a_policy_the_query_outgrew(workspace, capsys):
    (workspace / 'configuration' / 'jobs.yaml').write_text(AUDIT_JOBS_YAML.replace('        email: keep\n', ''))

    assert main(['audit', '--quiet', '--connect']) == EXIT_JOBS_DID_NOT_SUCCEED
    assert 'not in the masking policy: email' in capsys.readouterr().out


def test_validate_refuses_an_option_that_duplicates_a_field(workspace):
    (workspace / 'configuration' / 'database.yaml').write_text('demo:\n  type: sqlite\n  database: demo.db\n  options:\n    database: other.db\n')

    assert main(['validate', '--quiet']) == EXIT_BAD_CONFIGURATION


def _runWithManifest(workspace, monkeypatch, signingKey=None):
    import json

    (workspace / 'configuration' / 'jobs.yaml').write_text(AUDIT_JOBS_YAML)
    if signingKey:
        monkeypatch.setenv('BAUTA_MANIFEST_KEY', signingKey)
    else:
        monkeypatch.delenv('BAUTA_MANIFEST_KEY', raising=False)

    assert main(['run', '--quiet', '--manifest', 'manifest.json']) == EXIT_SUCCESS

    return workspace / 'manifest.json', json.loads((workspace / 'manifest.json').read_text())


def test_the_manifest_records_the_tool_and_the_jobs_file_and_is_sealed(workspace, monkeypatch, capsys):
    import hashlib

    path, manifest = _runWithManifest(workspace, monkeypatch)

    assert manifest['tool']['name'] == 'bauta'
    assert manifest['configuration']['sha256'] == hashlib.sha256((workspace / 'configuration' / 'jobs.yaml').read_bytes()).hexdigest()
    assert set(manifest['integrity']) == {'algorithm', 'digest'}

    assert main(['verify-manifest', str(path), '--quiet']) == EXIT_SUCCESS
    assert 'not signed' in capsys.readouterr().out


def test_verify_manifest_catches_an_edit(workspace, monkeypatch, caplog):
    import json

    path, manifest = _runWithManifest(workspace, monkeypatch)
    manifest['jobs'][0]['rowCount'] = 999
    path.write_text(json.dumps(manifest))

    assert main(['verify-manifest', str(path), '--quiet']) == EXIT_JOBS_DID_NOT_SUCCEED
    assert 'changed after it was written' in caplog.text


def test_a_signed_manifest_needs_its_key_to_verify(workspace, monkeypatch, capsys, caplog):
    path, manifest = _runWithManifest(workspace, monkeypatch, signingKey='a-cli-manifest-signing-key')
    assert manifest['integrity']['signatureAlgorithm'] == 'hmac-sha256'

    assert main(['verify-manifest', str(path), '--quiet']) == EXIT_SUCCESS
    assert 'intact, and signed with key' in capsys.readouterr().out

    monkeypatch.setenv('BAUTA_MANIFEST_KEY', 'some-other-signing-key')
    assert main(['verify-manifest', str(path), '--quiet']) == EXIT_JOBS_DID_NOT_SUCCEED
    assert 'signature is not valid' in caplog.text

    monkeypatch.delenv('BAUTA_MANIFEST_KEY')
    assert main(['verify-manifest', str(path), '--quiet']) == EXIT_BAD_CONFIGURATION


def test_a_manifest_stripped_of_its_signature_fails_when_a_key_is_set(workspace, monkeypatch, caplog):
    """Edit it, drop the signature, recompute the digest: only the missing
    signature gives it away.
    """
    import json

    from bauta.masking import sealManifest

    path, manifest = _runWithManifest(workspace, monkeypatch, signingKey='a-cli-manifest-signing-key')
    del manifest['integrity']
    manifest['jobs'][0]['columns'][0]['strategy'] = 'keep'
    path.write_text(json.dumps(sealManifest(manifest)))

    assert main(['verify-manifest', str(path), '--quiet']) == EXIT_JOBS_DID_NOT_SUCCEED
    assert 'signature removed' in caplog.text

    monkeypatch.delenv('BAUTA_MANIFEST_KEY')
    assert main(['verify-manifest', str(path), '--quiet']) == EXIT_SUCCESS


def test_run_records_history_and_history_shows_it(workspace, capsys):
    import json

    assert main(['run', '--quiet', '--history', 'state/history.jsonl']) == EXIT_SUCCESS
    assert main(['run', '--quiet', '--force', '--job', 'loadRows', '--history', 'state/history.jsonl']) == EXIT_SUCCESS
    capsys.readouterr()

    assert main(['history', '--quiet', '--history', 'state/history.jsonl']) == EXIT_SUCCESS
    rows = capsys.readouterr().out.splitlines()
    assert rows[0].split() == ['FINISHED', 'JOB', 'STATUS', 'ROWS', 'SECONDS', 'ERROR']
    assert [row.split()[2] for row in rows[1:]] == ['loadRows', 'dependent', 'loadRows']

    assert main(['history', '--quiet', '--history', 'state/history.jsonl', '--job', 'dependent', '--format', 'json']) == EXIT_SUCCESS
    assert [record['job'] for record in json.loads(capsys.readouterr().out)] == ['dependent']


def test_history_needs_to_be_told_where_to_read(workspace):
    assert main(['history', '--quiet']) == EXIT_BAD_CONFIGURATION


def test_a_failed_run_posts_a_notification(workspace, monkeypatch):
    import http.server
    import json
    import threading

    received = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            received.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
            self.send_response(204)
            self.end_headers()

        def log_message(self, *arguments):
            pass

    server = http.server.HTTPServer(('127.0.0.1', 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setenv('BAUTA_NOTIFY_URL', 'http://127.0.0.1:{}/hook'.format(server.server_port))

    try:
        assert main(['run', '--quiet']) == EXIT_SUCCESS
        (workspace / 'configuration' / 'jobs.yaml').write_text(JOBS_YAML.replace('FROM src', 'FROM missing_table', 1))
        assert main(['run', '--quiet', '--force']) == EXIT_JOBS_DID_NOT_SUCCEED
    finally:
        server.shutdown()

    assert [payload['status'] for payload in received] == ['failed']
    assert received[0]['summary'] == {'completed': 0, 'failed': 1, 'skipped': 1, 'rows': 0}


def test_reporting_failures_do_not_fail_the_run(workspace, caplog):
    assert main(['run', '--quiet', '--notify-url', 'http://127.0.0.1:9/unreachable', '--notify-on', 'always']) == EXIT_SUCCESS
    assert 'Could not send the notification' in caplog.text


def test_run_memory_can_live_in_a_database(workspace):
    import sqlite3

    from bauta.memory import DATABASE_MEMORY_SCHEMA

    connection = sqlite3.connect(str(workspace / 'demo.db'))
    connection.execute(DATABASE_MEMORY_SCHEMA)
    connection.commit()
    connection.close()

    (workspace / 'configuration' / 'jobs.yaml').write_text('memory: ../transaction/memory.yaml\n' + JOBS_YAML)

    assert main(['run', '--quiet', '--memory-database', 'demo']) == EXIT_SUCCESS

    connection = sqlite3.connect(str(workspace / 'demo.db'))
    assert {row[0] for row in connection.execute('SELECT job FROM bauta_memory')} == {'loadRows', 'dependent'}
    connection.close()
    assert not (workspace / 'transaction' / 'memory.yaml').exists()
    # The run lock still needs a file; it goes where the memory file would.
    assert (workspace / 'transaction' / 'memory.run.lock').exists()


def test_a_rotated_masking_key_needs_clear_or_acknowledgement(workspace, monkeypatch, caplog):
    monkeypatch.setenv('MASKING_KEY', 'the-original-masking-key')
    (workspace / 'configuration' / 'jobs.yaml').write_text(MASKED_JOBS_YAML)
    assert main(['run', '--quiet']) == EXIT_SUCCESS

    monkeypatch.setenv('MASKING_KEY', 'the-rotated-masking-key')
    assert main(['run', '--quiet', '--force']) == EXIT_BAD_CONFIGURATION
    assert 'masking key changed' in caplog.text

    assert main(['run', '--quiet', '--force', '--accept-key-change']) == EXIT_SUCCESS

    monkeypatch.setenv('MASKING_KEY', 'a-third-masking-key-value')
    assert main(['clear', '--quiet', '--yes']) == EXIT_SUCCESS
    assert main(['run', '--quiet', '--force']) == EXIT_SUCCESS


def test_validate_never_runs_a_password_command(workspace, capsys):
    (workspace / 'configuration' / 'database.yaml').write_text(
        'demo:\n  type: sqlite\n  database: demo.db\n'
        'warehouse:\n  type: postgresql\n  database: w\n  host: h\n  user: u\n  passwordCommand: [/no/such/command]\n')

    assert main(['validate', '--quiet']) == EXIT_SUCCESS
    assert '2 database alias(es)' in capsys.readouterr().out


def test_synthesize_needs_yes_and_dry_run_writes_nothing(workspace, capsys):
    import sqlite3

    assert main(['synthesize', '--quiet', '--database', 'demo', '--table', 'tgt:3']) == EXIT_BAD_CONFIGURATION

    assert main(['synthesize', '--quiet', '--database', 'demo', '--table', 'tgt:3', '--dry-run']) == EXIT_SUCCESS
    out = capsys.readouterr().out
    assert 'tgt: 3 row(s)' in out and 'primary key' in out and out.count('sample:') == 3
    assert _targetRowCount(workspace) == 0

    assert main(['synthesize', '--quiet', '--database', 'demo', '--table', 'tgt:3', '--table', 'src', '--rows', '2', '--seed', '4', '--yes']) == EXIT_SUCCESS
    assert _targetRowCount(workspace) == 3
    connection = sqlite3.connect(str(workspace / 'demo.db'))
    assert connection.execute('SELECT count(*) FROM src').fetchone() == (7,)
    connection.close()


def test_synthesize_rejects_a_bad_table_argument(workspace):
    assert main(['synthesize', '--quiet', '--database', 'demo', '--table', 'tgt:0', '--yes']) == EXIT_BAD_CONFIGURATION


def _createTables(workspace, *schemas):
    connection = sqlite3.connect(str(workspace / 'demo.db'))
    for schema in schemas:
        connection.execute(schema)
    connection.commit()
    connection.close()


def test_history_and_the_manifest_go_where_the_jobs_file_says(maskedWorkspace, capsys):
    """Relative to the jobs file, as memory is, and read back by `history` and
    `verify-manifest` with no flags at all.
    """
    import json

    (maskedWorkspace / 'configuration' / 'jobs.yaml').write_text(
        'history: ../state/history.jsonl\nmanifest: ../state/manifest.json\n' + MASKED_JOBS_YAML)

    assert main(['run', '--quiet']) == EXIT_SUCCESS
    assert json.loads((maskedWorkspace / 'state' / 'manifest.json').read_text())['jobs'][0]['job'] == 'maskRows'
    capsys.readouterr()

    assert main(['history', '--quiet']) == EXIT_SUCCESS
    assert 'maskRows' in capsys.readouterr().out
    assert main(['verify-manifest', '--quiet']) == EXIT_SUCCESS
    assert 'state/manifest.json: intact' in capsys.readouterr().out


def test_memory_history_and_the_manifest_can_all_live_in_tables(maskedWorkspace, capsys):
    from bauta.memory import DATABASE_MEMORY_SCHEMA
    from bauta.reporting import DATABASE_HISTORY_SCHEMA, DATABASE_MANIFEST_SCHEMA

    _createTables(maskedWorkspace, DATABASE_MEMORY_SCHEMA.replace('bauta_memory', 'etl_memory'), DATABASE_HISTORY_SCHEMA, DATABASE_MANIFEST_SCHEMA)
    (maskedWorkspace / 'configuration' / 'jobs.yaml').write_text(
        'memory:\n  database: demo\n  table: etl_memory\nhistory:\n  database: demo\nmanifest:\n  database: demo\n' + MASKED_JOBS_YAML)

    assert main(['validate', '--quiet']) == EXIT_SUCCESS
    output = capsys.readouterr().out
    assert 'run state: table etl_memory in demo' in output
    assert 'history: table bauta_history in demo' in output
    assert 'manifest: table bauta_manifest in demo' in output

    assert main(['run', '--quiet']) == EXIT_SUCCESS
    assert main(['run', '--quiet', '--force']) == EXIT_SUCCESS

    connection = sqlite3.connect(str(maskedWorkspace / 'demo.db'))
    assert {row[0] for row in connection.execute('SELECT job FROM etl_memory')} == {'maskRows', 'maskRows#maskingKey'}
    assert connection.execute('SELECT count(*) FROM bauta_history').fetchone()[0] == 2
    runs = [row[0] for row in connection.execute('SELECT DISTINCT run_id FROM bauta_manifest ORDER BY written_at')]
    connection.close()
    assert len(runs) == 2
    assert not (maskedWorkspace / 'configuration' / 'memory.yaml').exists()
    capsys.readouterr()

    assert main(['history', '--quiet', '--format', 'json']) == EXIT_SUCCESS
    assert len(__import__('json').loads(capsys.readouterr().out)) == 2
    assert main(['verify-manifest', '--quiet']) == EXIT_SUCCESS
    assert 'run {} in table bauta_manifest in demo: intact'.format(runs[1]) in capsys.readouterr().out
    assert main(['verify-manifest', '--quiet', '--run', runs[0]]) == EXIT_SUCCESS
    assert 'run {} '.format(runs[0]) in capsys.readouterr().out


def test_a_manifest_edited_in_its_table_fails_verification(maskedWorkspace):
    from bauta.reporting import DATABASE_MANIFEST_SCHEMA

    _createTables(maskedWorkspace, DATABASE_MANIFEST_SCHEMA)
    assert main(['run', '--quiet', '--manifest-database', 'demo']) == EXIT_SUCCESS

    connection = sqlite3.connect(str(maskedWorkspace / 'demo.db'))
    connection.execute('UPDATE bauta_manifest SET content = replace(content, \'"rowCount": 5\', \'"rowCount": 6\')')
    connection.commit()
    connection.close()

    assert main(['verify-manifest', '--quiet', '--manifest-database', 'demo']) == EXIT_JOBS_DID_NOT_SUCCEED


def test_flags_override_the_jobs_file(maskedWorkspace):
    (maskedWorkspace / 'configuration' / 'jobs.yaml').write_text('history: ../configured.jsonl\nmanifest: ../configured.json\n' + MASKED_JOBS_YAML)

    assert main(['run', '--quiet', '--history', 'flagged.jsonl', '--manifest', 'flagged.json']) == EXIT_SUCCESS

    assert (maskedWorkspace / 'flagged.jsonl').exists() and (maskedWorkspace / 'flagged.json').exists()
    assert not (maskedWorkspace / 'configured.jsonl').exists() and not (maskedWorkspace / 'configured.json').exists()


def test_a_table_setting_needs_a_known_alias(workspace, caplog):
    (workspace / 'configuration' / 'jobs.yaml').write_text('history:\n  database: nowhere\n' + JOBS_YAML)

    assert main(['validate']) == EXIT_BAD_CONFIGURATION
    assert 'history: database "nowhere" is not a known database alias' in caplog.text


def test_a_misspelled_table_setting_is_refused(workspace, caplog):
    (workspace / 'configuration' / 'jobs.yaml').write_text('history:\n  database: demo\n  tabel: runs\n' + JOBS_YAML)

    assert main(['validate']) == EXIT_BAD_CONFIGURATION
    assert 'tabel' in caplog.text


def test_history_and_verify_manifest_say_where_to_look_when_nothing_is_set(workspace):
    assert main(['history', '--quiet']) == EXIT_BAD_CONFIGURATION
    assert main(['verify-manifest', '--quiet']) == EXIT_BAD_CONFIGURATION


DISCOVERY_YAML = """exclude: [email]
names:
- words: [tier]
  policy: {strategy: shuffle}
  reason: membership tier, which singles people out
"""


def test_discover_reads_discovery_yaml_from_the_configuration_directory(schemaWorkspace, capsys):
    (schemaWorkspace / 'configuration' / 'discovery.yaml').write_text(DISCOVERY_YAML)

    assert main(['discover', '--quiet', '--database', 'demo', '--table', 'customers', '--target', 'copy']) == EXIT_SUCCESS

    output = capsys.readouterr().out
    assert 'tier: {strategy: shuffle}  # membership tier, which singles people out' in output
    # Leaving out `email` leaves out the name rule and the value rule both.
    assert 'email address' not in output


def test_rules_can_be_named_on_the_command_line(schemaWorkspace, capsys):
    (schemaWorkspace / 'mine.yaml').write_text(DISCOVERY_YAML)

    assert main(['discover', '--quiet', '--database', 'demo', '--table', 'customers', '--rules', 'mine.yaml']) == EXIT_SUCCESS
    assert 'membership tier' in capsys.readouterr().out
    assert main(['discover', '--quiet', '--database', 'demo', '--table', 'customers', '--rules', 'missing.yaml']) == EXIT_BAD_CONFIGURATION


def test_audit_questions_a_kept_column_by_your_own_rules(maskedWorkspace, capsys):
    (maskedWorkspace / 'configuration' / 'discovery.yaml').write_text("names:\n- words: [name]\n  policy: fakeName\n")
    (maskedWorkspace / 'configuration' / 'jobs.yaml').write_text(MASKED_JOBS_YAML.replace('name: hash', 'name: keep'))

    main(['audit', '--quiet'])

    assert 'column name is kept unmasked, but its name matches a rule in discovery.yaml' in capsys.readouterr().out


def test_validate_checks_and_summarises_the_rules(workspace, capsys, caplog):
    assert main(['validate', '--quiet']) == EXIT_SUCCESS
    assert 'discovery rules: built-in' in capsys.readouterr().out

    (workspace / 'configuration' / 'discovery.yaml').write_text(DISCOVERY_YAML)
    assert main(['validate', '--quiet']) == EXIT_SUCCESS
    assert 'discovery rules: configuration/discovery.yaml (1 name, 0 value), built-in except email' in capsys.readouterr().out

    (workspace / 'configuration' / 'discovery.yaml').write_text("values:\n- pattern: '[0-9'\n  policy: hash\n")
    assert main(['validate']) == EXIT_BAD_CONFIGURATION
    assert 'not a valid regular expression' in caplog.text


def test_validate_says_how_many_threads_masking_would_use(maskedWorkspace, capsys, monkeypatch, caplog):
    import bauta.masking as masking
    from bauta.masking import nativeVersion

    monkeypatch.delenv('BAUTA_MASKING_THREADS', raising=False)
    monkeypatch.setattr(masking, 'availableCores', lambda: 4)
    (maskedWorkspace / 'configuration' / 'jobs.yaml').write_text('maskingThreads: 3\n' + MASKED_JOBS_YAML)
    assert main(['validate', '--quiet']) == EXIT_SUCCESS
    output = capsys.readouterr().out

    if nativeVersion() is None:
        assert 'masking: in Python, one thread per job' in output
    else:
        assert 'masking: bauta-rs {}, 3 thread(s) per job (maskingThreads: 3;'.format(nativeVersion()) in output
        monkeypatch.setenv('BAUTA_MASKING_THREADS', '2')
        assert main(['validate', '--quiet']) == EXIT_SUCCESS
        assert '2 thread(s) per job ($BAUTA_MASKING_THREADS=2;' in capsys.readouterr().out


def test_validate_refuses_more_masking_threads_than_cores(maskedWorkspace, monkeypatch, caplog):
    import bauta.masking as masking

    monkeypatch.delenv('BAUTA_MASKING_THREADS', raising=False)
    monkeypatch.setattr(masking, 'availableCores', lambda: 4)
    (maskedWorkspace / 'configuration' / 'jobs.yaml').write_text('maskingThreads: 16\n' + MASKED_JOBS_YAML)

    assert main(['validate']) == EXIT_BAD_CONFIGURATION
    assert 'maskingThreads is 16, but this machine has 4 core(s) available to it: set at most 4, or auto' in caplog.text
