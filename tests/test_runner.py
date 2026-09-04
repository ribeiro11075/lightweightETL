import pickle
from typing import Any, List, Tuple

import pytest

from library.configurationInterface import Configuration, DatabaseConnectionConfig, DatabaseType, DataJobConfig, DataJobsFile, InsertStrategy, \
    ScrambleJobConfig, ScrambleJobsFile
from library.memoryInterface import FileMemory
from library.runner import _dataJobWorker, _executeDataJob, _executeScrambleJob, _scrambleJobWorker, runDataJobs, runScrambleJobs


def test_worker_functions_are_picklable():
    """multiprocessing's spawn start method (macOS/Windows default) unpickles the
    Pool initializer by module+qualname in the child process -- a closure or
    nested function wouldn't survive that. This is the trickiest part of moving
    the worker functions into a library module rather than the entry script.
    """
    pickle.dumps(_dataJobWorker)
    pickle.dumps(_scrambleJobWorker)


def test_run_scramble_jobs_completes_with_zero_active_jobs(tmp_path):
    """Exercises a real Pool spawn + teardown without any network I/O, since no
    job is ever active enough to be pulled off the queue.
    """
    raw = {'workers': 2, 'jobs': {'noop': {'active': False, 'database': 'x', 'table': 'y', 'randomSalt': 's'}}}
    jobsFile = Configuration.validateJobConfiguration(raw, ScrambleJobsFile)

    runScrambleJobs(jobsFile=jobsFile, databaseConfiguration={}, logDirectory=tmp_path / 'runner.log', runForever=False)


def test_run_data_jobs_completes_with_zero_active_jobs_when_not_forever(tmp_path):
    raw = {'workers': 2, 'jobs': {'noop': {'active': False, 'sourceDatabase': 'x', 'targetDatabase': 'y', 'insertStrategy': 'upsert',
                                            'chunkSize': 1, 'targetTableFinal': 't', 'sourceQuery': 'select 1'}}}
    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)

    runDataJobs(jobsFile=jobsFile, databaseConfiguration={}, logDirectory=tmp_path / 'runner.log',
                memory=FileMemory(memoryDirectory=tmp_path / 'runner.yaml'), runForever=False)


class _FakeDatabase:
    """Records calls instead of touching a real connection, so _executeDataJob and
    _executeScrambleJob can be exercised without mocking at the driver level.
    """

    queryResult: List[Tuple[Any, ...]] = [(1, 'a'), (2, 'b')]
    columnNames: List[str] = ['id', 'name']
    columnTypes: List[str] = ['INT', 'VARCHAR']

    def __init__(self, connectionSettings: DatabaseConnectionConfig) -> None:
        self.connectionSettings = connectionSettings
        self.calls: List[Tuple[Any, ...]] = []

    def __enter__(self) -> '_FakeDatabase':
        return self

    def __exit__(self, *args: Any) -> None:
        return None

    def query(self, query: str) -> List[Tuple[Any, ...]]:
        self.calls.append(('query', query))
        return self.queryResult

    def getAllColumnNames(self, table: str) -> List[str]:
        self.calls.append(('getAllColumnNames', table))
        return self.columnNames

    def getAllColumnTypes(self, table: str) -> List[str]:
        self.calls.append(('getAllColumnTypes', table))
        return self.columnTypes

    def truncate(self, table: str) -> None:
        self.calls.append(('truncate', table))

    def insert(self, table: str, data: List[Tuple[Any, ...]], chunkSize: int = 100) -> None:
        self.calls.append(('insert', table, data, chunkSize))

    def alter(self, query: str) -> None:
        self.calls.append(('alter', query))

    def swap(self, targetTable: str, stageTable: str) -> None:
        self.calls.append(('swap', targetTable, stageTable))

    def upsert(self, table: str, data: List[Tuple[Any, ...]], chunkSize: int = 100) -> None:
        self.calls.append(('upsert', table, data, chunkSize))

    def upsertFromStage(self, targetTable: str, stageTable: str) -> None:
        self.calls.append(('upsertFromStage', targetTable, stageTable))


@pytest.fixture
def fakeDatabases(monkeypatch):
    """Each Database(...) call under test creates a new _FakeDatabase, appended
    here in creation order -- for _executeDataJob that's (source, target).
    """
    created: List[_FakeDatabase] = []

    class _TrackedFakeDatabase(_FakeDatabase):
        def __init__(self, connectionSettings: DatabaseConnectionConfig) -> None:
            super().__init__(connectionSettings)
            created.append(self)

    monkeypatch.setattr('library.runner.Database', _TrackedFakeDatabase)

    return created


def _dbConfig(host: str = 'h') -> DatabaseConnectionConfig:
    return DatabaseConnectionConfig(type=DatabaseType.MYSQL, user='u', password='p', database='d', host=host)


def _dataJobConfig(**overrides: Any) -> DataJobConfig:
    fields = dict(active=True, sourceDatabase='src', targetDatabase='tgt', insertStrategy=InsertStrategy.UPSERT,
                  chunkSize=100, targetTableFinal='people', sourceQuery='select * from people')
    fields.update(overrides)
    return DataJobConfig(**fields)


def test_execute_data_job_opens_source_and_target_with_the_right_settings(fakeDatabases):
    jobConfig = _dataJobConfig()
    databaseConfiguration = {'src': _dbConfig('source-host'), 'tgt': _dbConfig('target-host')}

    _executeDataJob(jobConfig, databaseConfiguration)

    sourceDatabase, targetDatabase = fakeDatabases
    assert sourceDatabase.connectionSettings.host == 'source-host'
    assert targetDatabase.connectionSettings.host == 'target-host'
    assert ('query', 'select * from people') in sourceDatabase.calls


def test_execute_data_job_upserts_directly_when_there_is_no_stage_table(fakeDatabases):
    jobConfig = _dataJobConfig()
    databaseConfiguration = {'src': _dbConfig(), 'tgt': _dbConfig()}

    _executeDataJob(jobConfig, databaseConfiguration)

    _, targetDatabase = fakeDatabases
    calledMethods = [call[0] for call in targetDatabase.calls]
    assert 'upsert' in calledMethods
    assert 'upsertFromStage' not in calledMethods
    assert 'truncate' not in calledMethods
    assert 'swap' not in calledMethods


def test_execute_data_job_upserts_from_stage_when_a_stage_table_is_set(fakeDatabases):
    jobConfig = _dataJobConfig(targetTableStage='people_stage')
    databaseConfiguration = {'src': _dbConfig(), 'tgt': _dbConfig()}

    _executeDataJob(jobConfig, databaseConfiguration)

    _, targetDatabase = fakeDatabases
    assert ('truncate', 'people_stage') in targetDatabase.calls
    assert any(call[0] == 'insert' and call[1] == 'people_stage' for call in targetDatabase.calls)
    assert any(call[0] == 'upsertFromStage' for call in targetDatabase.calls)
    assert not any(call[0] == 'upsert' for call in targetDatabase.calls)


def test_execute_data_job_swap_loads_stage_then_swaps_not_upserts(fakeDatabases):
    jobConfig = _dataJobConfig(insertStrategy=InsertStrategy.SWAP, targetTableStage='people_stage')
    databaseConfiguration = {'src': _dbConfig(), 'tgt': _dbConfig()}

    _executeDataJob(jobConfig, databaseConfiguration)

    _, targetDatabase = fakeDatabases
    calledMethods = [call[0] for call in targetDatabase.calls]
    assert ('swap', 'people', 'people_stage') in targetDatabase.calls
    assert 'upsert' not in calledMethods
    assert 'upsertFromStage' not in calledMethods


def test_execute_data_job_applies_column_transforms_before_loading(fakeDatabases):
    jobConfig = _dataJobConfig(columnTransforms={'name': ['json:dumps']})
    databaseConfiguration = {'src': _dbConfig(), 'tgt': _dbConfig()}

    _executeDataJob(jobConfig, databaseConfiguration)

    _, targetDatabase = fakeDatabases
    _, _, transformedData, _ = next(call for call in targetDatabase.calls if call[0] == 'upsert')
    assert transformedData == [(1, '"a"'), (2, '"b"')]


def test_execute_data_job_runs_adhoc_queries_before_and_after_load(fakeDatabases):
    jobConfig = _dataJobConfig(preTargetAdhocQueries=['pre1'], postTargetAdhocQueries=['post1'])
    databaseConfiguration = {'src': _dbConfig(), 'tgt': _dbConfig()}

    _executeDataJob(jobConfig, databaseConfiguration)

    _, targetDatabase = fakeDatabases
    calls = targetDatabase.calls
    preIndex = calls.index(('alter', 'pre1'))
    upsertIndex = next(index for index, call in enumerate(calls) if call[0] == 'upsert')
    postIndex = calls.index(('alter', 'post1'))
    assert preIndex < upsertIndex < postIndex


def _scrambleJobConfig(**overrides: Any) -> ScrambleJobConfig:
    fields = dict(active=True, database='db1', table='people', randomSalt='s')
    fields.update(overrides)
    return ScrambleJobConfig(**fields)


def test_execute_scramble_job_truncates_and_reinserts_when_the_table_has_rows(fakeDatabases):
    jobConfig = _scrambleJobConfig(identifierColumns=['id'])
    databaseConfiguration = {'db1': _dbConfig()}

    _executeScrambleJob('scrambleJob1', jobConfig, databaseConfiguration)

    (database,) = fakeDatabases
    assert ('truncate', 'people') in database.calls
    assert any(call[0] == 'insert' and call[1] == 'people' for call in database.calls)


def test_execute_scramble_job_writes_nothing_when_the_table_is_empty(fakeDatabases, monkeypatch):
    monkeypatch.setattr(_FakeDatabase, 'queryResult', [])
    jobConfig = _scrambleJobConfig()
    databaseConfiguration = {'db1': _dbConfig()}

    _executeScrambleJob('scrambleJob1', jobConfig, databaseConfiguration)

    (database,) = fakeDatabases
    calledMethods = [call[0] for call in database.calls]
    assert 'truncate' not in calledMethods
    assert 'insert' not in calledMethods


def test_execute_scramble_job_runs_adhoc_queries_before_and_after(fakeDatabases):
    jobConfig = _scrambleJobConfig(preTargetAdhocQueries=['pre1'], postTargetAdhocQueries=['post1'])
    databaseConfiguration = {'db1': _dbConfig()}

    _executeScrambleJob('scrambleJob1', jobConfig, databaseConfiguration)

    (database,) = fakeDatabases
    calls = database.calls
    preIndex = calls.index(('alter', 'pre1'))
    insertIndex = next(index for index, call in enumerate(calls) if call[0] == 'insert')
    postIndex = calls.index(('alter', 'post1'))
    assert preIndex < insertIndex < postIndex
