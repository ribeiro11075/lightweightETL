import os
import pickle
import signal
import threading
from typing import Any, List, Tuple

import pytest

from lightweight_etl.configuration import Configuration, ConfigurationError, DatabaseConnectionConfig, DatabaseType, DataJobConfig, DataJobsFile, \
    InsertStrategy, ScrambleJobConfig, ScrambleJobsFile
from lightweight_etl.databaseDialects import ColumnCategory
from lightweight_etl.dependencyGraph import JobOutcome, JobStatus
from lightweight_etl.log import Log
from lightweight_etl.memory import FileMemory, MemoryBackend
from lightweight_etl.runner import RunResult, _dataJobWorker, _terminationHandling, _executeDataJob, _executeScrambleJob, _scrambleJobWorker, runDataJobs, runScrambleJobs
from lightweight_etl.transform import TransformError


class _FakeDialect:
    """columnCategory only -- _executeScrambleJob is the one caller that needs a
    dialect off of _FakeDatabase; nothing else here touches connect()/queries.
    """

    def columnCategory(self, dataType: Any) -> Any:
        return {'INT': ColumnCategory.NUMBER, 'VARCHAR': ColumnCategory.TEXT}.get(dataType)


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

    runScrambleJobs(jobsFile=jobsFile, databaseConfiguration={}, logFile=tmp_path / 'runner.log', runForever=False)


def test_run_data_jobs_completes_with_zero_active_jobs_when_not_forever(tmp_path):
    raw = {'workers': 2, 'jobs': {'noop': {'active': False, 'sourceDatabase': 'x', 'targetDatabase': 'y', 'insertStrategy': 'upsert',
                                            'chunkSize': 1, 'targetTableFinal': 't', 'sourceQuery': 'select 1'}}}
    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)

    runDataJobs(jobsFile=jobsFile, databaseConfiguration={}, logFile=tmp_path / 'runner.log',
                memory=FileMemory(memoryFile=tmp_path / 'runner.yaml'), runForever=False)


class _FakeDatabase:
    """Records calls instead of touching a real connection, so _executeDataJob and
    _executeScrambleJob can be exercised without mocking at the driver level.
    """

    queryResult: List[Tuple[Any, ...]] = [(1, 'a'), (2, 'b')]
    columnNames: List[str] = ['id', 'name']
    columnTypes: List[str] = ['INT', 'VARCHAR']
    dialect = _FakeDialect()

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

    def substituteWatermarkPlaceholder(self, query: str) -> str:
        return query.replace('{{ watermark }}', '?')

    def stream(self, query: str, chunkSize: int, parameters: Any = None) -> Tuple[List[str], Any]:
        """Mirrors Database.stream: (columns, chunkIterator), sliced at chunkSize
        so a test can set a small chunkSize and get genuinely multiple chunks.
        """
        self.calls.append(('stream', query, chunkSize, parameters))

        def chunks() -> Any:
            for index in range(0, len(self.queryResult), chunkSize):
                yield self.queryResult[index:index + chunkSize]

        return self.columnNames, chunks()

    def getLastQueryColumnNames(self) -> List[str]:
        self.calls.append(('getLastQueryColumnNames',))
        return self.columnNames

    def getAllColumnNames(self, table: str) -> List[str]:
        self.calls.append(('getAllColumnNames', table))
        return self.columnNames

    def getAllColumnTypes(self, table: str) -> List[str]:
        self.calls.append(('getAllColumnTypes', table))
        return self.columnTypes

    def truncate(self, table: str) -> None:
        self.calls.append(('truncate', table))

    def insert(self, table: str, data: List[Tuple[Any, ...]], chunkSize: int = 100, columns: Any = None) -> None:
        self.calls.append(('insert', table, data, chunkSize, columns))

    def alter(self, query: str) -> None:
        self.calls.append(('alter', query))

    def swap(self, targetTable: str, stageTable: str) -> None:
        self.calls.append(('swap', targetTable, stageTable))

    def upsert(self, table: str, data: List[Tuple[Any, ...]], chunkSize: int = 100, columns: Any = None) -> None:
        self.calls.append(('upsert', table, data, chunkSize, columns))

    def upsertFromStage(self, targetTable: str, stageTable: str, columns: Any = None) -> None:
        self.calls.append(('upsertFromStage', targetTable, stageTable, columns))


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

    monkeypatch.setattr('lightweight_etl.runner.Database', _TrackedFakeDatabase)

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

    _executeDataJob('job1', jobConfig, databaseConfiguration)

    sourceDatabase, targetDatabase = fakeDatabases
    assert sourceDatabase.connectionSettings.host == 'source-host'
    assert targetDatabase.connectionSettings.host == 'target-host'
    assert ('stream', 'select * from people', jobConfig.chunkSize, None) in sourceDatabase.calls


def test_execute_data_job_upserts_directly_when_there_is_no_stage_table(fakeDatabases):
    jobConfig = _dataJobConfig()
    databaseConfiguration = {'src': _dbConfig(), 'tgt': _dbConfig()}

    _executeDataJob('job1', jobConfig, databaseConfiguration)

    _, targetDatabase = fakeDatabases
    calledMethods = [call[0] for call in targetDatabase.calls]
    assert 'upsert' in calledMethods
    assert 'upsertFromStage' not in calledMethods
    assert 'truncate' not in calledMethods
    assert 'swap' not in calledMethods


def test_execute_data_job_infers_columns_from_target_table_when_target_columns_is_unset(fakeDatabases):
    jobConfig = _dataJobConfig()
    databaseConfiguration = {'src': _dbConfig(), 'tgt': _dbConfig()}
    assert jobConfig.targetColumns == []

    _executeDataJob('job1', jobConfig, databaseConfiguration)

    _, targetDatabase = fakeDatabases
    calledMethods = [call[0] for call in targetDatabase.calls]
    assert 'getAllColumnNames' in calledMethods
    upsertCall = next(call for call in targetDatabase.calls if call[0] == 'upsert')
    assert upsertCall[4] == targetDatabase.columnNames  # the introspected column list, in the table's own order


def test_execute_data_job_uses_target_columns_when_configured_instead_of_introspecting(fakeDatabases):
    """The actual point of targetColumns: sourceQuery's SELECT list doesn't have to
    match the target table's own column order (or even select every column) as
    long as it matches targetColumns positionally -- this proves that explicit
    list, not an introspected one, is what actually drives the transform and every
    insert/upsert call.
    """
    jobConfig = _dataJobConfig(targetColumns=['name', 'id'])
    databaseConfiguration = {'src': _dbConfig(), 'tgt': _dbConfig()}

    _executeDataJob('job1', jobConfig, databaseConfiguration)

    _, targetDatabase = fakeDatabases
    calledMethods = [call[0] for call in targetDatabase.calls]
    assert 'getAllColumnNames' not in calledMethods
    upsertCall = next(call for call in targetDatabase.calls if call[0] == 'upsert')
    assert upsertCall[4] == ['name', 'id']


def test_execute_data_job_passes_target_columns_to_stage_insert_and_upsert_from_stage(fakeDatabases):
    jobConfig = _dataJobConfig(targetTableStage='people_stage', targetColumns=['name', 'id'])
    databaseConfiguration = {'src': _dbConfig(), 'tgt': _dbConfig()}

    _executeDataJob('job1', jobConfig, databaseConfiguration)

    _, targetDatabase = fakeDatabases
    insertCall = next(call for call in targetDatabase.calls if call[0] == 'insert')
    upsertFromStageCall = next(call for call in targetDatabase.calls if call[0] == 'upsertFromStage')
    assert insertCall[4] == ['name', 'id']
    assert upsertFromStageCall[3] == ['name', 'id']


def test_execute_data_job_upserts_from_stage_when_a_stage_table_is_set(fakeDatabases):
    jobConfig = _dataJobConfig(targetTableStage='people_stage')
    databaseConfiguration = {'src': _dbConfig(), 'tgt': _dbConfig()}

    _executeDataJob('job1', jobConfig, databaseConfiguration)

    _, targetDatabase = fakeDatabases
    assert ('truncate', 'people_stage') in targetDatabase.calls
    assert any(call[0] == 'insert' and call[1] == 'people_stage' for call in targetDatabase.calls)
    assert any(call[0] == 'upsertFromStage' for call in targetDatabase.calls)
    assert not any(call[0] == 'upsert' for call in targetDatabase.calls)


def test_execute_data_job_swap_loads_stage_then_swaps_not_upserts(fakeDatabases):
    jobConfig = _dataJobConfig(insertStrategy=InsertStrategy.SWAP, targetTableStage='people_stage')
    databaseConfiguration = {'src': _dbConfig(), 'tgt': _dbConfig()}

    _executeDataJob('job1', jobConfig, databaseConfiguration)

    _, targetDatabase = fakeDatabases
    calledMethods = [call[0] for call in targetDatabase.calls]
    assert ('swap', 'people', 'people_stage') in targetDatabase.calls
    assert 'upsert' not in calledMethods
    assert 'upsertFromStage' not in calledMethods


def test_execute_data_job_applies_column_transforms_before_loading(fakeDatabases):
    jobConfig = _dataJobConfig(sourceQueryColumnTransforms={'name': ['json:dumps']})
    databaseConfiguration = {'src': _dbConfig(), 'tgt': _dbConfig()}

    _executeDataJob('job1', jobConfig, databaseConfiguration)

    _, targetDatabase = fakeDatabases
    upsertCall = next(call for call in targetDatabase.calls if call[0] == 'upsert')
    assert upsertCall[2] == [(1, '"a"'), (2, '"b"')]


def test_execute_data_job_raises_and_writes_nothing_when_a_transform_names_an_unknown_column(fakeDatabases):
    """The fake source's sourceQuery is understood to return columns ['id', 'name']
    (getLastQueryColumnNames); sourceQueryColumnTransforms names a column not in
    that list -- should fail loudly before any insert/upsert, rather than
    silently never applying.
    """
    jobConfig = _dataJobConfig(sourceQueryColumnTransforms={'doesNotExist': ['json:dumps']})
    databaseConfiguration = {'src': _dbConfig(), 'tgt': _dbConfig()}

    with pytest.raises(TransformError, match='doesNotExist'):
        _executeDataJob('job1', jobConfig, databaseConfiguration)

    _, targetDatabase = fakeDatabases
    calledMethods = [call[0] for call in targetDatabase.calls]
    assert 'insert' not in calledMethods
    assert 'upsert' not in calledMethods


def test_execute_data_job_validates_transforms_against_the_source_querys_columns_not_the_targets(fakeDatabases, monkeypatch):
    """A transform is applied to a value as extracted from the source, before it's
    ever mapped onto a target column name -- so it should succeed here even though
    the *target*'s own columns (introspected, since targetColumns is unset) don't
    include 'name' at all.
    """
    monkeypatch.setattr(_FakeDatabase, 'getAllColumnNames', lambda self, table: ['totallyDifferentTargetColumn'])
    jobConfig = _dataJobConfig(sourceQueryColumnTransforms={'name': ['json:dumps']})
    databaseConfiguration = {'src': _dbConfig(), 'tgt': _dbConfig()}

    _executeDataJob('job1', jobConfig, databaseConfiguration)

    _, targetDatabase = fakeDatabases
    upsertCall = next(call for call in targetDatabase.calls if call[0] == 'upsert')
    assert upsertCall[2] == [(1, '"a"'), (2, '"b"')]


def test_execute_data_job_raises_with_column_and_value_context_when_a_transform_fails(fakeDatabases):
    """The fake source's 'id' column holds ints (1, 2); a transform that only
    accepts strings should fail per-value with enough context to debug it, not a
    bare traceback from inside the row loop.
    """
    jobConfig = _dataJobConfig(sourceQueryColumnTransforms={'id': ['os.path:basename']})
    databaseConfiguration = {'src': _dbConfig(), 'tgt': _dbConfig()}

    with pytest.raises(TransformError, match='column "id"'):
        _executeDataJob('job1', jobConfig, databaseConfiguration)


def test_execute_data_job_runs_adhoc_queries_before_and_after_load(fakeDatabases):
    jobConfig = _dataJobConfig(preTargetAdhocQueries=['pre1'], postTargetAdhocQueries=['post1'])
    databaseConfiguration = {'src': _dbConfig(), 'tgt': _dbConfig()}

    _executeDataJob('job1', jobConfig, databaseConfiguration)

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


def test_execute_data_job_runs_pre_adhoc_queries_before_loading_the_stage_table(fakeDatabases):
    """The existing before/after test uses the stage-less upsert path, where a
    pre-query lands before the only write either way. With a stage table the
    ordering actually matters, and used to be wrong: pre-queries ran *after* the
    stage was truncated and loaded, so a query meant to prepare that table (drop
    an index to speed the load, disable a constraint, clear a partition) arrived
    after the rows it was preparing for.
    """
    jobConfig = _dataJobConfig(insertStrategy=InsertStrategy.SWAP, targetTableStage='people_stage', preTargetAdhocQueries=['pre1'],
                                postTargetAdhocQueries=['post1'])
    databaseConfiguration = {'src': _dbConfig(), 'tgt': _dbConfig()}

    _executeDataJob('job1', jobConfig, databaseConfiguration)

    _, targetDatabase = fakeDatabases
    calls = targetDatabase.calls
    preIndex = calls.index(('alter', 'pre1'))
    truncateIndex = calls.index(('truncate', 'people_stage'))
    insertIndex = next(index for index, call in enumerate(calls) if call[0] == 'insert')
    swapIndex = next(index for index, call in enumerate(calls) if call[0] == 'swap')
    postIndex = calls.index(('alter', 'post1'))

    assert preIndex < truncateIndex < insertIndex < swapIndex < postIndex


class _StopWorker(Exception):
    """Breaks _dataJobWorker out of its otherwise infinite readyQueue.get() loop."""


class _OneShotReadyQueue:

    def __init__(self, job: str) -> None:
        self.remaining = [job]

    def get(self) -> str:
        if self.remaining:
            return self.remaining.pop()
        raise _StopWorker


class _TimelineQueue:
    """Records completion signals into a timeline shared with _TimelineMemory, so
    a test can assert the *order* of the two, not just that both happened.
    """

    def __init__(self, timeline: List[Tuple[Any, ...]]) -> None:
        self.timeline = timeline

    def put(self, item: Any) -> None:
        self.timeline.append(('completed', item.job, item.status))


class _TimelineMemory(MemoryBackend):

    def __init__(self, timeline: List[Tuple[Any, ...]], failing: bool = False) -> None:
        self.timeline = timeline
        self.failing = failing

    def read(self) -> Any:
        return {}

    def recordRun(self, job: str) -> None:
        if self.failing:
            raise RuntimeError('memory backend is unavailable')
        self.timeline.append(('recordRun', job))


def _runDataWorkerOnce(monkeypatch, tmp_path, succeeds: bool, memoryFails: bool = False) -> List[Tuple[Any, ...]]:
    """Drives _dataJobWorker through exactly one job, returning the ordered
    timeline of its memory writes and completion signals.
    """

    def fakeExecute(job: Any, jobConfig: Any, databaseConfiguration: Any, log: Any = None, watermark: Any = None) -> JobOutcome:
        if not succeeds:
            raise RuntimeError('job blew up')
        return JobOutcome(job=job, status=JobStatus.COMPLETED, rowCount=1)

    monkeypatch.setattr('lightweight_etl.runner._executeDataJob', fakeExecute)

    timeline: List[Tuple[Any, ...]] = []

    with pytest.raises(_StopWorker):
        _dataJobWorker(_OneShotReadyQueue('job1'), _TimelineQueue(timeline), {'job1': _dataJobConfig()},
                        {}, tmp_path / 'worker.log', _TimelineMemory(timeline, failing=memoryFails))

    return timeline


def test_a_failed_data_job_does_not_record_a_run(monkeypatch, tmp_path):
    """recordRun used to run unconditionally, so a job that raised was still
    stamped as having just run -- and its `refresh` window then suppressed the
    retry. A job failing every time went quiet for `refresh` minutes instead of
    being retried on the next cycle.
    """
    timeline = _runDataWorkerOnce(monkeypatch, tmp_path, succeeds=False)

    assert ('recordRun', 'job1') not in timeline
    assert timeline == [('completed', 'job1', JobStatus.FAILED)]


def test_a_successful_data_job_records_its_run_before_signalling_completion(monkeypatch, tmp_path):
    """completedQueue is what DependencyGraph.run() watches to decide a cycle is
    done, and runDataJobs terminates the pool the moment it returns -- so a
    recordRun placed after the put could be killed before it landed, losing the
    stamp for a job that really did complete.
    """
    timeline = _runDataWorkerOnce(monkeypatch, tmp_path, succeeds=True)

    assert timeline == [('recordRun', 'job1'), ('completed', 'job1', JobStatus.COMPLETED)]


def test_a_completed_job_stays_completed_when_the_memory_backend_fails(monkeypatch, tmp_path):
    """The data did land, so reporting FAILED would be a worse lie than the
    missing stamp -- whose only consequence is an earlier re-run.
    """
    timeline = _runDataWorkerOnce(monkeypatch, tmp_path, succeeds=True, memoryFails=True)

    assert timeline == [('completed', 'job1', JobStatus.COMPLETED)]


def test_execute_data_job_streams_rather_than_materializing_the_whole_extract(monkeypatch):
    """The property that actually matters, and the one a fetchall() regression
    would break: loads begin *before* the source is exhausted.

    Asserting on peak memory would be flaky; asserting on strict alternation is
    deterministic and says the same thing. A buffered extract would record all
    four 'extract' events before the first 'load'.
    """

    timeline: List[Tuple[str, int]] = []
    sourceRows = [(index, 'name{}'.format(index)) for index in range(10)]

    class _StreamingFake:

        def __init__(self, connectionSettings: DatabaseConnectionConfig) -> None:
            self.connectionSettings = connectionSettings

        def __enter__(self) -> '_StreamingFake':
            return self

        def __exit__(self, *args: Any) -> None:
            return None

        def stream(self, query: str, chunkSize: int, parameters: Any = None) -> Tuple[List[str], Any]:

            def chunks() -> Any:
                for index in range(0, len(sourceRows), chunkSize):
                    chunk = sourceRows[index:index + chunkSize]
                    timeline.append(('extract', len(chunk)))
                    yield chunk

            return ['id', 'name'], chunks()

        def getAllColumnNames(self, table: str) -> List[str]:
            return ['id', 'name']

        def upsert(self, table: str, data: List[Tuple[Any, ...]], chunkSize: int = 100, columns: Any = None) -> None:
            timeline.append(('load', len(data)))

        def insert(self, table: str, data: List[Tuple[Any, ...]], chunkSize: int = 100, columns: Any = None) -> None:
            timeline.append(('load', len(data)))

        def truncate(self, table: str) -> None:
            return None

        def alter(self, query: str) -> None:
            return None

    monkeypatch.setattr('lightweight_etl.runner.Database', _StreamingFake)

    result = _executeDataJob('job1', _dataJobConfig(chunkSize=3), {'src': _dbConfig(), 'tgt': _dbConfig()})

    assert timeline == [
        ('extract', 3), ('load', 3),
        ('extract', 3), ('load', 3),
        ('extract', 3), ('load', 3),
        ('extract', 1), ('load', 1),
        ]
    assert result.rowCount == 10


def test_execute_data_job_never_holds_more_than_one_chunk_of_rows(fakeDatabases, monkeypatch):
    """chunkSize is the memory dial: no single load call should ever be handed
    more rows than it allows, however large the source is.
    """
    monkeypatch.setattr(_FakeDatabase, 'queryResult', [(index, 'n') for index in range(1000)])

    _executeDataJob('job1', _dataJobConfig(chunkSize=64), {'src': _dbConfig(), 'tgt': _dbConfig()})

    _, targetDatabase = fakeDatabases
    loadSizes = [len(call[2]) for call in targetDatabase.calls if call[0] == 'upsert']

    assert max(loadSizes) == 64
    assert sum(loadSizes) == 1000


def test_execute_data_job_returns_the_row_count_it_loaded(fakeDatabases, monkeypatch):
    monkeypatch.setattr(_FakeDatabase, 'queryResult', [(index, 'n') for index in range(150)])

    assert _executeDataJob('job1', _dataJobConfig(chunkSize=100), {'src': _dbConfig(), 'tgt': _dbConfig()}).rowCount == 150


def test_execute_data_job_loads_nothing_for_an_empty_source(fakeDatabases, monkeypatch):
    monkeypatch.setattr(_FakeDatabase, 'queryResult', [])

    result = _executeDataJob('job1', _dataJobConfig(), {'src': _dbConfig(), 'tgt': _dbConfig()})

    _, targetDatabase = fakeDatabases
    calledMethods = [call[0] for call in targetDatabase.calls]

    assert result.rowCount == 0
    assert 'upsert' not in calledMethods
    assert 'insert' not in calledMethods


def test_execute_data_job_applies_transforms_to_every_chunk_not_just_the_first(fakeDatabases, monkeypatch):
    """A transform hoisted out of the row loop could plausibly be applied once,
    to the first chunk only -- every row of every chunk must come through it.
    """
    monkeypatch.setattr(_FakeDatabase, 'queryResult', [(index, 'a') for index in range(250)])
    jobConfig = _dataJobConfig(chunkSize=100, sourceQueryColumnTransforms={'name': ['json:dumps']})

    _executeDataJob('job1', jobConfig, {'src': _dbConfig(), 'tgt': _dbConfig()})

    _, targetDatabase = fakeDatabases
    loadedRows = [row for call in targetDatabase.calls if call[0] == 'upsert' for row in call[2]]

    assert len(loadedRows) == 250
    assert all(row[1] == '"a"' for row in loadedRows)


def _watermarkJobConfig(**overrides: Any) -> DataJobConfig:
    fields = dict(active=True, sourceDatabase='src', targetDatabase='tgt', insertStrategy=InsertStrategy.UPSERT,
                   chunkSize=100, targetTableFinal='people', sourceQuery='select id, name from people where name > {{ watermark }}',
                   watermarkColumn='name', watermarkInitial='')
    fields.update(overrides)
    return DataJobConfig(**fields)


def test_execute_data_job_binds_the_watermark_rather_than_interpolating_it(fakeDatabases):
    """The value goes to the driver as a bound parameter, and the {{ watermark }}
    token is replaced by the dialect's own placeholder -- so one sourceQuery is
    portable across paramstyles and a string watermark can't alter the statement.
    """
    _executeDataJob('job1', _watermarkJobConfig(), {'src': _dbConfig(), 'tgt': _dbConfig()}, watermark='a')

    sourceDatabase, _ = fakeDatabases
    streamCall = next(call for call in sourceDatabase.calls if call[0] == 'stream')

    assert '{{ watermark }}' not in streamCall[1]
    assert streamCall[1] == 'select id, name from people where name > ?'
    assert streamCall[3] == ('a',)


def test_execute_data_job_returns_the_highest_value_of_the_watermark_column(fakeDatabases, monkeypatch):
    monkeypatch.setattr(_FakeDatabase, 'queryResult', [(1, 'b'), (2, 'd'), (3, 'c')])

    result = _executeDataJob('job1', _watermarkJobConfig(), {'src': _dbConfig(), 'tgt': _dbConfig()}, watermark='a')

    assert result.watermark == 'd'


def test_execute_data_job_takes_the_watermark_across_every_chunk_not_just_the_last(fakeDatabases, monkeypatch):
    """The max is tracked incrementally as chunks stream past; a chunk-local max
    would report whatever the final chunk happened to hold.
    """
    monkeypatch.setattr(_FakeDatabase, 'queryResult', [(1, 'a'), (2, 'z'), (3, 'b'), (4, 'c')])

    result = _executeDataJob('job1', _watermarkJobConfig(chunkSize=2), {'src': _dbConfig(), 'tgt': _dbConfig()}, watermark='')

    assert result.watermark == 'z'


def test_execute_data_job_takes_the_watermark_from_raw_rows_not_transformed_ones(fakeDatabases):
    """A transform may reformat the column. What goes back into the next run's
    predicate has to be a value the *source* can still compare against its own
    column, so the high-water mark is read before transforms run.
    """
    jobConfig = _watermarkJobConfig(sourceQueryColumnTransforms={'name': ['json:dumps']})

    result = _executeDataJob('job1', jobConfig, {'src': _dbConfig(), 'tgt': _dbConfig()}, watermark='')

    _, targetDatabase = fakeDatabases
    upsertCall = next(call for call in targetDatabase.calls if call[0] == 'upsert')

    assert upsertCall[2] == [(1, '"a"'), (2, '"b"')]
    assert result.watermark == 'b'


def test_execute_data_job_reports_no_watermark_when_the_source_had_no_rows(fakeDatabases, monkeypatch):
    """Nothing to advance to. The stored watermark must stay put rather than be
    overwritten with a null, which would re-extract everything next run.
    """
    monkeypatch.setattr(_FakeDatabase, 'queryResult', [])

    result = _executeDataJob('job1', _watermarkJobConfig(), {'src': _dbConfig(), 'tgt': _dbConfig()}, watermark='a')

    assert result.watermark is None


def test_execute_data_job_ignores_nulls_when_tracking_the_watermark(fakeDatabases, monkeypatch):
    monkeypatch.setattr(_FakeDatabase, 'queryResult', [(1, 'b'), (2, None), (3, 'a')])

    result = _executeDataJob('job1', _watermarkJobConfig(), {'src': _dbConfig(), 'tgt': _dbConfig()}, watermark='')

    assert result.watermark == 'b'


def test_execute_data_job_rejects_a_watermark_column_the_source_query_does_not_return(fakeDatabases):
    """Caught before any write, like the transform check -- otherwise the job
    loads rows and only then discovers it cannot tell how far it got.
    """
    jobConfig = _watermarkJobConfig(watermarkColumn='notSelected',
                                     sourceQuery='select id, name from people where id > {{ watermark }}')

    with pytest.raises(ConfigurationError, match='notSelected'):
        _executeDataJob('job1', jobConfig, {'src': _dbConfig(), 'tgt': _dbConfig()})

    _, targetDatabase = fakeDatabases
    calledMethods = [call[0] for call in targetDatabase.calls]

    assert 'upsert' not in calledMethods
    assert 'insert' not in calledMethods


class _WatermarkTimelineMemory(_TimelineMemory):

    def __init__(self, timeline: List[Tuple[Any, ...]], watermarks: Dict[str, Any], failing: bool = False) -> None:
        super().__init__(timeline, failing=failing)
        self.watermarks = watermarks

    def readWatermarks(self) -> Dict[str, Any]:
        return self.watermarks

    def recordWatermark(self, job: str, value: Any) -> None:
        self.timeline.append(('recordWatermark', job, value))


def _runWatermarkWorkerOnce(monkeypatch, tmp_path, succeeds: bool, watermarks: Dict[str, Any]) -> List[Tuple[Any, ...]]:

    def fakeExecute(job: Any, jobConfig: Any, databaseConfiguration: Any, log: Any = None, watermark: Any = None) -> JobOutcome:
        if not succeeds:
            raise RuntimeError('job blew up')
        return JobOutcome(job=job, status=JobStatus.COMPLETED, rowCount=3, watermark='reached-{}'.format(watermark))

    monkeypatch.setattr('lightweight_etl.runner._executeDataJob', fakeExecute)

    timeline: List[Tuple[Any, ...]] = []

    with pytest.raises(_StopWorker):
        _dataJobWorker(_OneShotReadyQueue('job1'), _TimelineQueue(timeline), {'job1': _watermarkJobConfig()},
                        {}, tmp_path / 'worker.log', _WatermarkTimelineMemory(timeline, watermarks))

    return timeline


def test_a_failed_incremental_job_does_not_advance_its_watermark(monkeypatch, tmp_path):
    """The one unrecoverable direction. Advancing past rows that were never
    loaded skips them permanently -- nothing comes back for them, because the
    next predicate starts beyond where they were.
    """
    timeline = _runWatermarkWorkerOnce(monkeypatch, tmp_path, succeeds=False, watermarks={'job1': 'stored'})

    assert not any(event[0] == 'recordWatermark' for event in timeline)
    assert timeline == [('completed', 'job1', JobStatus.FAILED)]


def test_a_successful_incremental_job_records_watermark_then_run_then_completion(monkeypatch, tmp_path):
    """Every step commits before the one after it, so each crash point falls
    backwards into re-reading rows already loaded -- harmless under upsert.
    """
    timeline = _runWatermarkWorkerOnce(monkeypatch, tmp_path, succeeds=True, watermarks={'job1': 'stored'})

    assert timeline == [
        ('recordWatermark', 'job1', 'reached-stored'),
        ('recordRun', 'job1'),
        ('completed', 'job1', JobStatus.COMPLETED),
        ]


def test_the_first_run_of_an_incremental_job_uses_watermark_initial(monkeypatch, tmp_path):
    """No stored watermark yet -- the job has to start from the configured
    initial value rather than from None, which would match nothing.
    """
    timeline = _runWatermarkWorkerOnce(monkeypatch, tmp_path, succeeds=True, watermarks={})

    assert ('recordWatermark', 'job1', 'reached-') in timeline


class _WatermarkBlindMemory(MemoryBackend):
    """A backend written before watermarks existed: read/recordRun only."""

    def read(self) -> Any:
        return {}

    def recordRun(self, job: str) -> None:
        return None


def test_run_data_jobs_refuses_an_incremental_job_whose_memory_cannot_store_a_watermark(tmp_path):
    """recordWatermark is non-abstract so old backends keep working for ordinary
    jobs; the cost is that a mismatch would otherwise surface inside a worker,
    after a job had already loaded rows, and then again every cycle. Checked up
    front instead, where it is a configuration error.
    """
    raw = {'workers': 1, 'jobs': {'loadOrders': {
        'active': True, 'sourceDatabase': 'src', 'targetDatabase': 'tgt', 'insertStrategy': 'upsert', 'chunkSize': 10,
        'targetTableFinal': 'orders', 'watermarkColumn': 'updated_at', 'watermarkInitial': '1970-01-01',
        'sourceQuery': 'select id, updated_at from orders where updated_at > {{ watermark }}',
        }}}
    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)

    with pytest.raises(ConfigurationError, match='does not implement recordWatermark'):
        runDataJobs(jobsFile=jobsFile, databaseConfiguration={}, logFile=tmp_path / 'runner.log',
                     memory=_WatermarkBlindMemory(), runForever=False)


def test_run_data_jobs_accepts_a_watermark_blind_memory_when_no_job_is_incremental(tmp_path):
    raw = {'workers': 1, 'jobs': {'noop': {'active': False, 'sourceDatabase': 'x', 'targetDatabase': 'y', 'insertStrategy': 'upsert',
                                            'chunkSize': 1, 'targetTableFinal': 't', 'sourceQuery': 'select 1'}}}
    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)

    runDataJobs(jobsFile=jobsFile, databaseConfiguration={}, logFile=tmp_path / 'runner.log',
                 memory=_WatermarkBlindMemory(), runForever=False)


def test_run_data_jobs_returns_a_result_rather_than_discarding_it(tmp_path):
    """runDataJobs used to return None, logging 'N completed, M failed' and
    throwing the numbers away -- so a scheduler wrapping it reported success on
    total failure. DependencyGraph already tracked this; nothing surfaced it.
    """
    raw = {'workers': 1, 'jobs': {'noop': {'active': False, 'sourceDatabase': 'x', 'targetDatabase': 'y', 'insertStrategy': 'upsert',
                                            'chunkSize': 1, 'targetTableFinal': 't', 'sourceQuery': 'select 1'}}}
    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)

    result = runDataJobs(jobsFile=jobsFile, databaseConfiguration={}, logFile=tmp_path / 'runner.log',
                          memory=FileMemory(memoryFile=tmp_path / 'runner.yaml'), runForever=False)

    assert result.outcomes == []
    assert result.succeeded is True


def test_run_scramble_jobs_returns_a_result(tmp_path):
    raw = {'workers': 1, 'jobs': {'noop': {'active': False, 'database': 'x', 'table': 'y', 'randomSalt': 's'}}}
    jobsFile = Configuration.validateJobConfiguration(raw, ScrambleJobsFile)

    result = runScrambleJobs(jobsFile=jobsFile, databaseConfiguration={}, logFile=tmp_path / 'runner.log', runForever=False)

    assert result.succeeded is True


def test_a_run_result_separates_completed_failed_and_skipped():
    result = RunResult(outcomes=[
        JobOutcome(job='a', status=JobStatus.COMPLETED, rowCount=400),
        JobOutcome(job='b', status=JobStatus.FAILED, error='OperationalError: boom'),
        JobOutcome(job='c', status=JobStatus.SKIPPED, error='predecessor(s) did not complete: b'),
        ])

    assert [outcome.job for outcome in result.completed] == ['a']
    assert [outcome.job for outcome in result.failed] == ['b']
    assert [outcome.job for outcome in result.skipped] == ['c']
    assert result.rowCount == 400


def test_a_skipped_job_counts_against_a_runs_success():
    """It didn't run, so the data it was meant to produce isn't there -- reporting
    the run as successful because nothing technically raised would be a lie a
    scheduler would act on.
    """
    result = RunResult(outcomes=[
        JobOutcome(job='a', status=JobStatus.COMPLETED),
        JobOutcome(job='b', status=JobStatus.SKIPPED, error='predecessor(s) did not complete: a'),
        ])

    assert result.succeeded is False


def test_a_cycle_with_no_active_jobs_succeeds_trivially():
    assert RunResult(outcomes=[]).succeeded is True


def test_a_job_outcome_reports_its_duration():
    outcome = JobOutcome(job='a', status=JobStatus.COMPLETED, startedAt=100.0, finishedAt=102.5)

    assert outcome.durationSeconds == 2.5


def test_a_job_outcome_survives_pickling(fakeDatabases):
    """Outcomes cross a process boundary on completedQueue. The error is a
    formatted string rather than the exception for exactly this reason: database
    drivers raise types that don't reliably pickle.
    """
    outcome = JobOutcome(job='a', status=JobStatus.FAILED, error='OperationalError: boom', rowCount=3)

    assert pickle.loads(pickle.dumps(outcome)) == outcome


def test_the_data_worker_reports_row_count_and_error_on_its_outcome(monkeypatch, tmp_path):

    def fakeExecute(job: Any, jobConfig: Any, databaseConfiguration: Any, log: Any = None, watermark: Any = None) -> JobOutcome:
        return JobOutcome(job=job, status=JobStatus.COMPLETED, rowCount=42)

    monkeypatch.setattr('lightweight_etl.runner._executeDataJob', fakeExecute)
    outcomes: List[Any] = []

    class _CollectingQueue:
        def put(self, item: Any) -> None:
            outcomes.append(item)

    with pytest.raises(_StopWorker):
        _dataJobWorker(_OneShotReadyQueue('job1'), _CollectingQueue(), {'job1': _dataJobConfig()},
                        {}, tmp_path / 'worker.log', _TimelineMemory([]))

    (outcome,) = outcomes

    assert outcome.job == 'job1'
    assert outcome.status == JobStatus.COMPLETED
    assert outcome.rowCount == 42
    assert outcome.error is None
    assert outcome.finishedAt >= outcome.startedAt


def test_the_data_worker_records_the_failure_text_on_its_outcome(monkeypatch, tmp_path):

    def fakeExecute(job: Any, jobConfig: Any, databaseConfiguration: Any, log: Any = None, watermark: Any = None) -> JobOutcome:
        raise RuntimeError('the source went away')

    monkeypatch.setattr('lightweight_etl.runner._executeDataJob', fakeExecute)
    outcomes: List[Any] = []

    class _CollectingQueue:
        def put(self, item: Any) -> None:
            outcomes.append(item)

    with pytest.raises(_StopWorker):
        _dataJobWorker(_OneShotReadyQueue('job1'), _CollectingQueue(), {'job1': _dataJobConfig()},
                        {}, tmp_path / 'worker.log', _TimelineMemory([]))

    (outcome,) = outcomes

    assert outcome.status == JobStatus.FAILED
    assert outcome.error == 'RuntimeError: the source went away'
    assert outcome.rowCount == 0


def test_termination_handling_sets_a_flag_and_restores_the_previous_handlers(tmp_path):
    """The handler only flips a flag; teardown happens at the run loop's next
    checkpoint. Doing it inside the handler would run pool teardown on whatever
    frame happened to be executing, including one inside multiprocessing itself.
    """
    log = Log(logFile=tmp_path / 'x.log')
    originalInterrupt = signal.getsignal(signal.SIGINT)
    originalTerminate = signal.getsignal(signal.SIGTERM)

    with _terminationHandling(log) as termination:
        assert termination['terminating'] is False
        assert signal.getsignal(signal.SIGTERM) is not originalTerminate

        os.kill(os.getpid(), signal.SIGTERM)

        assert termination['terminating'] is True

    assert signal.getsignal(signal.SIGINT) is originalInterrupt
    assert signal.getsignal(signal.SIGTERM) is originalTerminate


def test_termination_handling_restores_handlers_even_when_the_body_raises(tmp_path):
    log = Log(logFile=tmp_path / 'x.log')
    originalTerminate = signal.getsignal(signal.SIGTERM)

    with pytest.raises(RuntimeError):
        with _terminationHandling(log):
            raise RuntimeError('boom')

    assert signal.getsignal(signal.SIGTERM) is originalTerminate


def test_termination_handling_is_a_no_op_off_the_main_thread(tmp_path):
    """signal.signal raises anywhere but the main thread of the main interpreter,
    and a library has no business failing because its caller ran it on a worker
    thread -- it just leaves signal handling to them.
    """
    log = Log(logFile=tmp_path / 'x.log')
    failures = []

    def useOffMainThread():
        try:
            with _terminationHandling(log) as termination:
                assert termination['terminating'] is False
        except Exception as error:
            failures.append(error)

    thread = threading.Thread(target=useOffMainThread)
    thread.start()
    thread.join(timeout=5)

    assert failures == []


def test_run_data_jobs_stops_on_sigterm_rather_than_running_forever(tmp_path):
    """Without this, a Ctrl-C or a container's SIGTERM tears down the parent while
    its pool workers are mid-job, orphaning them. In Kubernetes, SIGTERM arrives
    on every ordinary pod shutdown, so it's the common path.
    """
    raw = {'workers': 1, 'cycleSleepSeconds': 0.1,
           'jobs': {'noop': {'active': False, 'sourceDatabase': 'x', 'targetDatabase': 'y', 'insertStrategy': 'upsert',
                              'chunkSize': 1, 'targetTableFinal': 't', 'sourceQuery': 'select 1'}}}
    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)

    threading.Timer(1.5, lambda: os.kill(os.getpid(), signal.SIGTERM)).start()

    result = runDataJobs(jobsFile=jobsFile, databaseConfiguration={}, logFile=tmp_path / 'runner.log',
                          memory=FileMemory(memoryFile=tmp_path / 'runner.yaml'), runForever=True)

    assert result.succeeded is True
