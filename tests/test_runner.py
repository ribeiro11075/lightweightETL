import contextlib
import json
import logging
import os
import pickle
import signal
import sqlite3
import threading
import time
from typing import Any, Dict, Iterator, List, Tuple

import pytest

from bauta.configuration import Configuration, ConfigurationError, DatabaseConnectionConfig, DatabaseType, DataJobConfig, DataJobsFile, \
    InsertStrategy
from bauta.dependencyGraph import DependencyGraph, JobOutcome, JobStatus
from bauta.memory import FileMemory, MemoryBackend
from bauta.runner import PIPELINE_DEPTH, RunResult, _initializeWorker, _jobProcess, _runCycle, _runDataJob, _terminationHandling, _executeDataJob, runDataJobs
from bauta.transform import TransformError


def test_worker_functions_are_picklable():
    """The spawn and forkserver start methods (macOS, Windows, and Linux from
    Python 3.14) unpickle what a worker runs by module+qualname in the child
    process -- a closure or nested function wouldn't survive that.
    """
    pickle.dumps(_runDataJob)
    pickle.dumps(_initializeWorker)
    pickle.dumps(_jobProcess)


def test_run_data_jobs_completes_with_zero_active_jobs_when_not_forever(tmp_path):
    raw = {'workers': 2, 'jobs': {'noop': {'active': False, 'sourceDatabase': 'x', 'targetDatabase': 'y', 'insertStrategy': 'upsert',
                                            'chunkSize': 1, 'targetTableFinal': 't', 'sourceQuery': 'select 1'}}}
    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)

    runDataJobs(jobsFile=jobsFile, databaseConfiguration={}, logFile=tmp_path / 'runner.log',
                memory=FileMemory(memoryFile=tmp_path / 'runner.yaml'), runForever=False)


class _FakeDatabase:
    """Records calls instead of touching a real connection, so _executeDataJob
    can be exercised without mocking at the driver level.
    """

    queryResult: List[Tuple[Any, ...]] = [(1, 'a'), (2, 'b')]
    columnNames: List[str] = ['id', 'name']

    def __init__(self, connectionSettings: DatabaseConnectionConfig) -> None:
        self.connectionSettings = connectionSettings
        self.calls: List[Tuple[Any, ...]] = []

    def __enter__(self) -> '_FakeDatabase':
        return self

    def __exit__(self, *args: Any) -> None:
        return None

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

    def getAllColumnNames(self, table: str) -> List[str]:
        self.calls.append(('getAllColumnNames', table))
        return self.columnNames

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

    monkeypatch.setattr('bauta.runner.Database', _TrackedFakeDatabase)

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


def _runJobWithTimeline(jobConfig: DataJobConfig, memory: '_TimelineMemory') -> List[Tuple[Any, ...]]:
    """Runs _runDataJob for one job, returning the ordered timeline of its
    memory writes, ending with its completion -- the point the parent sees it.
    """

    outcome = _runDataJob('job1', jobConfig, {}, memory)
    memory.timeline.append(('completed', outcome.job, outcome.status))

    return memory.timeline


def _runDataWorkerOnce(monkeypatch, succeeds: bool, memoryFails: bool = False) -> List[Tuple[Any, ...]]:

    def fakeExecute(job: Any, jobConfig: Any, databaseConfiguration: Any, watermark: Any = None) -> JobOutcome:
        if not succeeds:
            raise RuntimeError('job blew up')
        return JobOutcome(job=job, status=JobStatus.COMPLETED, rowCount=1)

    monkeypatch.setattr('bauta.runner._executeDataJob', fakeExecute)

    return _runJobWithTimeline(_dataJobConfig(), _TimelineMemory([], failing=memoryFails))


def test_a_failed_data_job_does_not_record_a_run(monkeypatch, tmp_path):
    """recordRun used to run unconditionally, so a job that raised was still
    stamped as having just run -- and its `refresh` window then suppressed the
    retry. A job failing every time went quiet for `refresh` minutes instead of
    being retried on the next cycle.
    """
    timeline = _runDataWorkerOnce(monkeypatch, succeeds=False)

    assert ('recordRun', 'job1') not in timeline
    assert timeline == [('completed', 'job1', JobStatus.FAILED)]


def test_a_successful_data_job_records_its_run_before_signalling_completion(monkeypatch, tmp_path):
    """The parent treats a returned outcome as the job being over, and may shut
    the pool down straight after -- so a recordRun placed after the return could
    be lost for a job that really did complete.
    """
    timeline = _runDataWorkerOnce(monkeypatch, succeeds=True)

    assert timeline == [('recordRun', 'job1'), ('completed', 'job1', JobStatus.COMPLETED)]


def test_a_completed_job_stays_completed_when_the_memory_backend_fails(monkeypatch, tmp_path):
    """The data did land, so reporting FAILED would be a worse lie than the
    missing stamp -- whose only consequence is an earlier re-run.
    """
    timeline = _runDataWorkerOnce(monkeypatch, succeeds=True, memoryFails=True)

    assert timeline == [('completed', 'job1', JobStatus.COMPLETED)]


def test_execute_data_job_streams_rather_than_materializing_the_whole_extract(monkeypatch):
    """The property that actually matters, and the one a fetchall() regression
    would break: loads begin *before* the source is exhausted.

    Asserting on peak memory would be flaky. A buffered extract would record all
    four 'extract' events before the first 'load', so this asserts that a load
    happens while extracting is still going on, and that the reader never runs
    further ahead than the pipeline's depth allows.

    Not strict alternation: the reader, the masker and the writer overlap, so
    the exact interleaving depends on thread scheduling. Strict alternation is
    what BAUTA_PIPELINE=0 restores, which the next test checks.
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

    monkeypatch.setenv('BAUTA_PIPELINE', '1')
    monkeypatch.setattr('bauta.runner.Database', _StreamingFake)

    result = _executeDataJob('job1', _dataJobConfig(chunkSize=3), {'src': _dbConfig(), 'tgt': _dbConfig()})

    assert [event for event, _ in timeline].count('extract') == 4
    assert [size for event, size in timeline if event == 'load'] == [3, 3, 3, 1]
    assert result.rowCount == 10

    # A load before the last extract: the source was never drained first.
    lastExtract = max(index for index, (event, _) in enumerate(timeline) if event == 'extract')
    firstLoad = min(index for index, (event, _) in enumerate(timeline) if event == 'load')
    assert firstLoad < lastExtract, 'loading only began once extracting had finished: {}'.format(timeline)

    # And no more than a few chunks are ever in hand at once: the streamed
    # extract's memory promise, which the pipeline widens but must not drop.
    ahead = 0
    for event, _ in timeline:
        ahead += 1 if event == 'extract' else -1
        assert ahead <= 2 * PIPELINE_DEPTH + 1, 'held {} chunks at once: {}'.format(ahead, timeline)


def test_the_pipeline_can_be_turned_off(monkeypatch):
    """BAUTA_PIPELINE=0 puts reading, masking and writing back in turn, for
    diagnosing a problem without the worker thread in the picture. Then the
    interleaving is strict, and deterministic to assert on.
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

    monkeypatch.setenv('BAUTA_PIPELINE', '0')
    monkeypatch.setattr('bauta.runner.Database', _StreamingFake)

    _executeDataJob('job1', _dataJobConfig(chunkSize=3), {'src': _dbConfig(), 'tgt': _dbConfig()})

    assert timeline == [
        ('extract', 3), ('load', 3),
        ('extract', 3), ('load', 3),
        ('extract', 3), ('load', 3),
        ('extract', 1), ('load', 1),
        ]


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

    def fakeExecute(job: Any, jobConfig: Any, databaseConfiguration: Any, watermark: Any = None) -> JobOutcome:
        if not succeeds:
            raise RuntimeError('job blew up')
        return JobOutcome(job=job, status=JobStatus.COMPLETED, rowCount=3, watermark='reached-{}'.format(watermark))

    monkeypatch.setattr('bauta.runner._executeDataJob', fakeExecute)

    return _runJobWithTimeline(_watermarkJobConfig(), _WatermarkTimelineMemory([], watermarks))


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
    """Outcomes cross a process boundary from worker to parent. The error is a
    formatted string rather than the exception for exactly this reason: database
    drivers raise types that don't reliably pickle.
    """
    outcome = JobOutcome(job='a', status=JobStatus.FAILED, error='OperationalError: boom', rowCount=3)

    assert pickle.loads(pickle.dumps(outcome)) == outcome


def test_the_data_worker_reports_row_count_and_error_on_its_outcome(monkeypatch, tmp_path):

    def fakeExecute(job: Any, jobConfig: Any, databaseConfiguration: Any, watermark: Any = None) -> JobOutcome:
        return JobOutcome(job=job, status=JobStatus.COMPLETED, rowCount=42)

    monkeypatch.setattr('bauta.runner._executeDataJob', fakeExecute)

    outcome = _runDataJob('job1', _dataJobConfig(), {}, _TimelineMemory([]))

    assert outcome.job == 'job1'
    assert outcome.status == JobStatus.COMPLETED
    assert outcome.rowCount == 42
    assert outcome.error is None
    assert outcome.finishedAt >= outcome.startedAt


def test_the_data_worker_records_the_failure_text_on_its_outcome(monkeypatch, tmp_path):

    def fakeExecute(job: Any, jobConfig: Any, databaseConfiguration: Any, watermark: Any = None) -> JobOutcome:
        raise RuntimeError('the source went away')

    monkeypatch.setattr('bauta.runner._executeDataJob', fakeExecute)

    outcome = _runDataJob('job1', _dataJobConfig(), {}, _TimelineMemory([]))

    assert outcome.status == JobStatus.FAILED
    assert outcome.error == 'RuntimeError: the source went away'
    assert outcome.rowCount == 0


def test_termination_handling_sets_a_flag_and_restores_the_previous_handlers(tmp_path):
    """The handler only flips a flag; teardown happens at the run loop's next
    checkpoint. Doing it inside the handler would run pool teardown on whatever
    frame happened to be executing, including one inside multiprocessing itself.
    """
    originalInterrupt = signal.getsignal(signal.SIGINT)
    originalTerminate = signal.getsignal(signal.SIGTERM)

    with _terminationHandling() as termination:
        assert termination['terminating'] is False
        assert signal.getsignal(signal.SIGTERM) is not originalTerminate

        os.kill(os.getpid(), signal.SIGTERM)

        assert termination['terminating'] is True

    assert signal.getsignal(signal.SIGINT) is originalInterrupt
    assert signal.getsignal(signal.SIGTERM) is originalTerminate


def test_termination_handling_restores_handlers_even_when_the_body_raises(tmp_path):
    originalTerminate = signal.getsignal(signal.SIGTERM)

    with pytest.raises(RuntimeError):
        with _terminationHandling():
            raise RuntimeError('boom')

    assert signal.getsignal(signal.SIGTERM) is originalTerminate


def test_termination_handling_is_a_no_op_off_the_main_thread(tmp_path):
    """signal.signal raises anywhere but the main thread of the main interpreter,
    and a library has no business failing because its caller ran it on a worker
    thread -- it just leaves signal handling to them.
    """
    failures = []

    def useOffMainThread():
        try:
            with _terminationHandling() as termination:
                assert termination['terminating'] is False
        except Exception as error:
            failures.append(error)

    thread = threading.Thread(target=useOffMainThread)
    thread.start()
    thread.join(timeout=5)

    assert failures == []


def test_run_data_jobs_stops_on_sigterm_rather_than_running_forever(tmp_path):
    """Without this, a Ctrl-C or a container's SIGTERM tears down the parent while
    its workers are mid-job, orphaning them. In Kubernetes, SIGTERM arrives on
    every ordinary pod shutdown, so it's the common path.
    """
    raw = {'workers': 1, 'cycleSleepSeconds': 0.1,
           'jobs': {'noop': {'active': False, 'sourceDatabase': 'x', 'targetDatabase': 'y', 'insertStrategy': 'upsert',
                              'chunkSize': 1, 'targetTableFinal': 't', 'sourceQuery': 'select 1'}}}
    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)

    threading.Timer(1.5, lambda: os.kill(os.getpid(), signal.SIGTERM)).start()

    result = runDataJobs(jobsFile=jobsFile, databaseConfiguration={}, logFile=tmp_path / 'runner.log',
                          memory=FileMemory(memoryFile=tmp_path / 'runner.yaml'), runForever=True)

    assert result.succeeded is True
    assert result.interrupted is True


def _retryJobConfig(**overrides: Any) -> DataJobConfig:
    fields = dict(active=True, sourceDatabase='src', targetDatabase='tgt', insertStrategy=InsertStrategy.UPSERT,
                   chunkSize=100, targetTableFinal='people', sourceQuery='select * from people',
                   retries=2, retryDelaySeconds=0.0)
    fields.update(overrides)
    return DataJobConfig(**fields)


def _runRetryWorker(monkeypatch, tmp_path, attempt: Any, jobConfig: Any = None) -> List[Any]:

    monkeypatch.setattr('bauta.runner._executeDataJob', lambda job, config, databases, watermark=None: attempt())

    return [_runDataJob('job1', jobConfig or _retryJobConfig(), {}, _TimelineMemory([]))]


def test_a_transient_failure_is_retried_and_can_succeed(monkeypatch, tmp_path):
    """The common production failure is a dropped connection, not a bug. Losing a
    nightly load to one blip -- until the next refresh window -- is the outcome
    retries exist to prevent.
    """
    calls = []

    def flakyThenFine():
        calls.append(1)
        if len(calls) < 3:
            raise OSError('connection reset by peer')
        return JobOutcome(job='job1', status=JobStatus.COMPLETED, rowCount=7)

    (outcome,) = _runRetryWorker(monkeypatch, tmp_path, flakyThenFine)

    assert outcome.status == JobStatus.COMPLETED
    assert outcome.rowCount == 7
    assert outcome.attempts == 3


def test_retries_are_bounded_and_the_last_error_is_reported(monkeypatch, tmp_path):
    calls = []

    def alwaysFails():
        calls.append(1)
        raise OSError('connection reset by peer')

    (outcome,) = _runRetryWorker(monkeypatch, tmp_path, alwaysFails)

    assert len(calls) == 3
    assert outcome.status == JobStatus.FAILED
    assert outcome.error == 'OSError: connection reset by peer'
    assert outcome.attempts == 3


def test_a_job_with_no_retries_configured_is_attempted_once(monkeypatch, tmp_path):
    calls = []

    def alwaysFails():
        calls.append(1)
        raise OSError('connection reset by peer')

    (outcome,) = _runRetryWorker(monkeypatch, tmp_path, alwaysFails, jobConfig=_retryJobConfig(retries=0))

    assert len(calls) == 1
    assert outcome.attempts == 1


@pytest.mark.parametrize('error', [
    ConfigurationError('watermarkColumn is not among the columns sourceQuery returns'),
    TransformError('sourceQueryColumnTransforms references column(s) not present'),
    ])
def test_a_deterministic_error_is_not_retried(monkeypatch, tmp_path, error):
    """These are raised by this package and cannot succeed on a second attempt.
    Retrying only delays the failure and buries the real message under repeats.
    """
    calls = []

    def alwaysFails():
        calls.append(1)
        raise error

    (outcome,) = _runRetryWorker(monkeypatch, tmp_path, alwaysFails)

    assert len(calls) == 1
    assert outcome.status == JobStatus.FAILED
    assert outcome.attempts == 1


def test_retry_backoff_grows_between_attempts(monkeypatch, tmp_path):
    """Exponential, so a database that is down doesn't get hammered at a fixed
    interval while it tries to come back.
    """
    delays = []
    monkeypatch.setattr('bauta.runner.time.sleep', lambda seconds: delays.append(seconds))

    def alwaysFails():
        raise OSError('down')

    _runRetryWorker(monkeypatch, tmp_path, alwaysFails, jobConfig=_retryJobConfig(retries=3, retryDelaySeconds=2.0))

    assert delays == [2.0, 4.0, 8.0]


def test_retry_backoff_stops_growing_at_five_minutes(monkeypatch, tmp_path):
    delays = []
    monkeypatch.setattr('bauta.runner.time.sleep', lambda seconds: delays.append(seconds))

    def alwaysFails():
        raise OSError('down')

    _runRetryWorker(monkeypatch, tmp_path, alwaysFails, jobConfig=_retryJobConfig(retries=40, retryDelaySeconds=5.0))

    assert delays[:7] == [5.0, 10.0, 20.0, 40.0, 80.0, 160.0, 300.0]
    assert set(delays[6:]) == {300.0} and len(delays) == 40


def test_the_stored_watermark_is_read_again_on_each_attempt(monkeypatch):
    """A memory backend kept in a database fails transiently like any other
    database, so reading it belongs inside the retried attempt.
    """

    class _FlakyWatermarks(_WatermarkTimelineMemory):
        reads = 0

        def readWatermarks(self) -> Dict[str, Any]:
            self.reads += 1
            if self.reads == 1:
                raise OSError('memory database unavailable')
            return {'job1': 'stored'}

    monkeypatch.setattr('bauta.runner._executeDataJob',
                        lambda job, config, databases, watermark=None: JobOutcome(job=job, status=JobStatus.COMPLETED, watermark=watermark))
    memory = _FlakyWatermarks([], {})

    outcome = _runDataJob('job1', _watermarkJobConfig(retries=1, retryDelaySeconds=0.0), {}, memory)

    assert outcome.status == JobStatus.COMPLETED
    assert outcome.attempts == 2
    assert ('recordWatermark', 'job1', 'stored') in memory.timeline


def _sqliteJob(databasePath: Any, **overrides: Any) -> Dict[str, Any]:
    fields = dict(active=True, sourceDatabase='lite', targetDatabase='lite', insertStrategy='upsert', chunkSize=10,
                  sourceQuery='select id, name from source', targetTableFinal='target')
    fields.update(overrides)
    return fields


@pytest.fixture
def sqliteDatabase(tmp_path):
    path = tmp_path / 'lite.db'
    connection = sqlite3.connect(path)
    # WAL up front: jobs starting together would otherwise race to switch the
    # file's journal mode, which SQLite refuses without waiting.
    connection.execute('PRAGMA journal_mode=WAL')
    connection.executescript('create table source (id integer primary key, name text);'
                             "insert into source values (1, 'Ann'), (2, 'Bo');"
                             'create table target (id integer primary key, name text);')
    connection.close()

    return {'lite': DatabaseConnectionConfig(type=DatabaseType.SQLITE, database=str(path))}


def test_a_worker_that_dies_fails_its_job_instead_of_hanging_the_run(tmp_path, sqliteDatabase):
    """A worker killed mid-job -- by the OOM killer, say -- never reports back.
    The run used to wait for it forever, so a cron job never exited and the next
    invocations piled up behind it. Now the job fails, its dependents are
    skipped, and the run returns.
    """
    raw = {'workers': 1, 'jobs': {
        'crashes': _sqliteJob(sqliteDatabase, sourceQueryColumnTransforms={'name': ['tests.crashingTransforms:exitAbruptly']}),
        'dependent': _sqliteJob(sqliteDatabase, predecessors=['crashes']),
        }}
    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)
    results: List[RunResult] = []

    thread = threading.Thread(target=lambda: results.append(runDataJobs(
        jobsFile=jobsFile, databaseConfiguration=sqliteDatabase, memory=FileMemory(tmp_path / 'memory.yaml'))), daemon=True)
    thread.start()
    thread.join(timeout=60)

    assert not thread.is_alive(), 'the run is still waiting on a worker that died'
    statuses = {outcome.job: outcome.status for outcome in results[0].outcomes}
    assert statuses == {'crashes': JobStatus.FAILED, 'dependent': JobStatus.SKIPPED}
    assert 'process exited abruptly' in results[0].failed[0].error


def _runJobs(jobs: Dict[str, Any], databases: Dict[str, Any], tmp_path: Any, workers: int = 1) -> RunResult:
    jobsFile = Configuration.validateJobConfiguration({'workers': workers, 'jobs': jobs}, DataJobsFile)

    return runDataJobs(jobsFile=jobsFile, databaseConfiguration=databases, memory=FileMemory(tmp_path / 'memory.yaml'))


def test_a_dead_worker_takes_no_other_job_with_it(tmp_path, sqliteDatabase):
    """Each job has its own process, so a crash fails only the job it was
    running -- not one running beside it, and not one that starts later.
    """
    result = _runJobs({
        'crashes': _sqliteJob(sqliteDatabase, sourceQueryColumnTransforms={'name': ['tests.crashingTransforms:exitAbruptly']}),
        'alongside': _sqliteJob(sqliteDatabase, sourceQueryColumnTransforms={'name': ['tests.crashingTransforms:slowly']}),
        'later': _sqliteJob(sqliteDatabase, predecessors=['alongside']),
        }, sqliteDatabase, tmp_path, workers=2)

    statuses = {outcome.job: outcome.status for outcome in result.outcomes}
    assert statuses == {'crashes': JobStatus.FAILED, 'alongside': JobStatus.COMPLETED, 'later': JobStatus.COMPLETED}


def test_a_job_past_its_timeout_is_stopped_and_fails(tmp_path, sqliteDatabase):
    """A hung query would otherwise hold a run -- and a container's shutdown --
    for as long as the database lets it.
    """
    startedAt = time.time()

    result = _runJobs({
        'hangs': _sqliteJob(sqliteDatabase, timeoutSeconds=1, sourceQueryColumnTransforms={'name': ['tests.crashingTransforms:hang']}),
        'dependent': _sqliteJob(sqliteDatabase, predecessors=['hangs']),
        'unrelated': _sqliteJob(sqliteDatabase, timeoutSeconds=60),
        }, sqliteDatabase, tmp_path, workers=2)

    statuses = {outcome.job: outcome.status for outcome in result.outcomes}
    assert statuses == {'hangs': JobStatus.FAILED, 'dependent': JobStatus.SKIPPED, 'unrelated': JobStatus.COMPLETED}
    assert result.failed[0].error == 'Timeout: stopped after exceeding timeoutSeconds (1)'
    assert time.time() - startedAt < 30


def test_no_more_than_workers_jobs_run_at_once(tmp_path, sqliteDatabase):
    result = _runJobs({name: _sqliteJob(sqliteDatabase, sourceQueryColumnTransforms={'name': ['tests.crashingTransforms:slowly']})
                       for name in ('a', 'b', 'c')}, sqliteDatabase, tmp_path, workers=2)

    spans = sorted((outcome.startedAt, outcome.finishedAt) for outcome in result.outcomes)
    assert result.succeeded
    assert all(sum(1 for start, end in spans if start <= moment < end) <= 2 for moment, _ in spans)
    assert spans[2][0] >= min(spans[0][1], spans[1][1]) - 0.05


def test_a_signal_stops_new_jobs_from_starting_and_reports_them_skipped():
    """After SIGTERM a container has seconds left: jobs already running are
    allowed to finish, but nothing new is started.
    """
    graph = DependencyGraph({'first': _dataJobConfig(), 'second': _dataJobConfig(predecessors=['first'])})

    _runCycle(graph, 1, {}, _TimelineMemory([]), {'terminating': True}, logLevel=0)

    assert {outcome.job: outcome.status for outcome in graph.outcomes} == {'first': JobStatus.SKIPPED, 'second': JobStatus.SKIPPED}
    assert 'stopped by a signal' in graph.outcomes[0].error


def test_worker_log_records_reach_the_parents_handlers_in_its_format(tmp_path, sqliteDatabase):
    """Workers used to log through a handler they didn't have: INFO lines were
    dropped, and warnings and errors reached stderr as bare text even under
    --log-format json. They now travel to the parent and are written there.
    """
    raw = {'workers': 1, 'jobs': {
        'copies': _sqliteJob(sqliteDatabase),
        'fails': _sqliteJob(sqliteDatabase, sourceQuery='select id, name from missing_table'),
        }}
    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)
    logPath = tmp_path / 'run.log'

    runDataJobs(jobsFile=jobsFile, databaseConfiguration=sqliteDatabase, memory=FileMemory(tmp_path / 'memory.yaml'),
                logFile=logPath, logFormat='json')

    records = [json.loads(line) for line in logPath.read_text().splitlines()]
    messages = [record['message'] for record in records]

    assert 'Starting copies' in messages
    assert any(record.get('job') == 'copies' and record.get('status') == 'completed' for record in records)
    failure = next(record for record in records if record['message'].startswith('Failed to complete fails'))
    assert 'no such table' in failure['exception']


def _maskedSqliteJob(databases, key='an-original-masking-key', **overrides):
    return _sqliteJob(databases, masking={'key': key, 'columns': {'id': 'key', 'name': 'fakeName'}}, **overrides)


def test_a_masked_job_records_the_key_it_completed_under(tmp_path, sqliteDatabase):
    from bauta.masking import keyFingerprint, splitMaskingIdentity

    _runJobs({'masked': _maskedSqliteJob(sqliteDatabase)}, sqliteDatabase, tmp_path)

    recorded = FileMemory(tmp_path / 'memory.yaml').readKeyFingerprints()
    assert list(recorded) == ['masked']
    assert splitMaskingIdentity(recorded['masked'])[0] == keyFingerprint('an-original-masking-key')


def test_a_changed_key_stops_an_upsert_job_before_anything_runs(tmp_path, sqliteDatabase):
    """Old rows masked under the old key and new ones under the new key would
    no longer match -- silently, which is why it has to be acknowledged.
    """
    _runJobs({'masked': _maskedSqliteJob(sqliteDatabase)}, sqliteDatabase, tmp_path)

    with pytest.raises(ConfigurationError, match='masking key changed since the last run of upsert job'):
        _runJobs({'masked': _maskedSqliteJob(sqliteDatabase, key='a-rotated-masking-key')}, sqliteDatabase, tmp_path)


def test_a_changed_key_is_accepted_when_acknowledged_and_then_recorded(tmp_path, sqliteDatabase):
    from bauta.masking import keyFingerprint, splitMaskingIdentity

    _runJobs({'masked': _maskedSqliteJob(sqliteDatabase)}, sqliteDatabase, tmp_path)
    jobsFile = Configuration.validateJobConfiguration(
        {'workers': 1, 'jobs': {'masked': _maskedSqliteJob(sqliteDatabase, key='a-rotated-masking-key')}}, DataJobsFile)

    result = runDataJobs(jobsFile=jobsFile, databaseConfiguration=sqliteDatabase, memory=FileMemory(tmp_path / 'memory.yaml'),
                         acceptKeyChange=True)

    assert result.succeeded
    recorded = FileMemory(tmp_path / 'memory.yaml').readKeyFingerprints()
    assert splitMaskingIdentity(recorded['masked'])[0] == keyFingerprint('a-rotated-masking-key')


@contextlib.contextmanager
def _warningsFromThePackage() -> Iterator[List[str]]:
    """The package's own warnings. Its logger doesn't propagate -- log.py keeps
    its records out of a host application's handlers -- so caplog, which listens
    at the root, never sees them.
    """

    messages: List[str] = []

    class _Collect(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            messages.append(record.getMessage())

    handler = _Collect(level=logging.WARNING)
    packageLogger = logging.getLogger('bauta')
    packageLogger.addHandler(handler)
    try:
        yield messages
    finally:
        packageLogger.removeHandler(handler)


def test_a_masked_job_records_which_implementation_masked_it(tmp_path, sqliteDatabase):
    """The key fingerprint covers the key, not the code that applied it. If the
    two implementations ever disagreed, the fingerprint would not change, and
    nothing else would show it.
    """
    from bauta.masking import maskingImplementation, splitMaskingIdentity

    _runJobs({'masked': _maskedSqliteJob(sqliteDatabase)}, sqliteDatabase, tmp_path)

    recorded = FileMemory(tmp_path / 'memory.yaml').readKeyFingerprints()
    assert splitMaskingIdentity(recorded['masked'])[1] == maskingImplementation()


def test_a_changed_implementation_is_noted_but_does_not_stop_an_upsert_job(tmp_path, sqliteDatabase, monkeypatch):
    """Installing or removing the extension changes the implementation and
    nothing else: the two are tested to produce identical masks. Refusing every
    upsert job on that would be friction for no safety, so it warns instead --
    the warning being the only trace there would be if they ever disagreed.
    """
    import bauta.masking as masking

    monkeypatch.setattr(masking, 'maskingImplementation', lambda: 'python')
    monkeypatch.setattr('bauta.runner.maskingImplementation', lambda: 'python')
    _runJobs({'masked': _maskedSqliteJob(sqliteDatabase)}, sqliteDatabase, tmp_path)

    monkeypatch.setattr(masking, 'maskingImplementation', lambda: 'bauta-rs/9.9.9')
    monkeypatch.setattr('bauta.runner.maskingImplementation', lambda: 'bauta-rs/9.9.9')
    with _warningsFromThePackage() as warnings:
        result = _runJobs({'masked': _maskedSqliteJob(sqliteDatabase)}, sqliteDatabase, tmp_path)

    assert result.succeeded
    noted = [message for message in warnings if 'Masking implementation changed' in message]
    assert len(noted) == 1, warnings
    assert 'now bauta-rs/9.9.9' in noted[0]


def test_state_recorded_before_the_implementation_was_still_reads(tmp_path, sqliteDatabase):
    """Memory written before the implementation was recorded holds a bare
    fingerprint. It must still guard the key, and must not read as an
    implementation change -- which would warn on every job the first run after
    an upgrade.
    """
    from bauta.masking import keyFingerprint

    FileMemory(tmp_path / 'memory.yaml').recordKeyFingerprint('masked', keyFingerprint('an-original-masking-key'))

    with _warningsFromThePackage() as warnings:
        result = _runJobs({'masked': _maskedSqliteJob(sqliteDatabase)}, sqliteDatabase, tmp_path)
    assert result.succeeded
    assert not [message for message in warnings if 'Masking implementation changed' in message]

    FileMemory(tmp_path / 'memory.yaml').recordKeyFingerprint('masked', keyFingerprint('an-original-masking-key'))
    with pytest.raises(ConfigurationError, match='masking key changed since the last run of upsert job'):
        _runJobs({'masked': _maskedSqliteJob(sqliteDatabase, key='a-rotated-masking-key')}, sqliteDatabase, tmp_path)


def test_a_changed_key_does_not_stop_a_swap_job(tmp_path, sqliteDatabase):
    """A swap replaces its whole target, so nothing masked under the old key is left."""
    connection = sqlite3.connect(sqliteDatabase['lite'].database)
    connection.execute('create table target_stage (id integer primary key, name text)')
    connection.close()

    for key in ('an-original-masking-key', 'a-rotated-masking-key'):
        result = _runJobs({'masked': _maskedSqliteJob(sqliteDatabase, key=key, insertStrategy='swap', targetTableStage='target_stage')},
                          sqliteDatabase, tmp_path)
        assert result.succeeded


def test_each_cycle_is_reported_to_on_cycle_and_its_failures_are_contained(tmp_path, sqliteDatabase):
    jobsFile = Configuration.validateJobConfiguration({'workers': 1, 'jobs': {'copies': _sqliteJob(sqliteDatabase)}}, DataJobsFile)
    seen: List[RunResult] = []

    def report(result: RunResult) -> None:
        seen.append(result)
        raise RuntimeError('the webhook host is down')

    result = runDataJobs(jobsFile=jobsFile, databaseConfiguration=sqliteDatabase, memory=FileMemory(tmp_path / 'memory.yaml'), onCycle=report)

    assert result.succeeded
    assert [[outcome.job for outcome in cycle.outcomes] for cycle in seen] == [['copies']]


def test_stopping_a_job_mid_log_record_leaves_the_other_jobs_working(tmp_path, sqliteDatabase):
    """Jobs used to share one log queue, and its lock. Stopping a job while it
    held that lock left every later job unable to log or exit, and the run
    hung. Each job now writes to a pipe of its own.
    """
    jobs = {'noisy': _sqliteJob(sqliteDatabase, timeoutSeconds=1, sourceQueryColumnTransforms={'name': ['tests.crashingTransforms:logWithoutPause']})}
    jobs.update({'after{}'.format(index): _sqliteJob(sqliteDatabase) for index in range(3)})
    jobsFile = Configuration.validateJobConfiguration({'workers': 1, 'jobs': jobs}, DataJobsFile)
    logPath = tmp_path / 'run.log'
    results: List[RunResult] = []

    thread = threading.Thread(target=lambda: results.append(runDataJobs(
        jobsFile=jobsFile, databaseConfiguration=sqliteDatabase, memory=FileMemory(tmp_path / 'memory.yaml'), logFile=logPath)), daemon=True)
    thread.start()
    thread.join(timeout=60)

    assert not thread.is_alive(), 'the run hung after a job was stopped'
    statuses = {outcome.job: outcome.status for outcome in results[0].outcomes}
    assert statuses == {'noisy': JobStatus.FAILED, 'after0': JobStatus.COMPLETED, 'after1': JobStatus.COMPLETED, 'after2': JobStatus.COMPLETED}
    log = logPath.read_text()
    assert all('Completed after{} (2 row(s))'.format(index) in log for index in range(3))


def test_a_noisy_job_does_not_starve_the_others(tmp_path, sqliteDatabase):
    """A job that logs without pause must not keep the others' records and
    outcomes from being read -- here the quiet job finishes long before the
    noisy one is stopped.
    """
    jobs = {'noisy': _sqliteJob(sqliteDatabase, timeoutSeconds=5, sourceQueryColumnTransforms={'name': ['tests.crashingTransforms:logWithoutPause']}),
            'quiet': _sqliteJob(sqliteDatabase)}
    result = _runJobs(jobs, sqliteDatabase, tmp_path, workers=2)

    quiet = next(outcome for outcome in result.outcomes if outcome.job == 'quiet')
    assert quiet.status == JobStatus.COMPLETED
    assert quiet.finishedAt - quiet.startedAt < 4


# --- the chunk pipeline -------------------------------------------------------

def _pipelineFake(rows, failReadAt=None, failWriteAt=None, written=None, reads=None):
    """A Database whose reads or writes can be made to fail at a given chunk."""

    state = {'read': 0, 'write': 0}

    class _Fake:

        def __init__(self, connectionSettings: Any = None) -> None:
            return None

        def __enter__(self) -> '_Fake':
            return self

        def __exit__(self, *args: Any) -> None:
            return None

        def stream(self, query: str, chunkSize: int, parameters: Any = None) -> Tuple[List[str], Any]:

            def chunks() -> Any:
                for index in range(0, len(rows), chunkSize):
                    state['read'] += 1
                    if reads is not None:
                        reads.append(state['read'])
                    if failReadAt is not None and state['read'] == failReadAt:
                        raise RuntimeError('the reader failed')
                    yield rows[index:index + chunkSize]

            return ['id', 'name'], chunks()

        def getAllColumnNames(self, table: str) -> List[str]:
            return ['id', 'name']

        def upsert(self, table: str, data: List[Any], chunkSize: int = 100, columns: Any = None) -> None:
            self.insert(table, data)

        def insert(self, table: str, data: List[Any], chunkSize: int = 100, columns: Any = None) -> None:
            state['write'] += 1
            if failWriteAt is not None and state['write'] == failWriteAt:
                raise RuntimeError('the writer failed')
            if written is not None:
                written.extend(data)

        def truncate(self, table: str) -> None:
            return None

        def alter(self, query: str) -> None:
            return None

    return _Fake


@pytest.mark.parametrize('chunkSize', [1, 3, 100, 997])
def test_the_pipeline_writes_chunks_in_source_order(monkeypatch, chunkSize):
    """Order is the pipeline's one hard requirement. A stage-less upsert writes
    straight into the live target, and one statement can't update the same row
    twice -- so a key that repeats across chunks has to arrive as it was read.
    """

    monkeypatch.setenv('BAUTA_PIPELINE', '1')

    rows = [(index % 17, 'name{}'.format(index)) for index in range(3000)]
    written: List[Any] = []
    monkeypatch.setattr('bauta.runner.Database', _pipelineFake(rows, written=written))

    _executeDataJob('job1', _dataJobConfig(chunkSize=chunkSize), {'src': _dbConfig(), 'tgt': _dbConfig()})

    assert written == rows


def test_a_failure_on_any_side_of_the_pipeline_surfaces(monkeypatch):
    """Whichever side fails, the job fails with that error rather than hanging.

    The masking case is the one that bites: a worker that has raised consumes
    nothing more, so a sender that blocks outright on a full queue waits for a
    thread that will never take from it.
    """

    monkeypatch.setenv('BAUTA_PIPELINE', '1')

    rows = [(index, 'name{}'.format(index)) for index in range(3000)]

    monkeypatch.setattr('bauta.runner.Database', _pipelineFake(rows, failReadAt=5))
    with pytest.raises(RuntimeError, match='the reader failed'):
        _executeDataJob('job1', _dataJobConfig(chunkSize=10), {'src': _dbConfig(), 'tgt': _dbConfig()})

    monkeypatch.setattr('bauta.runner.Database', _pipelineFake(rows, failWriteAt=5))
    with pytest.raises(RuntimeError, match='the writer failed'):
        _executeDataJob('job1', _dataJobConfig(chunkSize=10), {'src': _dbConfig(), 'tgt': _dbConfig()})

    monkeypatch.setattr('bauta.runner.Database', _pipelineFake(rows))
    monkeypatch.setattr('bauta.transform.Transform.apply',
                        lambda self, chunk: (_ for _ in ()).throw(RuntimeError('masking failed')))
    with pytest.raises(RuntimeError, match='masking failed'):
        _executeDataJob('job1', _dataJobConfig(chunkSize=10), {'src': _dbConfig(), 'tgt': _dbConfig()})


def test_the_pipeline_leaves_no_threads_behind(monkeypatch):
    """A worker per job would otherwise accumulate across a runForever cycle."""

    monkeypatch.setenv('BAUTA_PIPELINE', '1')

    rows = [(index, 'name{}'.format(index)) for index in range(500)]
    monkeypatch.setattr('bauta.runner.Database', _pipelineFake(rows))
    before = threading.active_count()

    for _ in range(5):
        _executeDataJob('job1', _dataJobConfig(chunkSize=10), {'src': _dbConfig(), 'tgt': _dbConfig()})

    for _ in range(5):
        # A fresh fake each time: its chunk counter is per-instance, and a
        # reused one would only fail on the first run.
        monkeypatch.setattr('bauta.runner.Database', _pipelineFake(rows, failWriteAt=3))
        with pytest.raises(RuntimeError):
            _executeDataJob('job1', _dataJobConfig(chunkSize=10), {'src': _dbConfig(), 'tgt': _dbConfig()})

    deadline = time.time() + 10
    while threading.active_count() > before and time.time() < deadline:
        time.sleep(0.1)

    assert threading.active_count() == before, [t.name for t in threading.enumerate()]


def test_the_pipeline_follows_the_native_masker_unless_told_otherwise(monkeypatch):
    """Overlapping only pays where masking is fast enough for the database's
    round trips to be the part worth hiding, which is where the extension is.

    Measured on 200,000 rows over a 5 ms round trip: Python masking goes 11.57s
    to 11.79s overlapped, and native masking 2.92s to 2.16s. So the default
    follows the extension, and either setting overrides it.
    """

    import bauta.masking as masking
    from bauta.runner import PIPELINE_DEPTH, _pipelineDepth

    monkeypatch.delenv('BAUTA_PIPELINE', raising=False)

    monkeypatch.setattr(masking, 'nativeVersion', lambda: '0.1.0')
    assert _pipelineDepth() == PIPELINE_DEPTH

    monkeypatch.setattr(masking, 'nativeVersion', lambda: None)
    assert _pipelineDepth() == 0

    # And the setting wins over the default, either way.
    monkeypatch.setenv('BAUTA_PIPELINE', '1')
    assert _pipelineDepth() == PIPELINE_DEPTH

    monkeypatch.setattr(masking, 'nativeVersion', lambda: '0.1.0')
    monkeypatch.setenv('BAUTA_PIPELINE', '0')
    assert _pipelineDepth() == 0
