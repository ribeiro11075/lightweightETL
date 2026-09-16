from __future__ import annotations

import contextlib
import logging
import multiprocessing as mp
import signal
import time
from multiprocessing.pool import Pool
from pathlib import Path
from typing import Any, Dict, Iterator, List, NamedTuple, Optional

from .configuration import ConfigurationError, DatabaseConnectionConfig, DataJobConfig, DataJobsFile, InsertStrategy, ScrambleJobConfig, \
    ScrambleJobsFile
from .databaseDialects import ColumnCategory
from .database import Database
from .scramble import Scramble
from .dependencyGraph import DependencyGraph, JobOutcome, JobStatus
from .log import Log
from .memory import MemoryBackend
from .transform import Transformer, resolveTransformer, Transform


class _TerminationRequested(Exception):
    """Raised inside the runner when SIGINT/SIGTERM arrives, to unwind normally."""


@contextlib.contextmanager
def _terminationHandling(log: Log) -> Iterator[Dict[str, bool]]:
    """Turns SIGINT/SIGTERM into a flag the run loop can act on, and restores the
    previous handlers on the way out.

    Without this, a Ctrl-C or a container's SIGTERM tears down the parent while
    its multiprocessing.Pool workers are mid-job: the pool is never terminated,
    so workers are orphaned and whatever they held open -- a streaming cursor, a
    half-committed chunked load -- is left to the database to clean up. A
    long-running runForever process in Kubernetes gets SIGTERM on every ordinary
    pod shutdown, so this is the common path, not the exceptional one.

    The handler only sets a flag. Doing the teardown inside a signal handler
    would run it on whatever stack frame happened to be executing, including one
    inside the multiprocessing machinery; letting the loop notice at its own next
    checkpoint keeps teardown on a stack that can safely do it.

    Handlers are only installed when this is the main thread of the main
    interpreter -- signal.signal raises anywhere else, and a library has no
    business failing because its caller ran it on a worker thread.
    """

    state = {'terminating': False}

    def handler(signalNumber: int, frame: Any) -> None:
        state['terminating'] = True
        log.logging.warning('Received {}, finishing the current cycle and shutting down'.format(signal.Signals(signalNumber).name))

    installed = []

    try:
        for signalNumber in (signal.SIGINT, signal.SIGTERM):
            installed.append((signalNumber, signal.signal(signalNumber, handler)))
    except ValueError:
        log.logging.debug('Not on the main thread; leaving signal handling to the caller')

    try:
        yield state
    finally:
        for signalNumber, previousHandler in installed:
            signal.signal(signalNumber, previousHandler)


class RunResult(NamedTuple):
    """What one call to runDataJobs/runScrambleJobs did.

    Returned rather than written anywhere. A result is the caller's to act on --
    turn into an exit code, raise on, print, forward to whatever they already
    use for alerting -- and making that a return value means it needs no backend,
    no configuration, and no decision from the user to be useful.

    This is deliberately not MemoryBackend's job. Memory is scheduler *input*:
    read before a job runs, one overwritten row per job, and required for
    correctness. A run's results are output: written after, append-only if kept
    at all, and read by people rather than by the scheduler. Durable run history
    is a separate concern, and an optional one -- nothing breaks without it.

    Only returned when runForever is False. A run that never ends has no results
    to hand back.
    """

    outcomes: List[JobOutcome]

    @property
    def completed(self) -> List[JobOutcome]:

        return [outcome for outcome in self.outcomes if outcome.status == JobStatus.COMPLETED]

    @property
    def failed(self) -> List[JobOutcome]:

        return [outcome for outcome in self.outcomes if outcome.status == JobStatus.FAILED]

    @property
    def skipped(self) -> List[JobOutcome]:

        return [outcome for outcome in self.outcomes if outcome.status == JobStatus.SKIPPED]

    @property
    def rowCount(self) -> int:

        return sum(outcome.rowCount for outcome in self.outcomes)

    @property
    def succeeded(self) -> bool:
        """True only if every active job completed.

        A skipped job counts against this as much as a failed one: it didn't run,
        and the data it was meant to produce isn't there. A cycle with no active
        jobs at all succeeded trivially -- there was nothing to get wrong.
        """

        return not self.failed and not self.skipped


def _executeDataJob(job: str, jobConfig: DataJobConfig, databaseConfiguration: Dict[str, DatabaseConnectionConfig], log: Optional[Log] = None,
                     watermark: Any = None) -> JobOutcome:
    """Runs one data job to completion, raising on failure.

    Extract, transform and load are streamed: rows are pulled from the source a
    chunk at a time and written to the target as they arrive, so peak memory is
    bounded by jobConfig.chunkSize rather than by the size of the result set.
    chunkSize is therefore the memory dial, not just the insert batch size.

    Each sourceQueryColumnTransforms entry is a "module.path:function_name" reference,
    resolved via resolveTransformer rather than a fixed lookup table. One source and one
    target Database connection are opened for the job and reused for every step, rather
    than a fresh connection per query. The swap branch asserts targetTableStage is set
    because DataJobConfig's validator guarantees that whenever insertStrategy is swap.

    Transforms run against sourceQuery's own result columns (reported by
    Database.stream alongside the rows -- whatever that query actually selected,
    explicit list or `select *` alike), *not* the target table: a transform
    operates on a value as extracted from the source, before it's mapped onto any
    target column name. Transform.validate() runs once, before the first write,
    so a transform naming a column the query doesn't return still fails before
    anything lands in the target.

    columns (the target side) is resolved separately -- from targetColumns if the job
    configured it, otherwise by introspecting targetTableFinal -- and used for every
    insert/upsert call's column list, rather than each step re-introspecting the table
    independently. sourceQuery's SELECT list is assumed to match columns positionally;
    if targetColumns is unset, that means matching targetTableFinal's own column order
    exactly.

    preTargetAdhocQueries run before *any* write to the target, including the stage
    load. They previously ran after it, which made the name a lie and broke the one
    thing the hook is for: a pre-query that prepares the stage table (dropping an
    index to speed the load, disabling a constraint, clearing a partition) landed
    after the rows it was supposed to prepare for. This matches _executeScrambleJob,
    where pre-queries have always run first.

    When jobConfig.watermarkColumn is set, `watermark` is bound into
    sourceQuery's {{ watermark }} placeholder and the job extracts only the rows
    beyond it. The returned watermark is the highest value that column reached,
    taken from the *raw* source rows rather than the transformed ones: a
    transform may reformat the column (a number into a currency string, say),
    and what goes back into the next run's predicate has to be something the
    source can still compare against its own column.

    One consequence of streaming worth knowing: extract and load now interleave,
    so a source that fails part-way through leaves the rows it already yielded
    written, where buffering the whole extract first meant a mid-extract failure
    wrote nothing. That is invisible for `swap` and for stage-backed `upsert` --
    both land in the stage table, leaving targetTableFinal untouched until the
    final atomic-ish step -- but a stage-less `upsert` writes partial results
    directly into the live target. Prefer a targetTableStage for anything large.
    """

    columnTransforms: Dict[str, List[Transformer]] = {
        column: [resolveTransformer(reference) for reference in references] for column, references in jobConfig.sourceQueryColumnTransforms.items()
        }

    sourceDatabaseConnectionSettings = databaseConfiguration[jobConfig.sourceDatabase]
    targetDatabaseConnectionSettings = databaseConfiguration[jobConfig.targetDatabase]

    with Database(connectionSettings=sourceDatabaseConnectionSettings) as sourceDatabase, \
         Database(connectionSettings=targetDatabaseConnectionSettings) as targetDatabase:

        sourceQuery = jobConfig.sourceQuery
        parameters = None

        if jobConfig.watermarkColumn:
            sourceQuery = sourceDatabase.substituteWatermarkPlaceholder(sourceQuery)
            parameters = (watermark,)
            if log:
                log.logging.info('Extracting {} incrementally, from watermark {!r}'.format(jobConfig.sourceDatabase, watermark))

        if log:
            log.logging.debug('Streaming sourceQuery against {} in chunks of {}'.format(jobConfig.sourceDatabase, jobConfig.chunkSize))
        sourceQueryColumns, chunks = sourceDatabase.stream(query=sourceQuery, chunkSize=jobConfig.chunkSize, parameters=parameters)
        if log:
            log.logging.debug('sourceQuery returned columns: {}'.format(sourceQueryColumns))

        watermarkIndex = None

        if jobConfig.watermarkColumn:
            if jobConfig.watermarkColumn not in sourceQueryColumns:
                raise ConfigurationError(
                    'watermarkColumn "{}" is not among the columns sourceQuery returns {} -- '
                    'the job cannot tell how far it got'.format(jobConfig.watermarkColumn, sourceQueryColumns))
            watermarkIndex = sourceQueryColumns.index(jobConfig.watermarkColumn)

        transform = Transform(columns=sourceQueryColumns, columnTransforms=columnTransforms)
        transform.validate()
        if log and columnTransforms:
            log.logging.debug('Applying transforms to column(s): {}'.format(', '.join(columnTransforms)))

        columns = jobConfig.targetColumns or targetDatabase.getAllColumnNames(table=jobConfig.targetTableFinal)
        if log:
            log.logging.debug('Resolved target columns for {}: {}'.format(jobConfig.targetTableFinal, columns))

        for preTargetAdhocQuery in jobConfig.preTargetAdhocQueries:
            if log:
                log.logging.debug('Running preTargetAdhocQuery: {}'.format(preTargetAdhocQuery))
            targetDatabase.alter(preTargetAdhocQuery)

        if jobConfig.targetTableStage:
            if log:
                log.logging.debug('Truncating stage table {}'.format(jobConfig.targetTableStage))
            targetDatabase.truncate(table=jobConfig.targetTableStage)

        loadTable = jobConfig.targetTableStage or jobConfig.targetTableFinal
        streamsDirectlyIntoTarget = jobConfig.insertStrategy == InsertStrategy.UPSERT and not jobConfig.targetTableStage

        if log:
            log.logging.info('Loading into {} a chunk at a time'.format(loadTable))

        rowCount = 0
        highWatermark = None

        for chunk in chunks:

            if watermarkIndex is not None:
                for row in chunk:
                    value = row[watermarkIndex]
                    if value is not None and (highWatermark is None or value > highWatermark):
                        highWatermark = value

            rows = transform.apply(chunk)

            if streamsDirectlyIntoTarget:
                targetDatabase.upsert(table=loadTable, data=rows, chunkSize=jobConfig.chunkSize, columns=columns)
            else:
                targetDatabase.insert(table=loadTable, data=rows, chunkSize=jobConfig.chunkSize, columns=columns)

            rowCount += len(rows)
            if log:
                log.logging.debug('Loaded {} row(s) into {} ({} so far)'.format(len(rows), loadTable, rowCount))

        if log:
            log.logging.info('Streamed {} row(s) from {} into {}'.format(rowCount, jobConfig.sourceDatabase, loadTable))

        if jobConfig.insertStrategy == InsertStrategy.SWAP:
            assert jobConfig.targetTableStage is not None
            if log:
                log.logging.info('Swapping {} with stage table {}'.format(jobConfig.targetTableFinal, jobConfig.targetTableStage))
            targetDatabase.swap(targetTable=jobConfig.targetTableFinal, stageTable=jobConfig.targetTableStage)

        if jobConfig.insertStrategy == InsertStrategy.UPSERT and jobConfig.targetTableStage:
            if log:
                log.logging.info('Upserting {} from stage table {}'.format(jobConfig.targetTableFinal, jobConfig.targetTableStage))
            targetDatabase.upsertFromStage(targetTable=jobConfig.targetTableFinal, stageTable=jobConfig.targetTableStage, columns=columns)

        for postTargetAdhocQuery in jobConfig.postTargetAdhocQueries:
            if log:
                log.logging.debug('Running postTargetAdhocQuery: {}'.format(postTargetAdhocQuery))
            targetDatabase.alter(postTargetAdhocQuery)

    return JobOutcome(job=job, status=JobStatus.COMPLETED, rowCount=rowCount, watermark=highWatermark)


def _dataJobWorker(readyQueue: mp.Queue, completedQueue: mp.Queue, activeJobs: Dict[str, DataJobConfig],
                    databaseConfiguration: Dict[str, DatabaseConnectionConfig], logFile: Optional[Path], memory: MemoryBackend,
                    logLevel: int = logging.INFO) -> None:
    """Runs data jobs pulled off readyQueue, via _executeDataJob, until the process is torn down.

    recordRun is deliberately placed where it is, on both axes:

    *Only on success.* It used to run unconditionally, so a job that raised was
    still stamped as having just run -- and a `refresh` window then suppressed it
    for that many minutes. A job failing every time would go quiet for an hour
    rather than retrying on the next cycle, which is the opposite of what a
    refresh window is for. A failed job now records nothing, so it is eligible
    again immediately.

    *Before signalling completion.* completedQueue is what DependencyGraph.run()
    watches to decide the cycle is over, and runDataJobs terminates the pool as
    soon as it returns. Recording after the put opened a window where a worker
    could be killed between the two, losing the run stamp for a job that really
    did complete. Writing to memory first means the stamp is durable by the time
    anything can act on the completion.

    A memory backend that fails to record is logged and otherwise tolerated: the
    data did land, so the job is honestly COMPLETED, and the only consequence of
    the missing stamp is that the job re-runs sooner than its refresh window asks.
    Failing the job outright would be a worse lie than the one it replaces.

    The watermark follows the same rule for the same reason, and its ordering is
    what makes an incremental job crash-safe. Every step commits before the one
    after it, so every point this can die at is recoverable in the same
    direction -- backwards, into re-reading rows that were already loaded:

      load committed -> recordWatermark -> recordRun -> completedQueue

    Dying before recordWatermark leaves the old watermark, so the next run
    re-extracts rows it already loaded. Dying between recordWatermark and
    recordRun advances the watermark (correct -- the data did land) and re-runs
    sooner than the refresh window asked. Neither loses a row, and both are
    harmless precisely because watermarkColumn requires insertStrategy: upsert:
    re-loading a row that is already there is a no-op. A watermark advanced on a
    *failed* job would be the one unrecoverable direction, skipping rows nothing
    will come back for -- which is why this sits inside the success branch.
    """

    log = Log(logFile=logFile, level=logLevel)

    while True:

        job = readyQueue.get()
        jobConfig = activeJobs[job]
        log.logging.info('Starting {}'.format(job))
        startedAt = time.time()
        watermark = None

        try:
            if jobConfig.watermarkColumn:
                watermark = memory.readWatermarks().get(job, jobConfig.watermarkInitial)

            outcome = _executeDataJob(job, jobConfig, databaseConfiguration, log=log, watermark=watermark)

        except Exception as error:
            outcome = JobOutcome(job=job, status=JobStatus.FAILED, error='{}: {}'.format(type(error).__name__, error))
            log.logging.error('Failed to complete {} due to error {}'.format(job, error), exc_info=error)

        if outcome.status == JobStatus.COMPLETED:

            if jobConfig.watermarkColumn and outcome.watermark is not None:
                try:
                    memory.recordWatermark(job=job, value=outcome.watermark)
                    log.logging.info('Advanced {} watermark to {!r}'.format(job, outcome.watermark))
                except Exception as error:
                    log.logging.error(
                        'Completed {} but could not record its watermark -- the next run will re-extract from {!r}'.format(job, watermark), exc_info=error)

            try:
                memory.recordRun(job=job)
            except Exception as error:
                log.logging.error(
                    'Completed {} but could not record its run -- it will re-run before its refresh window is up'.format(job), exc_info=error)

            log.logging.info('Completed {} ({} row(s))'.format(job, outcome.rowCount))

        completedQueue.put(outcome._replace(startedAt=startedAt, finishedAt=time.time()))


def _logCycleSummary(log: Log, dependencyGraph: DependencyGraph) -> None:
    """One line per terminal non-success, plus totals.

    A skipped job previously left no trace at all -- it never reached a worker,
    so nothing logged it, and an operator looking for why a table was stale
    found silence. It's named here, along with what it was waiting on.
    """

    result = RunResult(outcomes=list(dependencyGraph.outcomes))

    for outcome in result.failed:
        log.logging.error('{} failed after {:.1f}s: {}'.format(outcome.job, outcome.durationSeconds, outcome.error))

    for outcome in result.skipped:
        log.logging.warning('{} skipped: {}'.format(outcome.job, outcome.error))

    log.logging.info('Cycle finished: {} completed, {} failed, {} skipped, {} row(s) moved'.format(
        len(result.completed), len(result.failed), len(result.skipped), result.rowCount))


def _requireWatermarkCapableMemory(jobsFile: DataJobsFile, memory: MemoryBackend) -> None:
    """Fails before any work starts if an incremental job has nowhere to persist its watermark.

    MemoryBackend.recordWatermark is deliberately not abstract, so backends
    written before watermarks existed keep working for jobs that don't use one.
    The cost of that choice is that the mismatch would otherwise surface as a
    NotImplementedError inside a worker process, after a job had already
    extracted and loaded its rows -- and then on every cycle after that. Checking
    the class up front turns it into a configuration error, where it belongs.
    """

    incrementalJobs = sorted(name for name, job in jobsFile.jobs.items() if job.active and job.watermarkColumn)

    if incrementalJobs and type(memory).recordWatermark is MemoryBackend.recordWatermark:
        raise ConfigurationError(
            '{} does not implement recordWatermark, but these active job(s) configure a watermarkColumn: {}. '
            'Implement readWatermarks/recordWatermark on it, or use FileMemory.'.format(type(memory).__name__, ', '.join(incrementalJobs)))


def runDataJobs(jobsFile: DataJobsFile, databaseConfiguration: Dict[str, DatabaseConnectionConfig], memory: MemoryBackend,
                 logFile: Optional[Path] = None, runForever: bool = False, logLevel: int = logging.INFO) -> RunResult:
    """Runs data jobs, honoring each job's `refresh` window and `predecessors`.

    The caller's only responsibility is configuration: the validated jobs/database
    config, where logs should live, a MemoryBackend for run-history (FileMemory by
    default -- see memory.py; write your own to persist it elsewhere, e.g.
    a database), and whether this is a one-time pass (runForever=False, the
    default) or should keep running (runForever=True).

    Single-shot is the default because it composes with whatever already schedules
    work in your deployment -- cron, a systemd timer, a Kubernetes CronJob, an
    Airflow task -- rather than competing with it. Those give you alerting,
    retries, backfill and calendar-aware schedules that `refresh` cannot express;
    `refresh` is a throttle, not a schedule. It still applies across separate
    invocations, because it is evaluated against MemoryBackend.read(), which is
    durable: running every five minutes from cron with `refresh: 60` correctly
    skips eleven runs out of twelve. And a run that returns hands back a RunResult,
    which runForever=True never can.

    Use runForever=True when you need freshness below cron's one-minute floor, or
    where there is no scheduler to hook into at all. Worker processes and their pool are managed entirely
    here: each cycle's pool is terminated and joined before the next one starts,
    rather than left running, so workers don't pile up as OS processes across
    cycles. The same `memory` instance is handed to every worker process (pickled
    and reconstructed per process, per MemoryBackend's contract).

    jobsFile.cycleSleepSeconds controls how long this sleeps between cycles once
    every active job in one has completed or failed (only reached when
    runForever=True, since runForever=False breaks out of the loop beforehand) --
    unrelated to DependencyGraph.run()'s own fixed 1-second poll, which is an
    inner loop that waits for jobs *within* a single cycle to finish, not the gap
    between cycles.

    logLevel defaults to logging.INFO (job start/completion, row counts, major
    steps); pass logging.DEBUG for the finer-grained detail this and _executeDataJob
    also emit (resolved columns, adhoc query text, per-step SQL) -- passed through
    to every worker process's own Log, not just this function's.
    """

    log = Log(logFile=logFile, level=logLevel)
    log.logging.info('Starting data job runner with {} worker(s)'.format(jobsFile.workers))

    _requireWatermarkCapableMemory(jobsFile, memory)

    pool: Optional[Pool] = None

    with _terminationHandling(log) as termination:

        while True:

            dependencyGraph = DependencyGraph(jobs=jobsFile.jobs, memory=memory.read())
            log.logging.info('Starting cycle with {} active job(s)'.format(len(dependencyGraph.activeJobs)))

            if pool is not None:
                pool.terminate()
                pool.join()

            pool = mp.Pool(jobsFile.workers, _dataJobWorker,
                            (dependencyGraph.readyQueue, dependencyGraph.completedQueue, dependencyGraph.activeJobs,
                             databaseConfiguration, logFile, memory, logLevel))
            dependencyGraph.run()

            pool.close()
            _logCycleSummary(log, dependencyGraph)

            if not runForever or termination['terminating']:
                pool.terminate()
                pool.join()
                break

            time.sleep(jobsFile.cycleSleepSeconds)

        log.logging.info('Finished data job runner')

    return RunResult(outcomes=list(dependencyGraph.outcomes))


def _executeScrambleJob(job: str, jobConfig: ScrambleJobConfig, databaseConfiguration: Dict[str, DatabaseConnectionConfig], log: Optional[Log] = None) -> JobOutcome:
    """Runs one scramble job to completion, raising on failure.

    One Database connection is opened for the job and reused for every step. If
    the table has no rows, nothing is scrambled or written -- only the pre-adhoc
    queries (if any) run.
    """

    connectionSettings = databaseConfiguration[jobConfig.database]

    with Database(connectionSettings=connectionSettings) as database:

        for preTargetAdhocQuery in jobConfig.preTargetAdhocQueries:
            if log:
                log.logging.debug('Running preTargetAdhocQuery: {}'.format(preTargetAdhocQuery))
            database.alter(preTargetAdhocQuery)

        dataQuery = 'select * from {}'.format(jobConfig.table)
        if log:
            log.logging.debug('Reading {} from {}'.format(jobConfig.table, jobConfig.database))
        data = database.query(query=dataQuery)
        columns = database.getAllColumnNames(table=jobConfig.table)
        dataTypes = database.getAllColumnTypes(table=jobConfig.table)
        columnCategories: Dict[str, ColumnCategory] = {}
        for column, dataType in zip(columns, dataTypes):
            category = database.dialect.columnCategory(dataType)
            if category is not None:
                columnCategories[column] = category
        if log:
            log.logging.info('Read {} row(s), {} column(s) from {}'.format(len(data), len(columns), jobConfig.table))
            log.logging.debug('Categorized column(s) for random generation: {}'.format(columnCategories))

        if data:

            scramble = Scramble(job=job, data=data, columns=columns, columnCategories=columnCategories, defaultColumnValues=jobConfig.defaultColumnValues,
                                 identifierColumns=jobConfig.identifierColumns, scrambleColumns=jobConfig.scrambleColumns, randomColumns=jobConfig.randomColumns,
                                 allDataRandom=jobConfig.allDataRandom, randomSalt=jobConfig.randomSalt)
            scramble.scramble()
            if log:
                log.logging.debug('Scrambled {} row(s) across column(s): {}'.format(len(scramble.dataScrambled), ', '.join(columns)))

            database.truncate(jobConfig.table)
            if log:
                log.logging.info('Reinserting {} scrambled row(s) into {}'.format(len(scramble.dataScrambled), jobConfig.table))
            database.insert(table=jobConfig.table, data=scramble.dataScrambled, chunkSize=5000)

            for postTargetAdhocQuery in jobConfig.postTargetAdhocQueries:
                if log:
                    log.logging.debug('Running postTargetAdhocQuery: {}'.format(postTargetAdhocQuery))
                database.alter(postTargetAdhocQuery)

            return JobOutcome(job=job, status=JobStatus.COMPLETED, rowCount=len(scramble.dataScrambled))

        if log:
            log.logging.info('{} has no rows -- nothing to scramble'.format(jobConfig.table))

    return JobOutcome(job=job, status=JobStatus.COMPLETED)


def _scrambleJobWorker(readyQueue: mp.Queue, completedQueue: mp.Queue, activeJobs: Dict[str, ScrambleJobConfig],
                        databaseConfiguration: Dict[str, DatabaseConnectionConfig], logFile: Optional[Path], logLevel: int = logging.INFO) -> None:
    """Runs scramble jobs pulled off readyQueue, via _executeScrambleJob, until the process is torn down."""

    log = Log(logFile=logFile, level=logLevel)

    while True:

        job = readyQueue.get()
        log.logging.info('Starting {}'.format(job))
        startedAt = time.time()

        try:
            outcome = _executeScrambleJob(job, activeJobs[job], databaseConfiguration, log=log)
            log.logging.info('Completed {} ({} row(s))'.format(job, outcome.rowCount))

        except Exception as error:
            outcome = JobOutcome(job=job, status=JobStatus.FAILED, error='{}: {}'.format(type(error).__name__, error))
            log.logging.error('Failed to complete {} due to error {}'.format(job, error), exc_info=error)

        completedQueue.put(outcome._replace(startedAt=startedAt, finishedAt=time.time()))


def runScrambleJobs(jobsFile: ScrambleJobsFile, databaseConfiguration: Dict[str, DatabaseConnectionConfig], logFile: Optional[Path] = None,
                     runForever: bool = False, logLevel: int = logging.INFO) -> RunResult:
    """Runs scramble jobs, honoring `predecessors`.

    Same division of responsibility as runDataJobs: the caller supplies validated
    configuration, a log location, and whether this is a one-time pass
    (runForever=False, the default -- a masking run is usually one-shot) or should
    keep running (runForever=True). Workers and their pool are managed here.

    jobsFile.cycleSleepSeconds controls how long this sleeps between cycles --
    see runDataJobs's docstring for why that's independent of
    DependencyGraph.run()'s own fixed 1-second poll. logLevel: see runDataJobs.
    """

    log = Log(logFile=logFile, level=logLevel)
    log.logging.info('Starting scramble job runner with {} worker(s)'.format(jobsFile.workers))

    pool: Optional[Pool] = None

    with _terminationHandling(log) as termination:

        while True:

            dependencyGraph = DependencyGraph(jobs=jobsFile.jobs)
            log.logging.info('Starting cycle with {} active job(s)'.format(len(dependencyGraph.activeJobs)))

            if pool is not None:
                pool.terminate()
                pool.join()

            pool = mp.Pool(jobsFile.workers, _scrambleJobWorker,
                            (dependencyGraph.readyQueue, dependencyGraph.completedQueue, dependencyGraph.activeJobs, databaseConfiguration, logFile, logLevel))
            dependencyGraph.run()

            pool.close()
            _logCycleSummary(log, dependencyGraph)

            if not runForever or termination['terminating']:
                pool.terminate()
                pool.join()
                break

            time.sleep(jobsFile.cycleSleepSeconds)

        log.logging.info('Finished scramble job runner')

    return RunResult(outcomes=list(dependencyGraph.outcomes))
