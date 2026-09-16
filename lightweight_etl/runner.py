from __future__ import annotations

import contextlib
import logging
import multiprocessing as mp
import signal
import time
import warnings
from multiprocessing.pool import Pool
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, NamedTuple, Optional, Tuple

from .configuration import ConfigurationError, DatabaseConnectionConfig, DataJobConfig, DataJobsFile, InsertStrategy, ScrambleJobConfig, \
    ScrambleJobsFile
from .databaseDialects import ColumnCategory
from .database import Database
from .scramble import Scramble
from .dependencyGraph import DependencyGraph, JobOutcome, JobStatus
from .log import Log
from .masking import BoundMasking, MaskingError, MaskingPlan, buildMaskingManifest, keyFingerprint
from .memory import MemoryBackend
from .transform import TransformError, Transformer, TransformResolutionError, resolveTransformer, Transform


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


    def maskingManifest(self, jobs: Mapping[str, DataJobConfig]) -> Dict[str, Any]:
        """The masking manifest for this run: see masking.buildMaskingManifest.

        Takes the job configurations because a skipped job never produced
        anything to describe itself with, yet still belongs in the record.
        """

        return buildMaskingManifest(self.outcomes, _declaredMasking(jobs))


def _declaredMasking(jobs: Mapping[str, DataJobConfig]) -> Dict[str, Dict[str, Any]]:
    """What each masked job's configuration says, for the manifest."""

    return {
        name: {
            'sourceDatabase': job.sourceDatabase,
            'targetDatabase': job.targetDatabase,
            'targetTable': job.targetTableFinal,
            'keyFingerprint': keyFingerprint(job.masking.key.get_secret_value()),
            }
        for name, job in jobs.items() if job.masking is not None
        }


def _bindMasking(job: str, jobConfig: DataJobConfig, columns: List[str], log: Optional[Log]) -> Optional[BoundMasking]:
    """Binds the job's masking policy to the columns its query returned.

    Raises MaskingError -- before anything is written -- if the policy doesn't
    cover every column. See MaskingPlan for why that is the default.
    """

    if jobConfig.masking is None:
        return None

    plan = MaskingPlan(key=jobConfig.masking.key.get_secret_value(), columns=jobConfig.masking.columns,
                        defaultStrategy=jobConfig.masking.defaultStrategy)
    bound = plan.bind(columns)

    if log:
        log.logging.info('Masking {} column(s) under key {}'.format(len(columns), plan.fingerprint),
                          extra={'job': job, 'keyFingerprint': plan.fingerprint})
        for entry in bound.manifest:
            log.logging.debug('Masking {} with {}{}'.format(
                entry.column, entry.strategy, ' in domain {}'.format(entry.domain) if entry.domain else ''))

    return bound


def _executeDataJob(job: str, jobConfig: DataJobConfig, databaseConfiguration: Dict[str, DatabaseConnectionConfig], log: Optional[Log] = None,
                     watermark: Any = None) -> JobOutcome:
    """Runs one data job to completion, raising on failure.

    Rows are pulled, transformed and written a chunk at a time, so peak memory is
    bounded by chunkSize rather than by the result set. Extract and load
    interleave, so a source failing part-way leaves the rows already yielded
    written. That is invisible for `swap` and stage-backed `upsert`, which only
    touch targetTableFinal in their last step; a stage-less `upsert` writes
    partial results into the live target, so prefer a stage table for large loads.

    Transforms apply to sourceQuery's own result columns, not the target's, since
    they act on a value as extracted. Transform.validate() runs before the first
    write, so naming a column the query doesn't return fails with nothing loaded.

    Masking runs after transforms, so a value is normalized (stripped, lower
    cased) before it is keyed and masks consistently. Its policy is bound to the
    returned columns before the first write too, so a column the policy doesn't
    cover fails the job with nothing loaded -- unmasked rows never reach the
    target, not even a stage table.

    Target columns come from targetColumns if set, otherwise from introspecting
    targetTableFinal, and are matched to the SELECT list by position.

    preTargetAdhocQueries run before any write, the stage load included, so they
    can prepare the stage table -- drop an index, clear a partition.

    With watermarkColumn set, `watermark` is bound into the {{ watermark }}
    placeholder, and the outcome carries that column's highest value. It is read
    from the raw rows, not the transformed ones: a transform may reformat the
    column, and the next run's predicate needs a value the source can compare.
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

        masking = _bindMasking(job, jobConfig, sourceQueryColumns, log)

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

            if masking is not None:
                rows = masking.apply(rows)

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

    maskingApplied = None
    if masking is not None:
        maskingApplied = {'columns': [entry._asdict() for entry in masking.manifest]}

    return JobOutcome(job=job, status=JobStatus.COMPLETED, rowCount=rowCount, watermark=highWatermark, masking=maskingApplied)


# Errors that a second attempt cannot fix. All are raised by this package itself
# and are deterministic: a transformer reference that doesn't resolve, a column
# the source query never returns, a watermark column that isn't selected, a
# masking policy that doesn't cover a column.
# Retrying them just delays a failure by retries * retryDelaySeconds and buries
# the real message under identical repeats. Everything else -- notably anything
# a driver raises -- is retried, because transient and permanent database errors
# cannot be told apart reliably across six drivers, and a needless retry costs
# far less than a nightly load lost to one dropped connection.
PERMANENT_ERRORS = (ConfigurationError, TransformError, TransformResolutionError, MaskingError)


def _executeWithRetries(jobConfig: DataJobConfig, log: Log, job: str, attempt: Any) -> JobOutcome:
    """Runs `attempt` up to 1 + jobConfig.retries times, backing off exponentially.

    Retrying a whole data job is safe because both insert strategies converge on
    a re-run: `swap` restages and re-swaps, and `upsert` re-applies rows that are
    already there as a no-op. Scramble jobs deliberately have no retries -- a
    failure there can leave the table truncated, and a second pass would find it
    empty and report success having masked nothing.
    """

    lastError: Optional[BaseException] = None

    for attemptNumber in range(1, jobConfig.retries + 2):

        try:
            outcome = attempt()
            return outcome._replace(attempts=attemptNumber)

        except PERMANENT_ERRORS as error:
            raise

        except Exception as error:
            lastError = error
            remaining = jobConfig.retries + 1 - attemptNumber

            if not remaining:
                break

            delay = jobConfig.retryDelaySeconds * (2 ** (attemptNumber - 1))
            log.logging.warning(
                'Attempt {} of {} for {} failed ({}: {}); retrying in {:.1f}s'.format(
                    attemptNumber, jobConfig.retries + 1, job, type(error).__name__, error, delay),
                extra={'job': job, 'attempt': attemptNumber, 'retryDelaySeconds': delay})
            time.sleep(delay)

    assert lastError is not None
    raise lastError


def _dataJobWorker(readyQueue: mp.Queue, completedQueue: mp.Queue, activeJobs: Dict[str, DataJobConfig],
                    databaseConfiguration: Dict[str, DatabaseConnectionConfig], logFile: Optional[Path], memory: MemoryBackend,
                    logLevel: int = logging.INFO, logFormat: str = 'text') -> None:
    """Runs data jobs pulled off readyQueue until the process is torn down.

    On success each step commits before the next:

        load committed -> recordWatermark -> recordRun -> completedQueue

    so every point this can die at falls backwards, into re-reading rows already
    loaded -- harmless, because a watermark requires upsert. Recording precedes
    signalling because the pool is terminated the moment the cycle sees every job
    finish.

    Nothing is recorded for a failed job. A stamped failure would suppress its
    retry for the whole refresh window, and an advanced watermark would skip rows
    permanently, which is the one unrecoverable direction.

    A memory backend that fails to record is logged and tolerated: the data
    landed, so the job is honestly COMPLETED, and the cost is an earlier re-run.
    """

    log = Log(logFile=logFile, level=logLevel, logFormat=logFormat)

    while True:

        job = readyQueue.get()
        jobConfig = activeJobs[job]
        log.logging.info('Starting {}'.format(job))
        startedAt = time.time()
        watermark = None

        try:
            if jobConfig.watermarkColumn:
                watermark = memory.readWatermarks().get(job, jobConfig.watermarkInitial)

            outcome = _executeWithRetries(
                jobConfig, log, job,
                lambda: _executeDataJob(job, jobConfig, databaseConfiguration, log=log, watermark=watermark))

        except Exception as error:
            outcome = JobOutcome(job=job, status=JobStatus.FAILED, error='{}: {}'.format(type(error).__name__, error),
                                  attempts=1 if isinstance(error, PERMANENT_ERRORS) else jobConfig.retries + 1)
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

            log.logging.info('Completed {} ({} row(s))'.format(job, outcome.rowCount),
                              extra={'job': job, 'status': outcome.status.value, 'rowCount': outcome.rowCount, 'attempts': outcome.attempts})

        completedQueue.put(outcome._replace(startedAt=startedAt, finishedAt=time.time()))


def _logCycleSummary(log: Log, dependencyGraph: DependencyGraph) -> None:
    """One line per terminal non-success, plus totals.

    A skipped job never reaches a worker, so this is the only place it gets
    reported -- named along with what it was waiting on, since a stale table
    with no log line is the hardest failure to diagnose.
    """

    result = RunResult(outcomes=list(dependencyGraph.outcomes))

    for outcome in result.failed:
        log.logging.error('{} failed after {:.1f}s: {}'.format(outcome.job, outcome.durationSeconds, outcome.error),
                           extra={'job': outcome.job, 'status': outcome.status.value, 'error': outcome.error,
                                  'attempts': outcome.attempts, 'durationSeconds': round(outcome.durationSeconds, 3)})

    for outcome in result.skipped:
        log.logging.warning('{} skipped: {}'.format(outcome.job, outcome.error),
                             extra={'job': outcome.job, 'status': outcome.status.value, 'error': outcome.error})

    log.logging.info('Cycle finished: {} completed, {} failed, {} skipped, {} row(s) moved'.format(
        len(result.completed), len(result.failed), len(result.skipped), result.rowCount),
        extra={'event': 'cycleFinished', 'completed': len(result.completed), 'failed': len(result.failed),
               'skipped': len(result.skipped), 'rowCount': result.rowCount})


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


def _runJobs(jobsFile: Any, label: str, workerFunction: Any, workerArguments: Tuple[Any, ...], logFile: Optional[Path],
              runForever: bool, logLevel: int, logFormat: str, memory: Optional[MemoryBackend] = None) -> RunResult:
    """The cycle loop both public runners share.

    Data jobs and scramble jobs differ in exactly five things -- the config type,
    one word in a log line, whether a MemoryBackend gates them, the worker
    function, and its arguments -- and in nothing about how a cycle is actually
    driven. Keeping two copies of that loop meant every change to it had to be
    made twice: the SIGINT/SIGTERM handling, threading logFormat through, and
    the cycle summary were each applied once and nearly missed in the other.

    Each cycle's pool is terminated and joined before the next one starts, so
    workers don't accumulate as OS processes. jobsFile.cycleSleepSeconds is the
    gap between cycles (only reached when runForever=True) -- unrelated to
    DependencyGraph.run()'s own fixed 1-second poll, which waits for jobs
    *within* a cycle.
    """

    log = Log(logFile=logFile, level=logLevel, logFormat=logFormat)
    log.logging.info('Starting {} job runner with {} worker(s)'.format(label, jobsFile.workers))

    pool: Optional[Pool] = None

    with _terminationHandling(log) as termination:

        while True:

            dependencyGraph = DependencyGraph(jobs=jobsFile.jobs, memory=memory.read() if memory else None)
            log.logging.info('Starting cycle with {} active job(s)'.format(len(dependencyGraph.activeJobs)))

            if pool is not None:
                pool.terminate()
                pool.join()

            pool = mp.Pool(jobsFile.workers, workerFunction,
                            (dependencyGraph.readyQueue, dependencyGraph.completedQueue, dependencyGraph.activeJobs) + workerArguments)
            dependencyGraph.run()

            pool.close()
            _logCycleSummary(log, dependencyGraph)

            if not runForever or termination['terminating']:
                pool.terminate()
                pool.join()
                break

            time.sleep(jobsFile.cycleSleepSeconds)

        log.logging.info('Finished {} job runner'.format(label))

    return RunResult(outcomes=list(dependencyGraph.outcomes))


def runDataJobs(jobsFile: DataJobsFile, databaseConfiguration: Dict[str, DatabaseConnectionConfig], memory: MemoryBackend,
                 logFile: Optional[Path] = None, runForever: bool = False, logLevel: int = logging.INFO,
                 logFormat: str = 'text') -> RunResult:
    """Runs data jobs, honoring each job's `refresh` window and `predecessors`.

    The caller supplies validated configuration, a MemoryBackend for run state,
    optionally somewhere to log, and whether this is a single pass
    (runForever=False, the default) or stays resident (runForever=True).

    Single-shot is the default because it composes with whatever already
    schedules work in your deployment -- cron, a systemd timer, a Kubernetes
    CronJob, an Airflow task -- rather than competing with it. Those offer
    alerting, backfill and calendar-aware schedules that `refresh` cannot
    express; `refresh` is a throttle, not a schedule. It still applies across
    separate invocations, since it is evaluated against MemoryBackend.read(),
    which is durable: running every five minutes with `refresh: 60` correctly
    skips eleven runs out of twelve. And a run that returns hands back a
    RunResult, which runForever=True never can.

    Use runForever=True for freshness below cron's one-minute floor, or where
    there is no scheduler to hook into.

    The same `memory` instance is handed to every worker process, pickled and
    reconstructed per process, per MemoryBackend's contract. logLevel and
    logFormat are passed through to each worker's own Log, not just this one's.
    """

    _requireWatermarkCapableMemory(jobsFile, memory)

    return _runJobs(jobsFile, 'data', _dataJobWorker,
                     (databaseConfiguration, logFile, memory, logLevel, logFormat),
                     logFile=logFile, runForever=runForever, logLevel=logLevel, logFormat=logFormat, memory=memory)


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
                        databaseConfiguration: Dict[str, DatabaseConnectionConfig], logFile: Optional[Path],
                        logLevel: int = logging.INFO, logFormat: str = 'text') -> None:
    """Runs scramble jobs pulled off readyQueue, via _executeScrambleJob, until the process is torn down."""

    log = Log(logFile=logFile, level=logLevel, logFormat=logFormat)

    while True:

        job = readyQueue.get()
        log.logging.info('Starting {}'.format(job))
        startedAt = time.time()

        try:
            outcome = _executeScrambleJob(job, activeJobs[job], databaseConfiguration, log=log)
            log.logging.info('Completed {} ({} row(s))'.format(job, outcome.rowCount),
                              extra={'job': job, 'status': outcome.status.value, 'rowCount': outcome.rowCount, 'attempts': outcome.attempts})

        except Exception as error:
            outcome = JobOutcome(job=job, status=JobStatus.FAILED, error='{}: {}'.format(type(error).__name__, error))
            log.logging.error('Failed to complete {} due to error {}'.format(job, error), exc_info=error)

        completedQueue.put(outcome._replace(startedAt=startedAt, finishedAt=time.time()))


SCRAMBLE_DEPRECATION = ('scramble jobs are deprecated and will be removed in the next release: '
                        'use a data job with a `masking` section instead (see docs/masking.md)')


def runScrambleJobs(jobsFile: ScrambleJobsFile, databaseConfiguration: Dict[str, DatabaseConnectionConfig], logFile: Optional[Path] = None,
                     runForever: bool = False, logLevel: int = logging.INFO, logFormat: str = 'text') -> RunResult:
    """Runs scramble jobs, honoring `predecessors`. Deprecated.

    Scrambling rewrites a table in place from a copy held in memory, and has
    none of what masking needs: consistency across tables, reproducibility,
    streaming or a transactional swap. A data job with a `masking` section does
    all of that -- see docs/masking.md for the migration. This keeps working
    for one release, with a warning.

    Same division of responsibility as runDataJobs, minus `memory`: scramble
    jobs have no `refresh` window or watermark to track, so there is no run
    state to persist between passes.
    """

    warnings.warn(SCRAMBLE_DEPRECATION, DeprecationWarning, stacklevel=2)
    Log(logFile=logFile, level=logLevel, logFormat=logFormat).logging.warning(SCRAMBLE_DEPRECATION)

    return _runJobs(jobsFile, 'scramble', _scrambleJobWorker,
                     (databaseConfiguration, logFile, logLevel, logFormat),
                     logFile=logFile, runForever=runForever, logLevel=logLevel, logFormat=logFormat)
