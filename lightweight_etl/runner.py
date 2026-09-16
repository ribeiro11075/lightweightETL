from __future__ import annotations

import contextlib
import logging
import signal
import time
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Mapping, NamedTuple, Optional, Tuple

from .configuration import ConfigurationError, DatabaseConnectionConfig, DataJobConfig, DataJobsFile, InsertStrategy
from .database import Database
from .dependencyGraph import DependencyGraph, JobOutcome, JobStatus
from .log import LOGGER_NAME, Log, forwardToQueue, receiveForwardedRecords
from .masking import BoundMasking, MaskingError, MaskingPlan, buildMaskingManifest, keyFingerprint
from .memory import MemoryBackend
from .transform import TransformError, Transformer, TransformResolutionError, resolveTransformer, Transform

logger = logging.getLogger(LOGGER_NAME)

# How often the run loop wakes while jobs are running, to notice SIGINT/SIGTERM.
# Job completions wake it immediately; this only bounds how late a signal is seen.
SIGNAL_POLL_SECONDS = 1.0


@contextlib.contextmanager
def _terminationHandling() -> Iterator[Dict[str, bool]]:
    """Turns SIGINT/SIGTERM into a flag the run loop can act on, and restores the
    previous handlers on the way out.

    On the flag, the loop starts nothing new, lets running jobs finish, and
    reports the rest as skipped. Tearing workers down mid-job instead would
    leave whatever they held open -- a streaming cursor, a half-committed
    chunked load -- for the database to clean up. A long-running runForever
    process in Kubernetes gets SIGTERM on every ordinary pod shutdown, so this
    is the common path, not the exceptional one.

    The handler only sets a flag. Doing the teardown inside a signal handler
    would run it on whatever stack frame happened to be executing, including one
    inside the multiprocessing machinery.

    Handlers are only installed when this is the main thread of the main
    interpreter -- signal.signal raises anywhere else, and a library has no
    business failing because its caller ran it on a worker thread.
    """

    state = {'terminating': False}

    def handler(signalNumber: int, frame: Any) -> None:
        state['terminating'] = True
        logger.warning('Received {}, letting running jobs finish and shutting down'.format(signal.Signals(signalNumber).name))

    installed = []

    try:
        for signalNumber in (signal.SIGINT, signal.SIGTERM):
            installed.append((signalNumber, signal.signal(signalNumber, handler)))
    except ValueError:
        logger.debug('Not on the main thread; leaving signal handling to the caller')

    try:
        yield state
    finally:
        for signalNumber, previousHandler in installed:
            signal.signal(signalNumber, previousHandler)


class RunResult(NamedTuple):
    """What one call to runDataJobs did.

    Returned rather than written anywhere. A result is the caller's to act on --
    turn into an exit code, raise on, print, forward to whatever they already
    use for alerting -- and making that a return value means it needs no backend,
    no configuration, and no decision from the user to be useful.

    This is deliberately not MemoryBackend's job. Memory is scheduler *input*:
    read before a job runs, one overwritten row per job, and required for
    correctness. A run's results are output: written after, append-only if kept
    at all, and read by people rather than by the scheduler.

    With runForever, this describes the last cycle, and is only returned once
    a signal has stopped the loop. `interrupted` says a signal ended the run.
    """

    outcomes: List[JobOutcome]
    interrupted: bool = False

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


def _bindMasking(job: str, jobConfig: DataJobConfig, columns: List[str]) -> Optional[BoundMasking]:
    """Binds the job's masking policy to the columns its query returned.

    Raises MaskingError -- before anything is written -- if the policy doesn't
    cover every column. See MaskingPlan for why that is the default.
    """

    if jobConfig.masking is None:
        return None

    plan = MaskingPlan(key=jobConfig.masking.key.get_secret_value(), columns=jobConfig.masking.columns,
                       defaultStrategy=jobConfig.masking.defaultStrategy)
    bound = plan.bind(columns)

    logger.info('Masking {} column(s) under key {}'.format(len(columns), plan.fingerprint), extra={'job': job, 'keyFingerprint': plan.fingerprint})
    for entry in bound.manifest:
        logger.debug('Masking {} with {}{}'.format(entry.column, entry.strategy, ' in domain {}'.format(entry.domain) if entry.domain else ''))

    return bound


def _executeDataJob(job: str, jobConfig: DataJobConfig, databaseConfiguration: Dict[str, DatabaseConnectionConfig], watermark: Any = None) -> JobOutcome:
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

    with Database(connectionSettings=databaseConfiguration[jobConfig.sourceDatabase]) as sourceDatabase, \
         Database(connectionSettings=databaseConfiguration[jobConfig.targetDatabase]) as targetDatabase:

        sourceQuery = jobConfig.sourceQuery
        parameters = None

        if jobConfig.watermarkColumn:
            sourceQuery = sourceDatabase.substituteWatermarkPlaceholder(sourceQuery)
            parameters = (watermark,)
            logger.info('Extracting {} incrementally, from watermark {!r}'.format(jobConfig.sourceDatabase, watermark))

        logger.debug('Streaming sourceQuery against {} in chunks of {}'.format(jobConfig.sourceDatabase, jobConfig.chunkSize))
        sourceQueryColumns, chunks = sourceDatabase.stream(query=sourceQuery, chunkSize=jobConfig.chunkSize, parameters=parameters)
        logger.debug('sourceQuery returned columns: {}'.format(sourceQueryColumns))

        watermarkIndex = None

        if jobConfig.watermarkColumn:
            if jobConfig.watermarkColumn not in sourceQueryColumns:
                raise ConfigurationError(
                    'watermarkColumn "{}" is not among the columns sourceQuery returns {} -- '
                    'the job cannot tell how far it got'.format(jobConfig.watermarkColumn, sourceQueryColumns))
            watermarkIndex = sourceQueryColumns.index(jobConfig.watermarkColumn)

        transform = Transform(columns=sourceQueryColumns, columnTransforms=columnTransforms)
        transform.validate()
        if columnTransforms:
            logger.debug('Applying transforms to column(s): {}'.format(', '.join(columnTransforms)))

        masking = _bindMasking(job, jobConfig, sourceQueryColumns)

        columns = jobConfig.targetColumns or targetDatabase.getAllColumnNames(table=jobConfig.targetTableFinal)
        logger.debug('Resolved target columns for {}: {}'.format(jobConfig.targetTableFinal, columns))

        for preTargetAdhocQuery in jobConfig.preTargetAdhocQueries:
            logger.debug('Running preTargetAdhocQuery: {}'.format(preTargetAdhocQuery))
            targetDatabase.alter(preTargetAdhocQuery)

        if jobConfig.targetTableStage:
            logger.debug('Truncating stage table {}'.format(jobConfig.targetTableStage))
            targetDatabase.truncate(table=jobConfig.targetTableStage)

        loadTable = jobConfig.targetTableStage or jobConfig.targetTableFinal
        streamsDirectlyIntoTarget = jobConfig.insertStrategy == InsertStrategy.UPSERT and not jobConfig.targetTableStage

        logger.info('Loading into {} a chunk at a time'.format(loadTable))

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
            logger.debug('Loaded {} row(s) into {} ({} so far)'.format(len(rows), loadTable, rowCount))

        logger.info('Streamed {} row(s) from {} into {}'.format(rowCount, jobConfig.sourceDatabase, loadTable))

        if jobConfig.insertStrategy == InsertStrategy.SWAP:
            assert jobConfig.targetTableStage is not None
            logger.info('Swapping {} with stage table {}'.format(jobConfig.targetTableFinal, jobConfig.targetTableStage))
            targetDatabase.swap(targetTable=jobConfig.targetTableFinal, stageTable=jobConfig.targetTableStage)

        if jobConfig.insertStrategy == InsertStrategy.UPSERT and jobConfig.targetTableStage:
            logger.info('Upserting {} from stage table {}'.format(jobConfig.targetTableFinal, jobConfig.targetTableStage))
            targetDatabase.upsertFromStage(targetTable=jobConfig.targetTableFinal, stageTable=jobConfig.targetTableStage, columns=columns)

        for postTargetAdhocQuery in jobConfig.postTargetAdhocQueries:
            logger.debug('Running postTargetAdhocQuery: {}'.format(postTargetAdhocQuery))
            targetDatabase.alter(postTargetAdhocQuery)

    maskingApplied = None
    if masking is not None:
        maskingApplied = {'columns': [entry._asdict() for entry in masking.manifest]}

    return JobOutcome(job=job, status=JobStatus.COMPLETED, rowCount=rowCount, watermark=highWatermark, masking=maskingApplied)


# Errors that a second attempt cannot fix. All are raised by this package itself
# and are deterministic: a transformer reference that doesn't resolve, a column
# the source query never returns, a watermark column that isn't selected, a
# masking policy that doesn't cover a column, a target without a primary key.
# Retrying them just delays a failure by retries * retryDelaySeconds and buries
# the real message under identical repeats. Everything else -- notably anything
# a driver raises -- is retried, because transient and permanent database errors
# cannot be told apart reliably across six drivers, and a needless retry costs
# far less than a nightly load lost to one dropped connection.
PERMANENT_ERRORS = (ConfigurationError, TransformError, TransformResolutionError, MaskingError)


def _executeWithRetries(jobConfig: DataJobConfig, job: str, attempt: Callable[[], JobOutcome]) -> JobOutcome:
    """Runs `attempt` up to 1 + jobConfig.retries times, backing off
    exponentially, and returns its outcome -- a FAILED one, carrying the last
    error, if no attempt succeeded.

    Retrying a whole data job is safe because both insert strategies converge on
    a re-run: `swap` restages and re-swaps, and `upsert` re-applies rows that are
    already there as a no-op.
    """

    for attemptNumber in range(1, jobConfig.retries + 2):

        try:
            return attempt()._replace(attempts=attemptNumber)

        except Exception as error:

            if isinstance(error, PERMANENT_ERRORS) or attemptNumber > jobConfig.retries:
                logger.error('Failed to complete {} due to error {}'.format(job, error), exc_info=error)
                return JobOutcome(job=job, status=JobStatus.FAILED, error='{}: {}'.format(type(error).__name__, error), attempts=attemptNumber)

            delay = jobConfig.retryDelaySeconds * (2 ** (attemptNumber - 1))
            logger.warning(
                'Attempt {} of {} for {} failed ({}: {}); retrying in {:.1f}s'.format(
                    attemptNumber, jobConfig.retries + 1, job, type(error).__name__, error, delay),
                extra={'job': job, 'attempt': attemptNumber, 'retryDelaySeconds': delay})
            time.sleep(delay)

    raise AssertionError('unreachable: the last attempt always returns')


def _runDataJob(job: str, jobConfig: DataJobConfig, databaseConfiguration: Dict[str, DatabaseConnectionConfig], memory: MemoryBackend) -> JobOutcome:
    """Runs one data job in a worker process, and records its success.

    On success each step commits before the next:

        load committed -> recordWatermark -> recordRun -> return the outcome

    so every point this can die at falls backwards, into re-reading rows already
    loaded -- harmless, because a watermark requires upsert.

    Nothing is recorded for a failed job. A stamped failure would suppress its
    retry for the whole refresh window, and an advanced watermark would skip rows
    permanently, which is the one unrecoverable direction.

    The stored watermark is read inside each attempt, so a memory backend that
    fails transiently is retried like the database it may well live in.

    A memory backend that fails to record is logged and tolerated: the data
    landed, so the job is honestly COMPLETED, and the cost is an earlier re-run.
    """

    logger.info('Starting {}'.format(job))
    startedAt = time.time()
    watermark = None

    def attempt() -> JobOutcome:
        nonlocal watermark
        if jobConfig.watermarkColumn:
            watermark = memory.readWatermarks().get(job, jobConfig.watermarkInitial)
        return _executeDataJob(job, jobConfig, databaseConfiguration, watermark=watermark)

    outcome = _executeWithRetries(jobConfig, job, attempt)

    if outcome.status == JobStatus.COMPLETED:

        if jobConfig.watermarkColumn and outcome.watermark is not None:
            try:
                memory.recordWatermark(job=job, value=outcome.watermark)
                logger.info('Advanced {} watermark to {!r}'.format(job, outcome.watermark))
            except Exception as error:
                logger.error('Completed {} but could not record its watermark -- the next run will re-extract from {!r}'.format(job, watermark),
                             exc_info=error)

        try:
            memory.recordRun(job=job)
        except Exception as error:
            logger.error('Completed {} but could not record its run -- it will re-run before its refresh window is up'.format(job), exc_info=error)

        logger.info('Completed {} ({} row(s))'.format(job, outcome.rowCount),
                    extra={'job': job, 'status': outcome.status.value, 'rowCount': outcome.rowCount, 'attempts': outcome.attempts})

    return outcome._replace(startedAt=startedAt, finishedAt=time.time())


def _initializeWorker(logQueue: Any, logLevel: int) -> None:
    """Runs once in each worker process.

    Ctrl-C reaches every process in the terminal's group, so without this a
    worker would die with KeyboardInterrupt mid-job. The parent decides how to
    stop instead: it lets running jobs finish. SIGTERM keeps its default, so a
    worker can still be killed on its own.
    """

    signal.signal(signal.SIGINT, signal.SIG_IGN)
    forwardToQueue(logQueue, logLevel)


def _logCycleSummary(dependencyGraph: DependencyGraph) -> None:
    """One line per terminal non-success, plus totals.

    A skipped job never reaches a worker, so this is the only place it gets
    reported -- named along with what it was waiting on, since a stale table
    with no log line is the hardest failure to diagnose.
    """

    result = RunResult(outcomes=list(dependencyGraph.outcomes))

    for outcome in result.failed:
        logger.error('{} failed after {:.1f}s: {}'.format(outcome.job, outcome.durationSeconds, outcome.error),
                     extra={'job': outcome.job, 'status': outcome.status.value, 'error': outcome.error,
                            'attempts': outcome.attempts, 'durationSeconds': round(outcome.durationSeconds, 3)})

    for outcome in result.skipped:
        logger.warning('{} skipped: {}'.format(outcome.job, outcome.error),
                       extra={'job': outcome.job, 'status': outcome.status.value, 'error': outcome.error})

    logger.info('Cycle finished: {} completed, {} failed, {} skipped, {} row(s) moved'.format(
        len(result.completed), len(result.failed), len(result.skipped), result.rowCount),
        extra={'event': 'cycleFinished', 'completed': len(result.completed), 'failed': len(result.failed),
               'skipped': len(result.skipped), 'rowCount': result.rowCount})


def _requireWatermarkCapableMemory(jobsFile: DataJobsFile, memory: MemoryBackend) -> None:
    """Fails before any work starts if an incremental job has nowhere to persist its watermark.

    MemoryBackend.recordWatermark is deliberately not abstract, so backends
    written before watermarks existed keep working for jobs that don't use one.
    The cost of that choice is that the mismatch would otherwise surface inside
    a worker process, after a job had already extracted and loaded its rows --
    and then on every cycle after that. Checking the class up front turns it
    into a configuration error, where it belongs.
    """

    incrementalJobs = sorted(name for name, job in jobsFile.jobs.items() if job.active and job.watermarkColumn)

    if incrementalJobs and type(memory).recordWatermark is MemoryBackend.recordWatermark:
        raise ConfigurationError(
            '{} does not implement recordWatermark, but these active job(s) configure a watermarkColumn: {}. '
            'Implement readWatermarks/recordWatermark on it, or use FileMemory.'.format(type(memory).__name__, ', '.join(incrementalJobs)))


class _Workers:
    """The process pool jobs run in, replaced if a worker dies.

    A worker killed mid-job -- by the kernel's OOM killer, a segfault in a
    driver, a stray `kill` -- breaks a ProcessPoolExecutor: every job running
    in it fails with BrokenProcessPool. That is reported as those jobs'
    failure, and the next job gets a fresh pool, so one crash can neither hang
    the run nor take the rest of the cycle with it.
    """

    def __init__(self, count: int, logQueue: Any, logLevel: int) -> None:
        self.count = count
        self.logQueue = logQueue
        self.logLevel = logLevel
        self._executor: Optional[ProcessPoolExecutor] = None


    def submit(self, function: Callable[..., JobOutcome], *arguments: Any) -> 'Future[JobOutcome]':

        if self._executor is None:
            self._executor = ProcessPoolExecutor(max_workers=self.count, initializer=_initializeWorker, initargs=(self.logQueue, self.logLevel))

        return self._executor.submit(function, *arguments)


    def discardBroken(self) -> None:

        if self._executor is not None:
            self._executor.shutdown(wait=False)
            self._executor = None


    def close(self) -> None:

        if self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None


def _runCycle(dependencyGraph: DependencyGraph, workers: _Workers, databaseConfiguration: Dict[str, DatabaseConnectionConfig],
              memory: MemoryBackend, termination: Dict[str, bool]) -> None:
    """Runs one cycle's jobs to completion, each as soon as its predecessors finish."""

    running: Dict['Future[JobOutcome]', Tuple[str, float]] = {}

    while not dependencyGraph.finished:

        if termination['terminating']:
            dependencyGraph.skipNotStarted('the run was stopped by a signal before this job started')
        else:
            for job in dependencyGraph.takeReady():
                future = workers.submit(_runDataJob, job, dependencyGraph.activeJobs[job], databaseConfiguration, memory)
                running[future] = (job, time.time())

        if not running:
            # Nothing running and nothing ready means every job is decided --
            # DependencyGraph refuses the cycles that could make this false.
            assert dependencyGraph.finished, 'jobs remain, yet none is running or ready'
            continue

        done, _ = wait(running, timeout=SIGNAL_POLL_SECONDS, return_when=FIRST_COMPLETED)

        for future in done:
            job, startedAt = running.pop(future)

            try:
                outcome = future.result()
            except BrokenProcessPool:
                workers.discardBroken()
                outcome = JobOutcome(job=job, status=JobStatus.FAILED, startedAt=startedAt, finishedAt=time.time(),
                                     error='WorkerDied: a worker process exited abruptly (killed, out of memory, or crashed) while this job was running')
                logger.error('{}: {}'.format(job, outcome.error))
            except Exception as error:
                outcome = JobOutcome(job=job, status=JobStatus.FAILED, startedAt=startedAt, finishedAt=time.time(),
                                     error='{}: {}'.format(type(error).__name__, error))
                logger.error('{} could not be run: {}'.format(job, outcome.error), exc_info=error)

            dependencyGraph.finish(outcome)


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
    skips eleven runs out of twelve.

    Use runForever=True for freshness below cron's one-minute floor, or where
    there is no scheduler to hook into. It runs until SIGINT or SIGTERM.

    Jobs run in worker processes, so `memory` is pickled and reconstructed in
    each, per MemoryBackend's contract, and so is everything a job's
    configuration references. Worker log records are sent back to this
    process and written by its handlers -- the ones Log sets up from logFile
    and logFormat, plus any the caller added -- so they share one format and
    one set of destinations.

    Worker processes are started with multiprocessing's default method, which
    re-imports the calling script on macOS and Windows (and on Linux from
    Python 3.14): call this from under `if __name__ == '__main__':`.
    """

    _requireWatermarkCapableMemory(jobsFile, memory)

    if jobsFile.workers < 1:
        raise ConfigurationError('workers must be at least 1, got {}'.format(jobsFile.workers))

    Log(logFile=logFile, level=logLevel, logFormat=logFormat)
    logger.info('Starting data job runner with {} worker(s)'.format(jobsFile.workers))

    with _terminationHandling() as termination, receiveForwardedRecords() as logQueue:

        workers = _Workers(jobsFile.workers, logQueue, logLevel)

        try:
            while True:
                dependencyGraph = DependencyGraph(jobs=jobsFile.jobs, memory=memory.read())
                logger.info('Starting cycle with {} active job(s)'.format(len(dependencyGraph.activeJobs)))

                _runCycle(dependencyGraph, workers, databaseConfiguration, memory, termination)
                _logCycleSummary(dependencyGraph)

                if not runForever or termination['terminating']:
                    break

                time.sleep(jobsFile.cycleSleepSeconds)
        finally:
            workers.close()

        logger.info('Finished data job runner')

    return RunResult(outcomes=list(dependencyGraph.outcomes), interrupted=termination['terminating'])
