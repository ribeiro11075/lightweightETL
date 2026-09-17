from __future__ import annotations

import contextlib
import logging
import multiprocessing as mp
import signal
import time
from multiprocessing.connection import wait as waitForAny
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Mapping, NamedTuple, Optional

from .configuration import ConfigurationError, DatabaseConnectionConfig, DataJobConfig, DataJobsFile, InsertStrategy
from .database import Database
from .dependencyGraph import DependencyGraph, JobOutcome, JobStatus
from .log import LOGGER_NAME, Log, forwardToQueue, receiveForwardedRecords
from .masking import BoundMasking, MaskingError, MaskingPlan, buildMaskingManifest, keyFingerprint
from .memory import MemoryBackend
from .transform import TransformError, Transformer, TransformResolutionError, resolveTransformer, Transform

logger = logging.getLogger(LOGGER_NAME)

# How often the run loop wakes while jobs are running, to notice SIGINT/SIGTERM.
# Job completions and timeouts wake it on time; this only bounds how late a
# signal is seen.
SIGNAL_POLL_SECONDS = 1.0

# Jobs run in processes started this way on every platform. `fork` -- Linux's
# default before Python 3.14 -- copies a parent that is running a log listener
# thread, which can deadlock the child on a lock that thread held.
PROCESS_CONTEXT = mp.get_context('spawn')

# How long a timed-out job gets to exit after SIGTERM before it is killed.
TERMINATE_GRACE_SECONDS = 5.0


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

        if jobConfig.masking is not None:
            try:
                memory.recordKeyFingerprint(job, keyFingerprint(jobConfig.masking.key.get_secret_value()))
            except Exception as error:
                logger.error('Completed {} but could not record its masking key fingerprint'.format(job), exc_info=error)

        try:
            memory.recordRun(job=job)
        except Exception as error:
            logger.error('Completed {} but could not record its run -- it will re-run before its refresh window is up'.format(job), exc_info=error)

        logger.info('Completed {} ({} row(s))'.format(job, outcome.rowCount),
                    extra={'job': job, 'status': outcome.status.value, 'rowCount': outcome.rowCount, 'attempts': outcome.attempts})

    return outcome._replace(startedAt=startedAt, finishedAt=time.time())


def _initializeWorker(logQueue: Any, logLevel: int) -> None:
    """Runs first in each job's process.

    Ctrl-C reaches every process in the terminal's group, so without this a
    job would die with KeyboardInterrupt part-way. The parent decides how to
    stop instead: it lets running jobs finish. SIGTERM keeps its default, which
    is how the parent stops a job that has run out of time.
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


def _jobProcess(outcomes: Any, logQueue: Any, logLevel: int, job: str, jobConfig: DataJobConfig,
                databaseConfiguration: Dict[str, DatabaseConnectionConfig], memory: MemoryBackend) -> None:
    """The whole life of one job's process: run the job, send back its outcome."""

    _initializeWorker(logQueue, logLevel)
    outcomes.send(_runDataJob(job, jobConfig, databaseConfiguration, memory))
    outcomes.close()


def _requireUnchangedMaskingKeys(jobsFile: DataJobsFile, memory: MemoryBackend, acceptKeyChange: bool) -> None:
    """Refuses to run an upsert job whose masking key changed since it last completed.

    Its target keeps the rows it already has, masked under the old key, and
    new rows would be masked under the new one: the same customer would get two
    different masked ids, and joins between old and new rows would silently
    stop matching. A swap job replaces its whole target, so a new key is
    harmless there. A key change is deliberate, so the fix is to acknowledge
    it -- after emptying the targets, or knowingly.
    """

    recorded = memory.readKeyFingerprints()
    changed = []

    for name, job in sorted(jobsFile.jobs.items()):
        if not job.active or job.masking is None or job.insertStrategy != InsertStrategy.UPSERT:
            continue
        previous = recorded.get(name)
        current = keyFingerprint(job.masking.key.get_secret_value())
        if previous is not None and previous != current:
            changed.append('{} (was {}, now {})'.format(name, previous, current))

    if not changed:
        return

    if acceptKeyChange:
        logger.warning('Masking key changed for {}; continuing, as acknowledged'.format(', '.join(changed)))
        return

    raise ConfigurationError(
        'the masking key changed since the last run of upsert job(s) {}. Their targets still hold rows masked under the old key, '
        'which would no longer match rows masked under the new one. Empty those targets first (lightweight-etl clear, which also '
        'forgets the old key), or acknowledge the change with --accept-key-change'.format(', '.join(changed)))


class _JobProcess:
    """One job, running in a process of its own.

    A process per job rather than a slot in a shared pool, so each job can be
    ended on its own: one past its timeoutSeconds is stopped, and one whose
    process dies -- killed for memory, crashed in a driver -- fails without
    taking any other job with it. Starting a process costs a fraction of a
    second, which is noise beside a database load.

    The outcome comes back over a pipe whose sending end only the child holds,
    so a child that dies without sending shows up as end-of-file.
    """

    def __init__(self, job: str, jobConfig: DataJobConfig, databaseConfiguration: Dict[str, DatabaseConnectionConfig],
                 memory: MemoryBackend, logQueue: Any, logLevel: int) -> None:
        self.job = job
        self.startedAt = time.time()
        self.deadline = self.startedAt + jobConfig.timeoutSeconds if jobConfig.timeoutSeconds else None

        self._outcomes, sendingEnd = PROCESS_CONTEXT.Pipe(duplex=False)
        self.process = PROCESS_CONTEXT.Process(
            target=_jobProcess, name='lightweight-etl {}'.format(job), daemon=True,
            args=(sendingEnd, logQueue, logLevel, job, jobConfig, databaseConfiguration, memory))
        self.process.start()
        sendingEnd.close()


    @property
    def waitables(self) -> List[Any]:

        return [self._outcomes, self.process.sentinel]


    def poll(self, now: float) -> Optional[JobOutcome]:
        """The job's outcome once it is over -- finished, died or timed out --
        and None while it is still running.
        """

        if self._outcomes.poll():
            try:
                outcome: JobOutcome = self._outcomes.recv()
            except EOFError:
                return self._died()
            self.process.join()
            return outcome

        if not self.process.is_alive():
            return self._died()

        if self.deadline is not None and now >= self.deadline:
            self.stop()
            timeoutSeconds = self.deadline - self.startedAt
            logger.error('{} exceeded timeoutSeconds ({:g}) and was stopped'.format(self.job, timeoutSeconds),
                         extra={'job': self.job, 'status': JobStatus.FAILED.value})
            return self._failed('Timeout: stopped after exceeding timeoutSeconds ({:g})'.format(timeoutSeconds))

        return None


    def stop(self) -> None:
        """SIGTERM, then SIGKILL if it hasn't exited within the grace period.

        Its database connections close with it, so each server rolls back
        whatever the job had not committed.
        """

        self.process.terminate()
        self.process.join(TERMINATE_GRACE_SECONDS)

        if self.process.is_alive():
            self.process.kill()
            self.process.join()


    def _died(self) -> JobOutcome:

        self.process.join()
        logger.error('{}: its process exited with code {} before reporting an outcome'.format(self.job, self.process.exitcode),
                     extra={'job': self.job, 'status': JobStatus.FAILED.value})

        return self._failed('WorkerDied: the job\'s process exited abruptly (code {}) -- killed, out of memory, or crashed'.format(
            self.process.exitcode))


    def _failed(self, error: str) -> JobOutcome:

        return JobOutcome(job=self.job, status=JobStatus.FAILED, error=error, startedAt=self.startedAt, finishedAt=time.time())


def _runCycle(dependencyGraph: DependencyGraph, workers: int, databaseConfiguration: Dict[str, DatabaseConnectionConfig],
              memory: MemoryBackend, termination: Dict[str, bool], logQueue: Any, logLevel: int) -> None:
    """Runs one cycle's jobs to completion, each as soon as its predecessors
    finish and one of the `workers` slots is free.
    """

    running: List[_JobProcess] = []

    try:
        while not dependencyGraph.finished:

            if termination['terminating']:
                dependencyGraph.skipNotStarted('the run was stopped by a signal before this job started')
            else:
                for job in dependencyGraph.takeReady(limit=workers - len(running)):
                    running.append(_JobProcess(job, dependencyGraph.activeJobs[job], databaseConfiguration, memory, logQueue, logLevel))  # type: ignore[arg-type]

            if not running:
                # Nothing running and nothing startable means every job is
                # decided -- DependencyGraph refuses the cycles that could make
                # this false.
                assert dependencyGraph.finished, 'jobs remain, yet none is running or ready'
                continue

            timeout = SIGNAL_POLL_SECONDS
            deadlines = [process.deadline for process in running if process.deadline is not None]
            if deadlines:
                timeout = max(0.0, min(timeout, min(deadlines) - time.time()))

            waitForAny([waitable for process in running for waitable in process.waitables], timeout=timeout)

            now = time.time()
            for process in list(running):
                outcome = process.poll(now)
                if outcome is not None:
                    running.remove(process)
                    dependencyGraph.finish(outcome)
    finally:
        # Only reached with jobs still running if something above raised.
        for process in running:
            process.stop()


def runDataJobs(jobsFile: DataJobsFile, databaseConfiguration: Dict[str, DatabaseConnectionConfig], memory: MemoryBackend,
                logFile: Optional[Path] = None, runForever: bool = False, logLevel: int = logging.INFO,
                logFormat: str = 'text', acceptKeyChange: bool = False,
                onCycle: Optional[Callable[['RunResult'], None]] = None) -> RunResult:
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

    `onCycle` is called with each cycle's RunResult as the cycle ends -- the
    place to keep history, publish metrics or notify, which a run that never
    returns couldn't otherwise do. An exception from it is logged, not raised:
    reporting must not stop the loads.

    A masked upsert job whose key changed since it last completed stops the run
    before anything starts, unless acceptKeyChange; see
    _requireUnchangedMaskingKeys.

    Each job runs in a process of its own, at most jobsFile.workers at once,
    so `memory` is pickled and reconstructed in each, per MemoryBackend's
    contract, and so is everything a job's configuration references. Their log
    records are sent back to this process and written by its handlers -- the
    ones Log sets up from logFile and logFormat, plus any the caller added --
    so they share one format and one set of destinations.

    Those processes are started with multiprocessing's `spawn` method, which
    imports the calling script afresh: call this from under
    `if __name__ == '__main__':`.
    """

    _requireWatermarkCapableMemory(jobsFile, memory)
    _requireUnchangedMaskingKeys(jobsFile, memory, acceptKeyChange)

    if jobsFile.workers < 1:
        raise ConfigurationError('workers must be at least 1, got {}'.format(jobsFile.workers))

    Log(logFile=logFile, level=logLevel, logFormat=logFormat)
    logger.info('Starting data job runner with {} worker(s)'.format(jobsFile.workers))

    with _terminationHandling() as termination, receiveForwardedRecords(PROCESS_CONTEXT) as logQueue:

        while True:
            dependencyGraph = DependencyGraph(jobs=jobsFile.jobs, memory=memory.read())
            logger.info('Starting cycle with {} active job(s)'.format(len(dependencyGraph.activeJobs)))

            _runCycle(dependencyGraph, jobsFile.workers, databaseConfiguration, memory, termination, logQueue, logLevel)
            _logCycleSummary(dependencyGraph)

            if onCycle is not None:
                try:
                    onCycle(RunResult(outcomes=list(dependencyGraph.outcomes), interrupted=termination['terminating']))
                except Exception as error:
                    logger.error('Reporting on the cycle failed: {}: {}'.format(type(error).__name__, error), exc_info=error)

            if not runForever or termination['terminating']:
                break

            time.sleep(jobsFile.cycleSleepSeconds)

        logger.info('Finished data job runner')

    return RunResult(outcomes=list(dependencyGraph.outcomes), interrupted=termination['terminating'])
