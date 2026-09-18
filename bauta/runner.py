from __future__ import annotations

import collections
import contextlib
import logging
import multiprocessing as mp
import os
import signal
import time
from concurrent.futures import Future, ThreadPoolExecutor
from multiprocessing.connection import wait as waitForAny
from pathlib import Path
from typing import Any, Callable, Deque, Dict, Iterable, Iterator, List, Mapping, NamedTuple, Optional, Sequence, Tuple

from .configuration import ConfigurationError, DatabaseConnectionConfig, DataJobConfig, DataJobsFile, InsertStrategy
from .database import Database
from .dependencyGraph import DependencyGraph, JobOutcome, JobStatus
from .log import LOGGER_NAME, ConnectionForwarder, Log, forwardToConnection, handleForwardedRecord
from . import masking as maskingModule
from .masking import (BoundMasking, MaskingError, MaskingPlan, buildMaskingManifest, keyFingerprint, maskingIdentity,
                      maskingImplementation, splitMaskingIdentity)
from .memory import MemoryBackend
from .scrubbing import describeError
from .transform import TransformError, Transformer, TransformResolutionError, resolveTransformer, Transform

logger = logging.getLogger(LOGGER_NAME)

# How late the run loop may notice SIGINT/SIGTERM. Completions and timeouts
# wake it on time.
SIGNAL_POLL_SECONDS = 1.0

# Jobs run in processes started this way on every platform. `fork` -- Linux's
# default before Python 3.14 -- copies whatever locks the parent's threads
# happen to hold, which can deadlock the child.
PROCESS_CONTEXT = mp.get_context('spawn')

# How long a timed-out job gets to exit after SIGTERM before it is killed.
TERMINATE_GRACE_SECONDS = 5.0

# How long a job may take to exit once it has sent its outcome, before it is
# stopped. It has nothing left to do by then but close its connections.
EXIT_GRACE_SECONDS = 10.0

# How many chunks may be masked ahead of the one being written. One already
# keeps the reader, masker and writer all busy, holding three chunks at once;
# more buys no overlap and costs a chunk of memory each. (It also beats larger
# chunks: at 10 ms a round trip, three chunks of 5,000 rows took 2.24s against
# 2.63s for one of 20,000.)
PIPELINE_DEPTH = 1

# Caps the doubling backoff, which would otherwise wait 5.7 hours in all over
# `retries: 12`.
MAXIMUM_RETRY_DELAY_SECONDS = 300.0

# Messages read from one job's pipe before looking at the others, so a job
# logging without pause can't starve the rest.
MESSAGES_PER_POLL = 500


@contextlib.contextmanager
def _terminationHandling() -> Iterator[Dict[str, bool]]:
    """Turns SIGINT/SIGTERM into a flag the run loop acts on, restoring the
    previous handlers on the way out. See "Stopping" in docs/design.md.

    The handler only sets the flag: teardown inside it would run on whatever
    frame was executing, possibly inside multiprocessing. Off the main thread,
    where signal.signal raises, no handlers are installed.
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
    """What one call to runDataJobs did -- with runForever, its last cycle.
    `interrupted` says a signal ended the run.

    Returned rather than stored: MemoryBackend holds scheduler input, and this
    is output for the caller to act on.
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
        """True only if every active job completed; a skipped job counts as a
        failure, since its data isn't there.
        """

        return not self.failed and not self.skipped


    def maskingManifest(self, jobs: Mapping[str, DataJobConfig]) -> Dict[str, Any]:
        """See masking.buildMaskingManifest. Takes the configurations because a
        skipped job still belongs in the manifest but has no outcome to describe it.
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
    """Binds the job's masking policy to the columns its query returned, raising
    MaskingError before anything is written if it doesn't cover them all.
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
    """Runs one data job to completion, raising on failure. See "How a data job
    moves rows" in docs/design.md.

    Transforms and the masking policy are both checked against the query's
    columns before the first write, so a misconfigured job fails with nothing
    loaded. Masking runs after transforms, so values are normalized before
    they are keyed.
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

        def writeChunk(rows: List[Any], watermark: Any) -> None:
            nonlocal rowCount, highWatermark

            if streamsDirectlyIntoTarget:
                targetDatabase.upsert(table=loadTable, data=rows, chunkSize=jobConfig.chunkSize, columns=columns)
            else:
                targetDatabase.insert(table=loadTable, data=rows, chunkSize=jobConfig.chunkSize, columns=columns)

            rowCount += len(rows)
            # Only once the rows have landed, or a failed job's next run would
            # start past them.
            if watermark is not None and (highWatermark is None or watermark > highWatermark):
                highWatermark = watermark

            logger.debug('Loaded {} row(s) into {} ({} so far)'.format(len(rows), loadTable, rowCount))

        def prepareChunk(chunkIndex: int, chunk: List[Tuple[Any, ...]]) -> List[Any]:
            rows = transform.apply(chunk)

            if masking is not None:
                # By read order, not masking order: `shuffle` keys on it.
                rows = masking.apply(rows, chunkIndex=chunkIndex)

            return rows

        _streamChunks(chunks, prepareChunk, writeChunk, watermarkIndex, _pipelineDepth())

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


def _pipelineDepth() -> int:
    """PIPELINE_DEPTH, or 0 to read, mask and write strictly in turn.

    On by default only with the native masker, the only place it pays:

        200,000 rows, 6 masked columns, 5 ms round trip each way

        Python masking, in turn      11.57s
        Python masking, overlapped   11.79s     0.98x
        native masking, in turn       2.92s     3.96x
        native masking, overlapped    2.16s     5.35x

    BAUTA_PIPELINE=1 or =0 overrides the default.
    """

    setting = os.environ.get('BAUTA_PIPELINE')

    if setting is not None:
        return PIPELINE_DEPTH if setting == '1' else 0

    return PIPELINE_DEPTH if maskingModule.nativeVersion() is not None else 0


def _highestWatermark(chunk: Sequence[Sequence[Any]], index: Optional[int]) -> Any:
    """The largest value of the watermark column in one chunk, or None. Read
    from the raw rows, since a transform may reformat the column.
    """

    if index is None:
        return None

    highest = None
    for row in chunk:
        value = row[index]
        if value is not None and (highest is None or value > highest):
            highest = value

    return highest


def _streamChunks(chunks: Iterable[List[Tuple[Any, ...]]], prepare: Callable[[int, List[Tuple[Any, ...]]], List[Any]],
                  write: Callable[[List[Any], Any], None], watermarkIndex: Optional[int], depth: int) -> None:
    """Prepares (transforms and masks) each chunk and writes it, in source
    order. With `depth` above 0, preparing runs on one worker thread up to
    `depth` chunks ahead, overlapping the masker with the database.

    Both connections stay on the calling thread: mysqlclient, PyMySQL and
    sqlite3 refuse use from any other.
    """

    if depth == 0:
        for chunkIndex, chunk in enumerate(chunks):
            write(prepare(chunkIndex, chunk), _highestWatermark(chunk, watermarkIndex))
        return

    pending: Deque[Tuple['Future[List[Any]]', Any]] = collections.deque()

    with ThreadPoolExecutor(max_workers=1, thread_name_prefix='bauta-rs') as executor:
        try:
            for chunkIndex, chunk in enumerate(chunks):
                pending.append((executor.submit(prepare, chunkIndex, chunk), _highestWatermark(chunk, watermarkIndex)))
                while len(pending) > depth:
                    future, watermark = pending.popleft()
                    write(future.result(), watermark)

            while pending:
                future, watermark = pending.popleft()
                write(future.result(), watermark)
        except BaseException:
            # Whatever is queued behind the failure is no longer wanted.
            executor.shutdown(wait=False, cancel_futures=True)
            raise


# Deterministic errors, raised by this package, that a retry can't fix.
# Everything else is retried; see "Retries" in docs/design.md.
PERMANENT_ERRORS = (ConfigurationError, TransformError, TransformResolutionError, MaskingError)


def _executeWithRetries(jobConfig: DataJobConfig, job: str, attempt: Callable[[], JobOutcome]) -> JobOutcome:
    """Runs `attempt` up to 1 + jobConfig.retries times with doubling backoff,
    returning its outcome, or a FAILED one carrying the last error.
    """

    for attemptNumber in range(1, jobConfig.retries + 2):

        try:
            return attempt()._replace(attempts=attemptNumber)

        except Exception as error:

            if isinstance(error, PERMANENT_ERRORS) or attemptNumber > jobConfig.retries:
                logger.error('Failed to complete {} due to error {}'.format(job, error), exc_info=error)
                return JobOutcome(job=job, status=JobStatus.FAILED, error=describeError(error), attempts=attemptNumber)

            delay = min(MAXIMUM_RETRY_DELAY_SECONDS, jobConfig.retryDelaySeconds * (2 ** min(attemptNumber - 1, 32)))
            logger.warning(
                'Attempt {} of {} for {} failed ({}); retrying in {:.1f}s'.format(
                    attemptNumber, jobConfig.retries + 1, job, describeError(error), delay),
                extra={'job': job, 'attempt': attemptNumber, 'retryDelaySeconds': delay})
            time.sleep(delay)

    raise AssertionError('unreachable: the last attempt always returns')


def _runDataJob(job: str, jobConfig: DataJobConfig, databaseConfiguration: Dict[str, DatabaseConnectionConfig], memory: MemoryBackend) -> JobOutcome:
    """Runs one data job in a worker process, and records its success. The
    order of the records is what makes a crash safe; see "Crash safety" in
    docs/design.md.

    Nothing is recorded for a failed job. A failure to record is logged, not
    raised: the data landed, and the cost is an earlier re-run.
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
                memory.recordKeyFingerprint(job, maskingIdentity(jobConfig.masking.key.get_secret_value()))
            except Exception as error:
                logger.error('Completed {} but could not record its masking key fingerprint'.format(job), exc_info=error)

        try:
            memory.recordRun(job=job)
        except Exception as error:
            logger.error('Completed {} but could not record its run -- it will re-run before its refresh window is up'.format(job), exc_info=error)

        logger.info('Completed {} ({} row(s))'.format(job, outcome.rowCount),
                    extra={'job': job, 'status': outcome.status.value, 'rowCount': outcome.rowCount, 'attempts': outcome.attempts})

    return outcome._replace(startedAt=startedAt, finishedAt=time.time())


def _initializeWorker(connection: Any, logLevel: int) -> ConnectionForwarder:
    """Runs first in each job's process. Ignores Ctrl-C, which reaches the whole
    process group, so the parent decides how to stop; SIGTERM keeps its
    default, since that is how a timed-out job is stopped.
    """

    signal.signal(signal.SIGINT, signal.SIG_IGN)

    return forwardToConnection(connection, logLevel)


def _logCycleSummary(dependencyGraph: DependencyGraph) -> None:
    """One line per failed or skipped job, plus totals. The only place a
    skipped job is logged, since it never reaches a worker.
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
    """Fails before any work starts if an incremental job has nowhere to persist
    its watermark. recordWatermark isn't abstract, so older backends still
    work for jobs without one -- and the mismatch would otherwise surface only
    after a job had loaded its rows.
    """

    incrementalJobs = sorted(name for name, job in jobsFile.jobs.items() if job.active and job.watermarkColumn)

    if incrementalJobs and type(memory).recordWatermark is MemoryBackend.recordWatermark:
        raise ConfigurationError(
            '{} does not implement recordWatermark, but these active job(s) configure a watermarkColumn: {}. '
            'Implement readWatermarks/recordWatermark on it, or use FileMemory.'.format(type(memory).__name__, ', '.join(incrementalJobs)))


def _jobProcess(connection: Any, logLevel: int, job: str, jobConfig: DataJobConfig,
                databaseConfiguration: Dict[str, DatabaseConnectionConfig], memory: MemoryBackend) -> None:
    """The whole life of one job's process: run the job, and send its log
    records and then its outcome back on `connection`, which it alone writes to.
    """

    forwarder = _initializeWorker(connection, logLevel)
    forwarder.send('outcome', _runDataJob(job, jobConfig, databaseConfiguration, memory))
    connection.close()


def _requireUnchangedMaskingKeys(jobsFile: DataJobsFile, memory: MemoryBackend, acceptKeyChange: bool) -> None:
    """Refuses to run an upsert job whose masking key changed since it last
    completed: its target's existing rows would no longer join with new ones.
    A swap job replaces its whole target, so it isn't checked.
    """

    recorded = memory.readKeyFingerprints()
    changed = []
    reimplemented = []

    for name, job in sorted(jobsFile.jobs.items()):
        if not job.active or job.masking is None or job.insertStrategy != InsertStrategy.UPSERT:
            continue

        if recorded.get(name) is None:
            continue

        previousKey, previousImplementation = splitMaskingIdentity(recorded[name])
        currentKey = keyFingerprint(job.masking.key.get_secret_value())

        if previousKey != currentKey:
            changed.append('{} (was {}, now {})'.format(name, previousKey, currentKey))
        elif previousImplementation is not None and previousImplementation != maskingImplementation():
            reimplemented.append('{} (was {}, now {})'.format(name, previousImplementation, maskingImplementation()))

    # Warned rather than refused: the implementations are tested to agree (see
    # maskingIdentity), and refusing would stop every upsert job whenever the
    # extension was installed.
    if reimplemented:
        logger.warning('Masking implementation changed since the last run of upsert job(s) {}. The two are tested to produce '
                       'identical masks, so this is recorded rather than refused -- but if rows masked before and after stop '
                       'joining, this is why'.format(', '.join(reimplemented)))

    if not changed:
        return

    if acceptKeyChange:
        logger.warning('Masking key changed for {}; continuing, as acknowledged'.format(', '.join(changed)))
        return

    raise ConfigurationError(
        'the masking key changed since the last run of upsert job(s) {}. Their targets still hold rows masked under the old key, '
        'which would no longer match rows masked under the new one. Empty those targets first (bauta clear, which also '
        'forgets the old key), or acknowledge the change with --accept-key-change'.format(', '.join(changed)))


class _JobProcess:
    """One job, running in a process of its own so it can be stopped or die
    alone; see "Workers" in docs/design.md.

    Log records and the outcome come back over a pipe only this job writes to.
    The child holds the only sending end, so its death shows up as end-of-file.
    """

    def __init__(self, job: str, jobConfig: DataJobConfig, databaseConfiguration: Dict[str, DatabaseConnectionConfig],
                 memory: MemoryBackend, logLevel: int) -> None:
        self.job = job
        self.startedAt = time.time()
        self.deadline = self.startedAt + jobConfig.timeoutSeconds if jobConfig.timeoutSeconds else None
        self._outcome: Optional[JobOutcome] = None
        self._closed = False

        self._connection, sendingEnd = PROCESS_CONTEXT.Pipe(duplex=False)
        self.process = PROCESS_CONTEXT.Process(
            target=_jobProcess, name='bauta {}'.format(job), daemon=True,
            args=(sendingEnd, logLevel, job, jobConfig, databaseConfiguration, memory))
        self.process.start()
        sendingEnd.close()


    @property
    def waitables(self) -> List[Any]:

        return [self.process.sentinel] if self._closed else [self._connection, self.process.sentinel]


    def _read(self) -> None:
        """Handles what the job has sent, up to MESSAGES_PER_POLL messages.
        Marks the pipe closed at end-of-file.
        """

        for _ in range(MESSAGES_PER_POLL):
            if self._closed or not self._connection.poll():
                return
            try:
                kind, payload = self._connection.recv()
            except (EOFError, OSError):
                self._closed = True
                self._connection.close()
                return
            if kind == 'log':
                handleForwardedRecord(payload)
            elif kind == 'outcome':
                self._outcome = payload


    def poll(self, now: float) -> Optional[JobOutcome]:
        """The job's outcome once it is over -- finished, died or timed out --
        and None while it is still running.
        """

        self._read()

        if self._outcome is not None:
            # Nothing is left for it to do but exit; don't wait on it forever.
            self.process.join(EXIT_GRACE_SECONDS)
            if self.process.is_alive():
                logger.warning('{} sent its outcome but did not exit; stopping it'.format(self.job))
                self.stop()
            self._drain()
            return self._outcome

        if not self.process.is_alive():
            self._drain()
            if self._outcome is not None:
                return self._outcome
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

        if not self._closed:
            self._closed = True
            self._connection.close()


    def _drain(self) -> None:
        """Handles whatever an exited job left in its pipe. Bounded, since an
        exited job can't write more.
        """

        while not self._closed:
            self._read()
            if not self._closed and not self._connection.poll():
                self._closed = True
                self._connection.close()


    def _died(self) -> JobOutcome:

        self.process.join()
        logger.error('{}: its process exited with code {} before reporting an outcome'.format(self.job, self.process.exitcode),
                     extra={'job': self.job, 'status': JobStatus.FAILED.value})

        return self._failed('WorkerDied: the job\'s process exited abruptly (code {}) -- killed, out of memory, or crashed'.format(
            self.process.exitcode))


    def _failed(self, error: str) -> JobOutcome:

        return JobOutcome(job=self.job, status=JobStatus.FAILED, error=error, startedAt=self.startedAt, finishedAt=time.time())


def _runCycle(dependencyGraph: DependencyGraph, workers: int, databaseConfiguration: Dict[str, DatabaseConnectionConfig],
              memory: MemoryBackend, termination: Dict[str, bool], logLevel: int) -> None:
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
                    running.append(_JobProcess(job, dependencyGraph.activeJobs[job], databaseConfiguration, memory, logLevel))  # type: ignore[arg-type]

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
    """Runs data jobs, honoring each job's `refresh` window and `predecessors`:
    one pass, or with runForever until SIGINT or SIGTERM. See "Single runs,
    not a daemon" in docs/design.md.

    `onCycle` receives each cycle's RunResult, for history, metrics or alerts;
    an exception from it is logged, not raised. A masked upsert job whose key
    changed stops the run before it starts, unless acceptKeyChange.

    Each job runs in its own process, so `memory` and each job's configuration
    are pickled. Processes are spawned, which re-imports the calling script:
    call this under `if __name__ == '__main__':`.
    """

    _requireWatermarkCapableMemory(jobsFile, memory)
    _requireUnchangedMaskingKeys(jobsFile, memory, acceptKeyChange)

    if jobsFile.workers < 1:
        raise ConfigurationError('workers must be at least 1, got {}'.format(jobsFile.workers))

    Log(logFile=logFile, level=logLevel, logFormat=logFormat)
    logger.info('Starting data job runner with {} worker(s)'.format(jobsFile.workers))

    with _terminationHandling() as termination:

        while True:
            dependencyGraph = DependencyGraph(jobs=jobsFile.jobs, memory=memory.read())
            logger.info('Starting cycle with {} active job(s)'.format(len(dependencyGraph.activeJobs)))

            _runCycle(dependencyGraph, jobsFile.workers, databaseConfiguration, memory, termination, logLevel)
            _logCycleSummary(dependencyGraph)

            if onCycle is not None:
                try:
                    onCycle(RunResult(outcomes=list(dependencyGraph.outcomes), interrupted=termination['terminating']))
                except Exception as error:
                    logger.error('Reporting on the cycle failed: {}'.format(describeError(error)), exc_info=error)

            if not runForever or termination['terminating']:
                break

            time.sleep(jobsFile.cycleSleepSeconds)

        logger.info('Finished data job runner')

    return RunResult(outcomes=list(dependencyGraph.outcomes), interrupted=termination['terminating'])
