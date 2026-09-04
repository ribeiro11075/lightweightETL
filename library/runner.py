from __future__ import annotations

import logging
import multiprocessing as mp
import time
from multiprocessing.pool import Pool
from pathlib import Path
from typing import Dict, List, Optional

from .configurationInterface import DatabaseConnectionConfig, DataJobConfig, DataJobsFile, InsertStrategy, ScrambleJobConfig, ScrambleJobsFile
from .databaseDialects import ColumnCategory
from .databaseInterface import Database
from .databaseScrambleInterface import Scramble
from .dependencyGraphInterface import DependencyGraph, JobStatus
from .logInterface import Log
from .memoryInterface import MemoryBackend
from .transformInterface import Transformer, resolveTransformer, Transform


def _executeDataJob(jobConfig: DataJobConfig, databaseConfiguration: Dict[str, DatabaseConnectionConfig], log: Optional[Log] = None) -> None:
    """Runs one data job to completion, raising on failure.

    Each sourceQueryColumnTransforms entry is a "module.path:function_name" reference,
    resolved via resolveTransformer rather than a fixed lookup table. One source and one
    target Database connection are opened for the job and reused for every step, rather
    than a fresh connection per query. The swap branch asserts targetTableStage is set
    because DataJobConfig's validator guarantees that whenever insertStrategy is swap.

    Transforms run against sourceQuery's own result columns (from cursor.description,
    via getLastQueryColumnNames -- reflects whatever that query actually selected,
    explicit list or `select *` alike), *not* the target table -- a transform operates
    on a value as extracted from the source, before it's mapped onto any target column
    name, so it doesn't matter whether that name means anything to the target.

    columns (the target side) is resolved separately -- from targetColumns if the job
    configured it, otherwise by introspecting targetTableFinal -- and used for every
    insert/upsert call's column list, rather than each step re-introspecting the table
    independently. sourceQuery's SELECT list is assumed to match columns positionally;
    if targetColumns is unset, that means matching targetTableFinal's own column order
    exactly.
    """

    columnTransforms: Dict[str, List[Transformer]] = {
        column: [resolveTransformer(reference) for reference in references] for column, references in jobConfig.sourceQueryColumnTransforms.items()
        }

    sourceDatabaseConnectionSettings = databaseConfiguration[jobConfig.sourceDatabase]
    targetDatabaseConnectionSettings = databaseConfiguration[jobConfig.targetDatabase]

    with Database(connectionSettings=sourceDatabaseConnectionSettings) as sourceDatabase, \
         Database(connectionSettings=targetDatabaseConnectionSettings) as targetDatabase:

        if log:
            log.logging.debug('Running sourceQuery against {}'.format(jobConfig.sourceDatabase))
        data = sourceDatabase.query(query=jobConfig.sourceQuery)
        sourceQueryColumns = sourceDatabase.getLastQueryColumnNames()
        if log:
            log.logging.info('Extracted {} row(s) from {}'.format(len(data), jobConfig.sourceDatabase))
            log.logging.debug('sourceQuery returned columns: {}'.format(sourceQueryColumns))

        transform = Transform(data=data, columns=sourceQueryColumns, columnTransforms=columnTransforms)
        data = transform.transform()
        if log and columnTransforms:
            log.logging.debug('Applied transforms to column(s): {}'.format(', '.join(columnTransforms)))

        columns = jobConfig.targetColumns or targetDatabase.getAllColumnNames(table=jobConfig.targetTableFinal)
        if log:
            log.logging.debug('Resolved target columns for {}: {}'.format(jobConfig.targetTableFinal, columns))

        if jobConfig.targetTableStage:
            if log:
                log.logging.debug('Truncating stage table {}'.format(jobConfig.targetTableStage))
            targetDatabase.truncate(table=jobConfig.targetTableStage)
            if log:
                log.logging.info('Inserting {} row(s) into stage table {}'.format(len(data), jobConfig.targetTableStage))
            targetDatabase.insert(table=jobConfig.targetTableStage, data=data, chunkSize=jobConfig.chunkSize, columns=columns)

        for preTargetAdhocQuery in jobConfig.preTargetAdhocQueries:
            if log:
                log.logging.debug('Running preTargetAdhocQuery: {}'.format(preTargetAdhocQuery))
            targetDatabase.alter(preTargetAdhocQuery)

        if jobConfig.insertStrategy == InsertStrategy.SWAP:
            assert jobConfig.targetTableStage is not None
            if log:
                log.logging.info('Swapping {} with stage table {}'.format(jobConfig.targetTableFinal, jobConfig.targetTableStage))
            targetDatabase.swap(targetTable=jobConfig.targetTableFinal, stageTable=jobConfig.targetTableStage)

        if jobConfig.insertStrategy == InsertStrategy.UPSERT:
            if jobConfig.targetTableStage:
                if log:
                    log.logging.info('Upserting {} from stage table {}'.format(jobConfig.targetTableFinal, jobConfig.targetTableStage))
                targetDatabase.upsertFromStage(targetTable=jobConfig.targetTableFinal, stageTable=jobConfig.targetTableStage, columns=columns)
            else:
                if log:
                    log.logging.info('Upserting {} row(s) directly into {}'.format(len(data), jobConfig.targetTableFinal))
                targetDatabase.upsert(table=jobConfig.targetTableFinal, data=data, chunkSize=jobConfig.chunkSize, columns=columns)

        for postTargetAdhocQuery in jobConfig.postTargetAdhocQueries:
            if log:
                log.logging.debug('Running postTargetAdhocQuery: {}'.format(postTargetAdhocQuery))
            targetDatabase.alter(postTargetAdhocQuery)


def _dataJobWorker(readyQueue: mp.Queue, completedQueue: mp.Queue, activeJobs: Dict[str, DataJobConfig],
                    databaseConfiguration: Dict[str, DatabaseConnectionConfig], logDirectory: Path, memory: MemoryBackend,
                    logLevel: int = logging.INFO) -> None:
    """Runs data jobs pulled off readyQueue, via _executeDataJob, until the process is torn down."""

    log = Log(logDirectory=logDirectory, level=logLevel)

    while True:

        job = readyQueue.get()
        log.logging.info('Starting {}'.format(job))

        try:
            _executeDataJob(activeJobs[job], databaseConfiguration, log=log)
            status = JobStatus.COMPLETED
            log.logging.info('Completed {}'.format(job))

        except Exception as error:
            status = JobStatus.FAILED
            log.logging.error('Failed to complete {} due to error {}'.format(job, error), exc_info=error)

        completedQueue.put({job: status})
        memory.recordRun(job=job)


def runDataJobs(jobsFile: DataJobsFile, databaseConfiguration: Dict[str, DatabaseConnectionConfig], logDirectory: Path, memory: MemoryBackend,
                 runForever: bool = True, logLevel: int = logging.INFO) -> None:
    """Runs data jobs, honoring each job's `refresh` window and `predecessors`.

    The caller's only responsibility is configuration: the validated jobs/database
    config, where logs should live, a MemoryBackend for run-history (FileMemory by
    default -- see memoryInterface.py; write your own to persist it elsewhere, e.g.
    a database), and whether this is a one-time pass (runForever=False) or should
    keep running (runForever=True, the default -- `refresh` only means anything in
    a long-running process). Worker processes and their pool are managed entirely
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

    log = Log(logDirectory=logDirectory, level=logLevel)
    log.logging.info('Starting data job runner with {} worker(s)'.format(jobsFile.workers))

    pool: Optional[Pool] = None

    while True:

        dependencyGraph = DependencyGraph(jobs=jobsFile.jobs, memory=memory.read())
        log.logging.info('Starting cycle with {} active job(s)'.format(len(dependencyGraph.activeJobs)))

        if pool is not None:
            pool.terminate()
            pool.join()

        pool = mp.Pool(jobsFile.workers, _dataJobWorker,
                        (dependencyGraph.readyQueue, dependencyGraph.completedQueue, dependencyGraph.activeJobs,
                         databaseConfiguration, logDirectory, memory, logLevel))
        dependencyGraph.run()

        pool.close()
        log.logging.info('Cycle finished: {} completed, {} failed'.format(len(dependencyGraph.completedJobs), len(dependencyGraph.failedJobs)))

        if not runForever:
            pool.terminate()
            pool.join()
            break

        time.sleep(jobsFile.cycleSleepSeconds)

    log.logging.info('Finished data job runner')


def _executeScrambleJob(job: str, jobConfig: ScrambleJobConfig, databaseConfiguration: Dict[str, DatabaseConnectionConfig], log: Optional[Log] = None) -> None:
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

        elif log:
            log.logging.info('{} has no rows -- nothing to scramble'.format(jobConfig.table))


def _scrambleJobWorker(readyQueue: mp.Queue, completedQueue: mp.Queue, activeJobs: Dict[str, ScrambleJobConfig],
                        databaseConfiguration: Dict[str, DatabaseConnectionConfig], logDirectory: Path, logLevel: int = logging.INFO) -> None:
    """Runs scramble jobs pulled off readyQueue, via _executeScrambleJob, until the process is torn down."""

    log = Log(logDirectory=logDirectory, level=logLevel)

    while True:

        job = readyQueue.get()
        log.logging.info('Starting {}'.format(job))

        try:
            _executeScrambleJob(job, activeJobs[job], databaseConfiguration, log=log)
            status = JobStatus.COMPLETED
            log.logging.info('Completed {}'.format(job))

        except Exception as error:
            status = JobStatus.FAILED
            log.logging.error('Failed to complete {} due to error {}'.format(job, error), exc_info=error)

        completedQueue.put({job: status})


def runScrambleJobs(jobsFile: ScrambleJobsFile, databaseConfiguration: Dict[str, DatabaseConnectionConfig], logDirectory: Path, runForever: bool = False,
                     logLevel: int = logging.INFO) -> None:
    """Runs scramble jobs, honoring `predecessors`.

    Same division of responsibility as runDataJobs: the caller supplies validated
    configuration, a log location, and whether this is a one-time pass
    (runForever=False, the default -- a masking run is usually one-shot) or should
    keep running (runForever=True). Workers and their pool are managed here.

    jobsFile.cycleSleepSeconds controls how long this sleeps between cycles --
    see runDataJobs's docstring for why that's independent of
    DependencyGraph.run()'s own fixed 1-second poll. logLevel: see runDataJobs.
    """

    log = Log(logDirectory=logDirectory, level=logLevel)
    log.logging.info('Starting scramble job runner with {} worker(s)'.format(jobsFile.workers))

    pool: Optional[Pool] = None

    while True:

        dependencyGraph = DependencyGraph(jobs=jobsFile.jobs)
        log.logging.info('Starting cycle with {} active job(s)'.format(len(dependencyGraph.activeJobs)))

        if pool is not None:
            pool.terminate()
            pool.join()

        pool = mp.Pool(jobsFile.workers, _scrambleJobWorker,
                        (dependencyGraph.readyQueue, dependencyGraph.completedQueue, dependencyGraph.activeJobs, databaseConfiguration, logDirectory, logLevel))
        dependencyGraph.run()

        pool.close()
        log.logging.info('Cycle finished: {} completed, {} failed'.format(len(dependencyGraph.completedJobs), len(dependencyGraph.failedJobs)))

        if not runForever:
            pool.terminate()
            pool.join()
            break

        time.sleep(jobsFile.cycleSleepSeconds)

    log.logging.info('Finished scramble job runner')
