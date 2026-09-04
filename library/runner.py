from __future__ import annotations

import multiprocessing as mp
import time
from multiprocessing.pool import Pool
from pathlib import Path
from typing import Dict, List, Optional

from .configurationInterface import DatabaseConnectionConfig, DataJobConfig, DataJobsFile, InsertStrategy, ScrambleJobConfig, ScrambleJobsFile
from .databaseInterface import Database
from .dependencyGraphInterface import DependencyGraph, JobStatus
from .logInterface import Log
from .memoryInterface import Memory
from .scrambleInterface import Scramble
from .transformInterface import Transformer, resolveTransformer, Transform


def _dataJobWorker(readyQueue: mp.Queue, completedQueue: mp.Queue, activeJobs: Dict[str, DataJobConfig],
                    databaseConfiguration: Dict[str, DatabaseConnectionConfig], logDirectory: Path, memoryDirectory: Path) -> None:
    """Runs data jobs pulled off readyQueue until the process is torn down.

    Each columnTransforms entry is a "module.path:function_name" reference, resolved
    via resolveTransformer rather than a fixed lookup table. One source and one target
    Database connection are opened per job and reused for every step, rather than a
    fresh connection per query. The swap branch asserts targetTableStage is set
    because DataJobConfig's validator guarantees that whenever insertStrategy is swap.
    """

    log = Log(logDirectory=logDirectory)
    memory = Memory(memoryDirectory=memoryDirectory)

    while True:

        job = readyQueue.get()
        log.logging.info('Starting {}'.format(job))

        try:
            jobConfig = activeJobs[job]

            columnTransforms: Dict[str, List[Transformer]] = {
                column: [resolveTransformer(reference) for reference in references] for column, references in jobConfig.columnTransforms.items()
                }

            sourceDatabaseConnectionSettings = databaseConfiguration[jobConfig.sourceDatabase]
            targetDatabaseConnectionSettings = databaseConfiguration[jobConfig.targetDatabase]

            with Database(connectionSettings=sourceDatabaseConnectionSettings) as sourceDatabase, \
                 Database(connectionSettings=targetDatabaseConnectionSettings) as targetDatabase:

                data = sourceDatabase.query(query=jobConfig.sourceQuery)

                columns = targetDatabase.getAllColumnNames(table=jobConfig.targetTableFinal)
                transform = Transform(data=data, columns=columns, columnTransforms=columnTransforms)
                data = transform.transform()

                if jobConfig.targetTableStage:
                    targetDatabase.truncate(table=jobConfig.targetTableStage)
                    targetDatabase.insert(table=jobConfig.targetTableStage, data=data, chunkSize=jobConfig.chunkSize)

                for preTargetAdhocQuery in jobConfig.preTargetAdhocQueries:
                    targetDatabase.alter(preTargetAdhocQuery)

                if jobConfig.insertStrategy == InsertStrategy.SWAP:
                    assert jobConfig.targetTableStage is not None
                    targetDatabase.swap(targetTable=jobConfig.targetTableFinal, stageTable=jobConfig.targetTableStage)

                if jobConfig.insertStrategy == InsertStrategy.UPSERT:
                    if jobConfig.targetTableStage:
                        targetDatabase.upsertFromStage(targetTable=jobConfig.targetTableFinal, stageTable=jobConfig.targetTableStage)
                    else:
                        targetDatabase.upsert(table=jobConfig.targetTableFinal, data=data, chunkSize=jobConfig.chunkSize)

                for postTargetAdhocQuery in jobConfig.postTargetAdhocQueries:
                    targetDatabase.alter(postTargetAdhocQuery)

            status = JobStatus.COMPLETED
            log.logging.info('Completed {}'.format(job))

        except Exception as error:
            status = JobStatus.FAILED
            log.logging.error('Failed to complete {} due to error {}'.format(job, error))

        completedQueue.put({job: status})
        memory.recordRun(job=job)


def runDataJobs(jobsFile: DataJobsFile, databaseConfiguration: Dict[str, DatabaseConnectionConfig], logDirectory: Path, memoryDirectory: Path,
                 runForever: bool = True) -> None:
    """Runs data jobs, honoring each job's `refresh` window and `predecessors`.

    The caller's only responsibility is configuration: the validated jobs/database
    config, where logs and run-history (memory) should live, and whether this is a
    one-time pass (runForever=False) or should keep running (runForever=True, the
    default -- `refresh` only means anything in a long-running process). Worker
    processes and their pool are managed entirely here: each cycle's pool is
    terminated and joined before the next one starts, rather than left running, so
    workers don't pile up as OS processes across cycles.
    """

    log = Log(logDirectory=logDirectory)
    log.logging.info('Starting data job runner')

    pool: Optional[Pool] = None

    while True:

        memory = Memory(memoryDirectory=memoryDirectory)
        dependencyGraph = DependencyGraph(jobs=jobsFile.jobs, memory=memory.memory)

        if pool is not None:
            pool.terminate()
            pool.join()

        pool = mp.Pool(jobsFile.workers, _dataJobWorker,
                        (dependencyGraph.readyQueue, dependencyGraph.completedQueue, dependencyGraph.activeJobs,
                         databaseConfiguration, logDirectory, memoryDirectory))
        dependencyGraph.run()

        pool.close()

        if not runForever:
            pool.terminate()
            pool.join()
            break

        time.sleep(.5)

    log.logging.info('Finished data job runner')


def _scrambleJobWorker(readyQueue: mp.Queue, completedQueue: mp.Queue, activeJobs: Dict[str, ScrambleJobConfig],
                        databaseConfiguration: Dict[str, DatabaseConnectionConfig], logDirectory: Path) -> None:
    """Runs scramble jobs pulled off readyQueue until the process is torn down.

    One Database connection is opened per job and reused for every step, rather
    than a fresh connection per query.
    """

    log = Log(logDirectory=logDirectory)

    while True:

        job = readyQueue.get()
        log.logging.info('Starting {}'.format(job))

        try:
            jobConfig = activeJobs[job]
            connectionSettings = databaseConfiguration[jobConfig.database]

            with Database(connectionSettings=connectionSettings) as database:

                for preTargetAdhocQuery in jobConfig.preTargetAdhocQueries:
                    database.alter(preTargetAdhocQuery)

                dataQuery = 'select * from {}'.format(jobConfig.table)
                data = database.query(query=dataQuery)
                columns = database.getAllColumnNames(table=jobConfig.table)
                dataTypes = database.getAllColumnTypes(table=jobConfig.table)

                if data:

                    scramble = Scramble(job=job, data=data, columns=columns, dataTypes=dataTypes, defaultColumnValues=jobConfig.defaultColumnValues,
                                         identifierColumns=jobConfig.identifierColumns, scrambleColumns=jobConfig.scrambleColumns, randomColumns=jobConfig.randomColumns,
                                         allDataRandom=jobConfig.allDataRandom, randomSalt=jobConfig.randomSalt)
                    scramble.scramble()

                    database.truncate(jobConfig.table)
                    database.insert(table=jobConfig.table, data=scramble.dataScrambled, chunkSize=5000)

                    for postTargetAdhocQuery in jobConfig.postTargetAdhocQueries:
                        database.alter(postTargetAdhocQuery)

            status = JobStatus.COMPLETED
            log.logging.info('Completed {}'.format(job))

        except Exception as error:
            status = JobStatus.FAILED
            log.logging.error('Failed to complete {} due to error {}'.format(job, error))

        completedQueue.put({job: status})


def runScrambleJobs(jobsFile: ScrambleJobsFile, databaseConfiguration: Dict[str, DatabaseConnectionConfig], logDirectory: Path, runForever: bool = False) -> None:
    """Runs scramble jobs, honoring `predecessors`.

    Same division of responsibility as runDataJobs: the caller supplies validated
    configuration, a log location, and whether this is a one-time pass
    (runForever=False, the default -- a masking run is usually one-shot) or should
    keep running (runForever=True). Workers and their pool are managed here.
    """

    log = Log(logDirectory=logDirectory)
    log.logging.info('Starting scramble job runner')

    pool: Optional[Pool] = None

    while True:

        dependencyGraph = DependencyGraph(jobs=jobsFile.jobs)

        if pool is not None:
            pool.terminate()
            pool.join()

        pool = mp.Pool(jobsFile.workers, _scrambleJobWorker, (dependencyGraph.readyQueue, dependencyGraph.completedQueue, dependencyGraph.activeJobs, databaseConfiguration, logDirectory))
        dependencyGraph.run()

        pool.close()

        if not runForever:
            pool.terminate()
            pool.join()
            break

        time.sleep(.5)

    log.logging.info('Finished scramble job runner')
