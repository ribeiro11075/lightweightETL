"""A runnable demonstration of streaming and incremental loads.

    python example/incremental_demo.py

Needs no server and no credentials. It loads its job from
example/configuration/demo/ through the same path the CLI uses -- YAML, then
${NAME} expansion, then validation -- builds a throwaway SQLite database, and
runs the job three times so you can watch the watermark move.

The interesting moment is the second run. Between runs one already-loaded
source row is edited *without* its updatedAt changing, and a new row is added.
A full re-extract would pick both up; an incremental one can only see the new
row, because the predicate now starts past the edited one. Row counts alone
would show nothing, since upsert is idempotent -- the edited row is the tell.
"""
from __future__ import annotations

import logging
import os
import shutil
import sys
from pathlib import Path
from typing import Any, List, Optional

import yaml

exampleDirectory = Path(__file__).resolve().parent
sys.path.append(str(exampleDirectory.parent))

from lightweight_etl import Configuration, Database, DataJobsFile, FileMemory, expandEnvironmentVariables, runDataJobs

DEFAULT_WORKING_DIRECTORY = exampleDirectory / 'memory' / 'incremental_demo'

DEMO_CONFIGURATION_DIRECTORY = exampleDirectory / 'configuration' / 'demo'


def loadConfiguration(name: str) -> Any:
    """Loads a demo YAML file the same way the CLI does, ${NAME} expansion included."""

    with open(DEMO_CONFIGURATION_DIRECTORY / name) as file:
        return expandEnvironmentVariables(yaml.load(file, Loader=yaml.FullLoader))


def describe(database: Database, memory: FileMemory, heading: str) -> Optional[str]:
    """Prints what the run did, and returns the stored watermark so a caller
    (the test that keeps this script from rotting) can assert on it.
    """

    rows = database.query('SELECT id, name, updatedAt FROM ordersTarget ORDER BY id')
    watermark = memory.readWatermarks().get('loadOrders')

    print('\n{}'.format(heading))
    print('  stored watermark: {!r}'.format(watermark))
    print('  ordersTarget ({} row(s)):'.format(len(rows)))
    for row in rows:
        print('    {}'.format(row))

    return watermark


def main(workingDirectory: Path = DEFAULT_WORKING_DIRECTORY) -> List[Optional[str]]:
    """Runs the demonstration, returning the stored watermark after each pass.

    workingDirectory is a parameter so the test that exercises this can point it
    at a temporary directory instead of writing into the source tree.
    """

    memoryPath = workingDirectory / 'memory.yaml'
    logPath = workingDirectory / 'incremental.log'

    shutil.rmtree(workingDirectory, ignore_errors=True)
    workingDirectory.mkdir(parents=True, exist_ok=True)

    # database.yaml reads its path from the environment, exactly the mechanism a
    # real deployment uses for credentials.
    os.environ['DEMO_DB_PATH'] = str(workingDirectory / 'demo.db')

    databaseConfiguration = Configuration.validateDatabaseConfiguration(loadConfiguration('database.yaml'))
    jobsFile = Configuration.validateJobConfiguration(loadConfiguration('jobs.yaml'), DataJobsFile)
    Configuration.validateJobGraph(jobsFile.jobs, databaseAliases=set(databaseConfiguration))

    watermarks: List[Optional[str]] = []
    memory = FileMemory(memoryFile=memoryPath)

    with Database(connectionSettings=databaseConfiguration['demo']) as database:

        for table in ('ordersSource', 'ordersTarget'):
            database.alter('CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR(50), updatedAt TEXT)'.format(table))

        database.insert(table='ordersSource', data=[
            (1, 'first', '2026-01-01T00:00:00'),
            (2, 'second', '2026-01-02T00:00:00'),
            (3, 'third', '2026-01-03T00:00:00'),
            ], chunkSize=10)

        runDataJobs(jobsFile=jobsFile, databaseConfiguration=databaseConfiguration,
                     logFile=logPath, memory=memory, logLevel=logging.DEBUG)
        watermarks.append(describe(database, memory, 'RUN 1 -- first run, so it extracts everything from watermarkInitial'))

        # Edited, but updatedAt deliberately left alone: an incremental extract
        # cannot see this row again, because the watermark is already past it.
        database.alter("UPDATE ordersSource SET name = 'edited-but-not-retouched' WHERE id = 1")
        database.insert(table='ordersSource', data=[(4, 'fourth', '2026-01-04T00:00:00')], chunkSize=10)

        runDataJobs(jobsFile=jobsFile, databaseConfiguration=databaseConfiguration,
                     logFile=logPath, memory=memory, logLevel=logging.DEBUG)
        watermarks.append(describe(database, memory, 'RUN 2 -- only row 4 is past the watermark; row 1 keeps its old name'))

        runDataJobs(jobsFile=jobsFile, databaseConfiguration=databaseConfiguration,
                     logFile=logPath, memory=memory, logLevel=logging.DEBUG)
        watermarks.append(describe(database, memory, 'RUN 3 -- nothing new, so nothing loads and the watermark stays put'))

    print('\nPer-chunk detail was logged at DEBUG to {}'.format(logPath))

    return watermarks


if __name__ == '__main__':
    main()
