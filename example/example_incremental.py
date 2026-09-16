"""A self-contained, runnable demonstration of streaming + incremental loads.

Unlike a configured deployment, this needs no configuration, no credentials and no
server: it builds a throwaway SQLite database under example/memory/, seeds it,
and runs a real job through runDataJobs twice so you can watch the watermark
move. Run it directly:

    python example/example_incremental.py

The interesting moment is the second run. Between the two, one already-loaded
source row is edited *without* its updatedAt changing, and a new row is added.
A full re-extract would pick both up; an incremental one can only see the new
row, because the predicate now starts past the edited one. Row counts alone
would not show the difference -- upsert is idempotent, so re-reading everything
would produce an identical target.
"""
from __future__ import annotations

import logging
import shutil
import sys
from pathlib import Path

exampleDirectory = Path(__file__).resolve().parent
sys.path.append(str(exampleDirectory.parent))

from lightweight_etl import Configuration, Database, DatabaseConnectionConfig, DatabaseType, DataJobsFile, FileMemory, runDataJobs

workingDirectory = exampleDirectory / 'memory' / 'incremental_demo'
databasePath = workingDirectory / 'demo.db'
memoryPath = workingDirectory / 'memory.yaml'
logPath = workingDirectory / 'incremental.log'

connectionSettings = DatabaseConnectionConfig(type=DatabaseType.SQLITE, database=str(databasePath))

JOB_CONFIGURATION = {
    'workers': 1,
    'jobs': {
        'loadOrders': {
            'active': True,
            'sourceDatabase': 'demo',
            'targetDatabase': 'demo',
            # {{ watermark }} is bound as a parameter, not pasted in as text.
            # A real deployment would subtract a lookback window here -- see
            # "Why the lookback window" in the README.
            'sourceQuery': 'SELECT id, name, updatedAt FROM ordersSource WHERE updatedAt > {{ watermark }} ORDER BY updatedAt',
            'watermarkColumn': 'updatedAt',
            'watermarkInitial': '1970-01-01T00:00:00',
            'targetTableFinal': 'ordersTarget',
            # upsert is required for an incremental job: swap would replace the
            # whole target with only the rows that changed.
            'insertStrategy': 'upsert',
            # Small on purpose, so the 3 seed rows genuinely stream in 2 chunks.
            'chunkSize': 2,
            },
        },
    }


def describe(database: Database, memory: FileMemory, heading: str) -> None:

    rows = database.query('SELECT id, name, updatedAt FROM ordersTarget ORDER BY id')

    print('\n{}'.format(heading))
    print('  stored watermark: {!r}'.format(memory.readWatermarks().get('loadOrders')))
    print('  ordersTarget ({} row(s)):'.format(len(rows)))
    for row in rows:
        print('    {}'.format(row))


def main() -> None:

    shutil.rmtree(workingDirectory, ignore_errors=True)
    workingDirectory.mkdir(parents=True, exist_ok=True)

    memory = FileMemory(memoryFile=memoryPath)
    jobsFile = Configuration.validateJobConfiguration(JOB_CONFIGURATION, DataJobsFile)
    Configuration.validateJobGraph(jobsFile.jobs, databaseAliases={'demo'})

    with Database(connectionSettings=connectionSettings) as database:

        for table in ('ordersSource', 'ordersTarget'):
            database.alter('CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR(50), updatedAt TEXT)'.format(table))

        database.insert(table='ordersSource', data=[
            (1, 'first', '2026-01-01T00:00:00'),
            (2, 'second', '2026-01-02T00:00:00'),
            (3, 'third', '2026-01-03T00:00:00'),
            ], chunkSize=10)

        runDataJobs(jobsFile=jobsFile, databaseConfiguration={'demo': connectionSettings},
                     logFile=logPath, memory=memory, runForever=False, logLevel=logging.DEBUG)
        describe(database, memory, 'RUN 1 -- first run, so it extracts everything from watermarkInitial')

        # Edited, but updatedAt deliberately left alone: an incremental extract
        # cannot see this row again, because the watermark is already past it.
        database.alter("UPDATE ordersSource SET name = 'edited-but-not-retouched' WHERE id = 1")
        database.insert(table='ordersSource', data=[(4, 'fourth', '2026-01-04T00:00:00')], chunkSize=10)

        runDataJobs(jobsFile=jobsFile, databaseConfiguration={'demo': connectionSettings},
                     logFile=logPath, memory=memory, runForever=False, logLevel=logging.DEBUG)
        describe(database, memory, 'RUN 2 -- only row 4 is past the watermark; row 1 keeps its old name')

        runDataJobs(jobsFile=jobsFile, databaseConfiguration={'demo': connectionSettings},
                     logFile=logPath, memory=memory, runForever=False, logLevel=logging.DEBUG)
        describe(database, memory, 'RUN 3 -- nothing new, so nothing loads and the watermark stays put')

    print('\nPer-chunk detail was logged at DEBUG to {}'.format(logPath))


if __name__ == '__main__':
    main()
