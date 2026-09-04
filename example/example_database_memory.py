"""A reference MemoryBackend that stores run history in a database table instead
of a file -- see library/memoryInterface.py's MemoryBackend for the contract this
must satisfy, and library/runner.py for how it's used.

Expects a table already created with this shape (adjust types to your database):

    CREATE TABLE lightweight_etl_memory (
        job VARCHAR(255) PRIMARY KEY,
        last_run DOUBLE NOT NULL
        )

recordRun doesn't create this table itself -- nothing else in this library issues
DDL on your behalf that a job config didn't explicitly ask for, and this follows
that same rule. The database's own UPSERT atomicity is what makes this safe across
concurrent worker processes; unlike FileMemory, there's no locking to write here.
"""
from __future__ import annotations

import time
from typing import Dict

from library import Database, DatabaseConnectionConfig, MemoryBackend


class DatabaseMemory(MemoryBackend):

    def __init__(self, connectionSettings: DatabaseConnectionConfig, table: str = 'lightweight_etl_memory') -> None:
        self.connectionSettings = connectionSettings
        self.table = table


    def read(self) -> Dict[str, float]:

        with Database(connectionSettings=self.connectionSettings) as database:
            rows = database.query('SELECT job, last_run FROM {}'.format(self.table))
            return {job: lastRun for job, lastRun in rows}


    def recordRun(self, job: str) -> None:

        with Database(connectionSettings=self.connectionSettings) as database:
            database.upsert(table=self.table, data=[(job, time.time())])
