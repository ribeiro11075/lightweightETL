from __future__ import annotations

import datetime
import decimal
import os
import sys
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Tuple

import yaml

from .configuration import DatabaseConnectionConfig
from .database import Database

if sys.platform == 'win32':
    import msvcrt

    def _lock(file: Any) -> None:
        file.seek(0)
        msvcrt.locking(file.fileno(), msvcrt.LK_LOCK, 1)

    def _unlock(file: Any) -> None:
        file.seek(0)
        msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, 1)
else:
    import fcntl

    def _lock(file: Any) -> None:
        fcntl.flock(file.fileno(), fcntl.LOCK_EX)

    def _unlock(file: Any) -> None:
        fcntl.flock(file.fileno(), fcntl.LOCK_UN)


class MemoryBackend(ABC):
    """Tracks each job's last-run time, wherever an implementation chooses to keep it.

    Implementations must be safe to pass through multiprocessing.Pool's initargs --
    runDataJobs hands the same instance to every worker process, which pickles it
    and reconstructs a separate copy per process. Concretely: hold picklable
    settings (a Path, connection settings, ...), not a live file handle or database
    connection, and open whatever resource you need inside read()/recordRun()
    itself -- the same contract Database's dialects follow for the ETL databases.
    """

    @abstractmethod
    def read(self) -> Dict[str, float]:
        """Every job's last-run time, keyed by job name."""

    @abstractmethod
    def recordRun(self, job: str) -> None:
        """Records that `job` just ran, now."""

    def readWatermarks(self) -> Dict[str, Any]:
        """Every job's stored watermark, keyed by job name.

        Not abstract, so a backend written before watermarks existed keeps
        working for every job that doesn't use one. Returning nothing here means
        an incremental job falls back to its watermarkInitial on every run, which
        is safe (it re-reads from the beginning and upserts) but not incremental
        -- so recordWatermark raises rather than letting that pass silently.
        """

        return {}

    def recordWatermark(self, job: str, value: Any) -> None:
        """Records the high-water mark `job` reached, for its next run to resume from.

        The default raises: a backend that cannot persist this cannot run
        incremental jobs correctly, and failing loudly beats a job that silently
        re-extracts its whole source forever. runDataJobs checks for this up
        front, before starting any work.
        """

        raise NotImplementedError(
            '{} does not implement recordWatermark, so it cannot persist a watermark for "{}" -- '
            'implement readWatermarks/recordWatermark on it, or use FileMemory'.format(type(self).__name__, job))


def _yamlSafe(value: Any) -> Any:
    """Coerce a driver's value into something yaml.dump/FullLoader round-trips.

    Dates, ints, floats and strings all survive as themselves. Decimal is the
    one that doesn't -- Oracle hands back every NUMBER as a Decimal, so an
    integer id column watermarks as Decimal('4711'), which PyYAML can only
    write as a python/object tag that FullLoader then refuses to load. An
    integral Decimal becomes an int (exact, no precision lost, which matters for
    ids beyond float's 53-bit range); a fractional one becomes a float.
    """

    if isinstance(value, decimal.Decimal):
        return int(value) if value == value.to_integral_value() else float(value)

    return value


class FileMemory(MemoryBackend):
    """The default MemoryBackend: a YAML file, safe to share across worker processes.

    Every write re-reads and re-locks the file rather than trusting a cached
    snapshot, since multiple worker processes each hold their own FileMemory
    instance -- without this, two workers finishing around the same time would
    each write back a stale copy of the whole file, silently losing each other's
    update.

    The document is namespaced:

        lastRun:
          loadOrders: 1726400000.0
        watermarks:
          loadOrders: 2026-09-15 10:00:00

    A file written before watermarks existed is a bare job -> timestamp mapping
    with neither key, and is read as lastRun so an existing deployment's refresh
    windows survive the upgrade rather than every job firing at once.
    """

    def __init__(self, memoryFile: Path) -> None:
        self.memoryFile = memoryFile


    def _load(self, file: Any) -> Dict[str, Any]:

        document = yaml.load(file, Loader=yaml.FullLoader) or {}

        if document and 'lastRun' not in document and 'watermarks' not in document:
            return {'lastRun': document, 'watermarks': {}}

        document.setdefault('lastRun', {})
        document.setdefault('watermarks', {})

        return document


    def _read(self, section: str) -> Dict[str, Any]:
        """A missing file means nothing has been recorded yet and reads as empty,
        rather than raising -- a fresh checkout has no memory file at all.
        """

        try:
            with open(self.memoryFile) as file:
                _lock(file)
                try:
                    return self._load(file)[section]
                finally:
                    _unlock(file)
        except FileNotFoundError:
            return {}


    def _write(self, section: str, job: str, value: Any) -> None:

        fileDescriptor = os.open(self.memoryFile, os.O_RDWR | os.O_CREAT, 0o644)

        with os.fdopen(fileDescriptor, 'r+') as file:
            _lock(file)
            try:
                file.seek(0)
                document = self._load(file)
                document[section][job] = value

                file.seek(0)
                file.truncate()
                yaml.dump(document, file)
            finally:
                _unlock(file)


    def read(self) -> Dict[str, float]:

        return self._read('lastRun')


    def recordRun(self, job: str) -> None:

        self._write('lastRun', job, time.time())


    def readWatermarks(self) -> Dict[str, Any]:

        return self._read('watermarks')


    def recordWatermark(self, job: str, value: Any) -> None:

        self._write('watermarks', job, _yamlSafe(value))


DATABASE_MEMORY_SCHEMA = """CREATE TABLE lightweight_etl_memory (
    job VARCHAR(255) PRIMARY KEY,
    last_run DOUBLE,
    watermark_value VARCHAR(255),
    watermark_type VARCHAR(32)
    )"""


class DatabaseMemory(MemoryBackend):
    """A MemoryBackend that keeps run state in a database table rather than a file.

    Use this wherever FileMemory's assumption -- a filesystem that persists
    between runs, shared by every worker -- doesn't hold. That covers more
    deployments than it sounds: a container with no volume, anything horizontally
    scaled across machines, and serverless in particular, where /tmp is scoped to
    one execution environment and vanishes on a cold start. FileMemory there
    doesn't fail loudly; it silently forgets every watermark and re-extracts from
    watermarkInitial, which is the exact failure incremental loads exist to avoid.

    The table must already exist -- see DATABASE_MEMORY_SCHEMA for the shape, and
    adjust the types to your database. Nothing in this package issues DDL a job
    config didn't ask for, and this follows that rule. The database's own UPSERT
    atomicity is what makes it safe across concurrent workers; unlike FileMemory
    there is no locking to do here.

    Every write names its columns explicitly rather than letting Database
    introspect them, which is what keeps recordRun and recordWatermark from
    clobbering each other: an upsert limited to (job, last_run) updates only
    last_run and leaves the watermark columns alone, and vice versa. It's also
    why last_run is nullable -- a worker records a watermark before it records
    the run, so the first write for a new job inserts a row with no last_run yet.

    Storing a watermark is the part a database-backed backend has to solve that
    FileMemory gets for free: YAML round-trips a datetime or an int as itself,
    while a SQL column has one type and a watermark may be a timestamp, an id or
    a string depending on the job. A type tag is kept alongside the text and the
    original rebuilt on the way out, so what gets bound into the next run's
    predicate is the same type the source column is compared against.
    """

    def __init__(self, connectionSettings: DatabaseConnectionConfig, table: str = 'lightweight_etl_memory') -> None:
        self.connectionSettings = connectionSettings
        self.table = table


    @staticmethod
    def _encodeWatermark(value: Any) -> Tuple[str, str]:

        if isinstance(value, datetime.datetime):
            return value.isoformat(), 'datetime'
        if isinstance(value, datetime.date):
            return value.isoformat(), 'date'
        if isinstance(value, int) and not isinstance(value, bool):
            return str(value), 'int'
        if isinstance(value, float):
            return repr(value), 'float'

        return str(value), 'str'


    @staticmethod
    def _decodeWatermark(text: str, typeTag: str) -> Any:

        if typeTag == 'datetime':
            return datetime.datetime.fromisoformat(text)
        if typeTag == 'date':
            return datetime.date.fromisoformat(text)
        if typeTag == 'int':
            return int(text)
        if typeTag == 'float':
            return float(text)

        return text


    def read(self) -> Dict[str, float]:
        """Rows whose last_run is still NULL are skipped -- a job can have a
        watermark recorded before it has ever recorded a completed run, and
        DependencyGraph expects a number it can subtract from time.time().
        """

        with Database(connectionSettings=self.connectionSettings) as database:
            rows = database.query('SELECT job, last_run FROM {}'.format(self.table))

            return {job: lastRun for job, lastRun in rows if lastRun is not None}


    def recordRun(self, job: str) -> None:

        with Database(connectionSettings=self.connectionSettings) as database:
            database.upsert(table=self.table, data=[(job, time.time())], columns=['job', 'last_run'])


    def readWatermarks(self) -> Dict[str, Any]:

        with Database(connectionSettings=self.connectionSettings) as database:
            rows = database.query('SELECT job, watermark_value, watermark_type FROM {}'.format(self.table))

            return {job: self._decodeWatermark(value, typeTag) for job, value, typeTag in rows if value is not None}


    def recordWatermark(self, job: str, value: Any) -> None:

        text, typeTag = self._encodeWatermark(value)

        with Database(connectionSettings=self.connectionSettings) as database:
            database.upsert(table=self.table, data=[(job, text, typeTag)], columns=['job', 'watermark_value', 'watermark_type'])
