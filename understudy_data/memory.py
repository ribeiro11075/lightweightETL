from __future__ import annotations

import contextlib
import datetime
import decimal
import os
import sys
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Tuple, Union

import yaml

from .configuration import DatabaseConnectionConfig
from .database import Database

# Locks are taken on a separate, empty `.lock` file rather than on the data
# file itself, because the data file is replaced on every write: a lock held on
# the old inode would guard nothing once the new one is renamed into place.
if sys.platform == 'win32':
    import msvcrt

    def _lock(file: Any, blocking: bool = True) -> None:
        file.seek(0)
        msvcrt.locking(file.fileno(), msvcrt.LK_LOCK if blocking else msvcrt.LK_NBLCK, 1)

    def _unlock(file: Any) -> None:
        file.seek(0)
        msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, 1)
else:
    import fcntl

    def _lock(file: Any, blocking: bool = True) -> None:
        fcntl.flock(file.fileno(), fcntl.LOCK_EX if blocking else fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock(file: Any) -> None:
        fcntl.flock(file.fileno(), fcntl.LOCK_UN)


@contextlib.contextmanager
def exclusiveLock(path: Path, blocking: bool = True) -> Iterator[None]:
    """Holds an exclusive lock on `path`, creating it if needed. Raises
    OSError at once if `blocking` is False and someone else holds it.
    """

    with open(path, 'a') as file:
        _lock(file, blocking=blocking)
        try:
            yield
        finally:
            _unlock(file)


class RunInProgressError(Exception):
    """Another run already holds the run lock."""


@contextlib.contextmanager
def exclusiveRun(lockFile: Union[str, Path]) -> Iterator[None]:
    """Holds `lockFile` for the life of a run, or raises RunInProgressError.

    Two runs sharing run state must not overlap -- a cron interval shorter than
    a slow run is enough to cause it. Both would run the same jobs at once:
    two swaps renaming the same tables, two upserts racing, and each recording
    watermarks the other then moves. The second run refuses to start instead.
    The lock is released by the operating system if the process dies.
    """

    try:
        with exclusiveLock(Path(lockFile), blocking=False):
            yield
    except OSError as error:
        raise RunInProgressError('another run is already using {} -- not starting a second one alongside it'.format(lockFile)) from error


class MemoryBackend(ABC):
    """Tracks each job's last-run time, wherever an implementation chooses to keep it.

    Implementations must be picklable -- runDataJobs hands the same instance to
    every job it runs in a worker process, which pickles it and reconstructs a
    separate copy there. Concretely: hold picklable settings (a Path, connection
    settings, ...), not a live file handle or database connection, and open
    whatever resource you need inside read()/recordRun() itself -- the same
    contract Database's dialects follow for the ETL databases.
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

    def readKeyFingerprints(self) -> Dict[str, str]:
        """The masking-key fingerprint each masked job last completed under.

        Not abstract: a backend without it simply never detects a changed key.
        """

        return {}

    def recordKeyFingerprint(self, job: str, fingerprint: Optional[str]) -> None:
        """Records the fingerprint `job` completed under; None forgets it."""


def _yamlSafe(value: Any) -> Any:
    """Coerce a driver's value into something yaml.safe_dump/safe_load round-trips.

    Dates, ints, floats and strings all survive as themselves. Decimal is the
    one that doesn't -- Oracle hands back every NUMBER as a Decimal, so an
    integer id column watermarks as Decimal('4711'), which safe_dump refuses.
    An integral Decimal becomes an int (exact, no precision lost, which matters
    for ids beyond float's 53-bit range); a fractional one becomes a float.
    """

    if isinstance(value, decimal.Decimal):
        return int(value) if value == value.to_integral_value() else float(value)

    return value


SECTIONS = ('lastRun', 'watermarks', 'maskingKeys')


class FileMemory(MemoryBackend):
    """The default MemoryBackend: a YAML file, safe to share across worker processes.

    Every write re-reads the file under a lock rather than trusting a cached
    snapshot, since multiple worker processes each hold their own FileMemory
    instance -- without this, two workers finishing around the same time would
    each write back a stale copy of the whole file, silently losing each other's
    update.

    A write goes to a temporary file that then replaces the original, so a
    process killed mid-write leaves the previous version intact rather than a
    truncated file that no later run could parse. The lock lives beside it, in
    `<file>.lock`.

    The document is namespaced:

        lastRun:
          loadOrders: 1726400000.0
        watermarks:
          loadOrders: 2026-09-15 10:00:00
        maskingKeys:
          maskCustomers: d5930cf83dea

    A file written before watermarks existed is a bare job -> timestamp mapping
    with neither key, and is read as lastRun so an existing deployment's refresh
    windows survive the upgrade rather than every job firing at once.
    """

    def __init__(self, memoryFile: Union[str, Path]) -> None:
        self.memoryFile = Path(memoryFile)
        self._lockFile = self.memoryFile.with_name(self.memoryFile.name + '.lock')


    def _load(self) -> Dict[str, Any]:
        """A missing file means nothing has been recorded yet and reads as
        empty, rather than raising -- a fresh checkout has no memory file at all.
        """

        try:
            with open(self.memoryFile) as file:
                document = yaml.safe_load(file) or {}
        except FileNotFoundError:
            document = {}

        if document and 'lastRun' not in document and 'watermarks' not in document:
            return {'lastRun': document, 'watermarks': {}}

        for section in SECTIONS:
            document.setdefault(section, {})

        return document


    def _read(self, section: str) -> Dict[str, Any]:

        # Checked first so that reading never creates a lock file beside a
        # memory file that doesn't exist yet.
        if not self.memoryFile.exists():
            return {}

        with exclusiveLock(self._lockFile):
            return dict(self._load()[section])


    def _write(self, section: str, job: str, value: Any) -> None:

        temporary = self.memoryFile.with_name(self.memoryFile.name + '.tmp')

        with exclusiveLock(self._lockFile):
            document = self._load()
            if value is None:
                document[section].pop(job, None)
            else:
                document[section][job] = value

            with open(temporary, 'w') as file:
                # safe_dump refuses a type safe_load couldn't read back, so an
                # unsupported watermark fails here rather than corrupting the file.
                yaml.safe_dump(document, file)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary, self.memoryFile)


    def read(self) -> Dict[str, float]:

        return self._read('lastRun')


    def recordRun(self, job: str) -> None:

        self._write('lastRun', job, time.time())


    def readWatermarks(self) -> Dict[str, Any]:

        return self._read('watermarks')


    def recordWatermark(self, job: str, value: Any) -> None:

        self._write('watermarks', job, _yamlSafe(value))


    def readKeyFingerprints(self) -> Dict[str, str]:

        return self._read('maskingKeys')


    def recordKeyFingerprint(self, job: str, fingerprint: Optional[str]) -> None:

        self._write('maskingKeys', job, fingerprint)


DATABASE_MEMORY_SCHEMA = """CREATE TABLE understudy_memory (
    job VARCHAR(255) PRIMARY KEY,
    last_run DOUBLE PRECISION,
    watermark_value VARCHAR(255),
    watermark_type VARCHAR(32)
    )"""


KEY_FINGERPRINT_SUFFIX = '#maskingKey'
KEY_FINGERPRINT_TYPE = 'maskingKey'


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

    def __init__(self, connectionSettings: DatabaseConnectionConfig, table: str = 'understudy_memory') -> None:
        self.connectionSettings = connectionSettings
        self.table = table


    @staticmethod
    def _encodeWatermark(value: Any) -> Tuple[str, str]:

        if isinstance(value, datetime.datetime):
            return value.isoformat(), 'datetime'
        if isinstance(value, datetime.date):
            return value.isoformat(), 'date'
        if isinstance(value, datetime.time):
            return value.isoformat(), 'time'
        if isinstance(value, int) and not isinstance(value, bool):
            return str(value), 'int'
        if isinstance(value, float):
            return repr(value), 'float'
        if isinstance(value, decimal.Decimal):
            return str(value), 'decimal'
        # SQL Server's rowversion, the usual way to track changes there.
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value).hex(), 'bytes'

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
        if typeTag == 'time':
            return datetime.time.fromisoformat(text)
        if typeTag == 'decimal':
            return decimal.Decimal(text)
        if typeTag == 'bytes':
            return bytes.fromhex(text)

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

            return {job: self._decodeWatermark(value, typeTag) for job, value, typeTag in rows
                    if value is not None and typeTag != KEY_FINGERPRINT_TYPE}


    def readKeyFingerprints(self) -> Dict[str, str]:
        """Kept in rows of their own, named `<job>#maskingKey`, so the table
        needs no new column: their type tag keeps them out of readWatermarks,
        and their NULL last_run out of read().
        """

        with Database(connectionSettings=self.connectionSettings) as database:
            rows = database.query("SELECT job, watermark_value FROM {} WHERE watermark_type = '{}'".format(self.table, KEY_FINGERPRINT_TYPE))

            return {job[:-len(KEY_FINGERPRINT_SUFFIX)]: value for job, value in rows if job.endswith(KEY_FINGERPRINT_SUFFIX) and value}


    def recordKeyFingerprint(self, job: str, fingerprint: Optional[str]) -> None:

        with Database(connectionSettings=self.connectionSettings) as database:
            database.upsert(table=self.table, data=[(job + KEY_FINGERPRINT_SUFFIX, fingerprint, KEY_FINGERPRINT_TYPE)],
                            columns=['job', 'watermark_value', 'watermark_type'])


    def recordWatermark(self, job: str, value: Any) -> None:

        text, typeTag = self._encodeWatermark(value)

        with Database(connectionSettings=self.connectionSettings) as database:
            database.upsert(table=self.table, data=[(job, text, typeTag)], columns=['job', 'watermark_value', 'watermark_type'])
