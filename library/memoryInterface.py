from __future__ import annotations

import os
import sys
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict

import yaml

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


class FileMemory(MemoryBackend):
    """The default MemoryBackend: a YAML file, safe to share across worker processes.

    recordRun re-reads and re-locks the file on every call rather than trusting a
    cached snapshot, since multiple worker processes each hold their own FileMemory
    instance -- without this, two workers finishing around the same time would each
    write back a stale copy of the whole file, silently losing each other's update.
    """

    def __init__(self, memoryDirectory: Path) -> None:
        self.memoryDirectory = memoryDirectory


    def read(self) -> Dict[str, float]:
        """A missing memoryDirectory means no run has been recorded yet and reads as
        empty, rather than raising -- a fresh checkout has no memory file at all.
        """

        try:
            with open(self.memoryDirectory) as file:
                _lock(file)
                try:
                    return yaml.load(file, Loader=yaml.FullLoader) or {}
                finally:
                    _unlock(file)
        except FileNotFoundError:
            return {}


    def recordRun(self, job: str) -> None:

        fileDescriptor = os.open(self.memoryDirectory, os.O_RDWR | os.O_CREAT, 0o644)

        with os.fdopen(fileDescriptor, 'r+') as file:
            _lock(file)
            try:
                file.seek(0)
                memory = yaml.load(file, Loader=yaml.FullLoader) or {}
                memory[job] = time.time()

                file.seek(0)
                file.truncate()
                yaml.dump(memory, file)
            finally:
                _unlock(file)
