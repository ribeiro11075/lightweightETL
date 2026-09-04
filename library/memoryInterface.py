from __future__ import annotations

import os
import sys
import time
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


class Memory:
    """Tracks each job's last-run time, in a file safely shared across worker processes.

    Each worker process holds its own Memory instance. Without locking, two workers
    finishing jobs around the same time would each write back their own stale
    in-memory snapshot (loaded once at __init__) and silently clobber each other's
    update -- recordRun re-reads and re-locks the file on every call instead of
    trusting that snapshot, closing that race.
    """

    def __init__(self, memoryDirectory: Path) -> None:
        self.memoryDirectory = memoryDirectory
        self.memory: Dict[str, float] = self._read()


    def _read(self) -> Dict[str, float]:
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
                self.memory = yaml.load(file, Loader=yaml.FullLoader) or {}
                self.memory[job] = time.time()

                file.seek(0)
                file.truncate()
                yaml.dump(self.memory, file)
            finally:
                _unlock(file)
