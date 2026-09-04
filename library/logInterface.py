from __future__ import annotations

import logging
from pathlib import Path


class Log:

    def __init__(self, logDirectory: Path, level: int = logging.INFO) -> None:
        """level defaults to INFO (job start/completion, row counts, major steps);
        pass logging.DEBUG for the finer-grained detail runner.py and
        databaseScrambleInterface.py also emit (resolved columns, adhoc query text,
        per-step SQL) without changing anything at the call sites.
        """
        format = '%(asctime)s.%(msecs)03d [%(levelname)s] :: %(message)s [%(filename)s:%(lineno)d]'
        datefmt = '%Y-%m-%d %H:%M:%S'
        formatter = logging.Formatter(fmt=format, datefmt=datefmt)
        self.logging = logging.getLogger()
        self.logging.setLevel(level)
        fh = logging.FileHandler(filename=str(logDirectory))
        fh.setLevel(level)
        fh.setFormatter(formatter)
        self.logging.addHandler(fh)
