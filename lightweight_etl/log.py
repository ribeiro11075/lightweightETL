from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

LOGGER_NAME = 'lightweight_etl'


class Log:

    def __init__(self, logFile: Optional[Path] = None, level: int = logging.INFO) -> None:
        """level defaults to INFO (job start/completion, row counts, major steps);
        pass logging.DEBUG for the finer-grained detail runner.py and
        scramble.py also emit (resolved columns, adhoc query text,
        per-step SQL) without changing anything at the call sites.

        Two things this deliberately does not do, both of which it used to:

        (1) It doesn't configure the root logger. `logging.getLogger()` with no
        name *is* the root logger, so every handler added here was attached to the
        logger every other library in the process also logs through -- the host
        application's own log records were silently duplicated into this file, and
        a caller who had configured logging themselves had their setup quietly
        extended. A named logger with propagate=False keeps this package's records
        in this package's file, and everyone else's out of it.

        (2) It doesn't stack a second handler for a file it's already writing to.
        Every instantiation used to add another FileHandler, so N instances meant
        every record written N times: runDataJobs builds one Log and each worker
        process builds its own, which under a `fork` start method inherits the
        parent's handler and then added a duplicate of it. Handlers are matched on
        the resolved path of the file they write, so re-instantiating for the same
        destination reuses the open handler (and just updates its level) instead.

        logFile is optional so a caller that wants records on a stream
        instead -- the CLI, where logs have to reach stdout/stderr to be
        collected in a container at all -- can configure the logger without
        first naming a file it doesn't want.
        """

        format = '%(asctime)s.%(msecs)03d [%(levelname)s] :: %(message)s [%(filename)s:%(lineno)d]'
        datefmt = '%Y-%m-%d %H:%M:%S'
        formatter = logging.Formatter(fmt=format, datefmt=datefmt)

        self.logging = logging.getLogger(LOGGER_NAME)
        self.logging.setLevel(level)
        self.logging.propagate = False
        self._formatter = formatter

        if logFile is None:
            return

        path = Path(logFile).resolve()
        existing = self._findHandler(path)

        if existing is not None:
            existing.setLevel(level)
            return

        handler = logging.FileHandler(filename=str(path))
        handler.setLevel(level)
        handler.setFormatter(formatter)
        self.logging.addHandler(handler)


    def addStreamHandler(self, stream: Any, level: int = logging.INFO) -> None:
        """Send records to a stream as well as (or instead of) a file.

        Deduplicated on the stream object for the same reason file handlers are
        deduplicated on their path: this logger is process-wide, so a second
        call for the same destination would double every record.
        """

        for handler in self.logging.handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler) and handler.stream is stream:
                handler.setLevel(level)
                return

        handler = logging.StreamHandler(stream=stream)
        handler.setLevel(level)
        handler.setFormatter(self._formatter)
        self.logging.addHandler(handler)


    def _findHandler(self, path: Path) -> Optional[logging.FileHandler]:
        """The handler already writing to `path`, if this logger has one.

        FileHandler.baseFilename is set by FileHandler itself via
        os.path.abspath, so it's already absolute -- resolve() on both sides
        makes the comparison agree through symlinks too.
        """

        for handler in self.logging.handlers:
            if isinstance(handler, logging.FileHandler) and Path(handler.baseFilename).resolve() == path:
                return handler

        return None
