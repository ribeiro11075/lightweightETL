from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

LOGGER_NAME = 'lightweight_etl'

TEXT_FORMAT = '%(asctime)s.%(msecs)03d [%(levelname)s] :: %(message)s [%(filename)s:%(lineno)d]'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# Everything LogRecord sets on itself. Anything *not* in here arrived through a
# log call's extra={...} and is a field the caller wanted in the output.
_RESERVED_RECORD_FIELDS = frozenset({
    'args', 'asctime', 'created', 'exc_info', 'exc_text', 'filename', 'funcName', 'levelname', 'levelno', 'lineno',
    'message', 'module', 'msecs', 'msg', 'name', 'pathname', 'process', 'processName', 'relativeCreated', 'stack_info',
    'taskName', 'thread', 'threadName',
    })


class JsonFormatter(logging.Formatter):
    """One JSON object per line, for a log collector rather than a person.

    The point isn't the encoding, it's the fields: job runs emit `job`,
    `status`, `rowCount` and `durationSeconds` through extra={...}, so a
    collector can alert on `status="failed"` or chart rows moved per job without
    anyone parsing a message string. Text output stays the default, because a
    human reading a terminal is the more common case.

    Anything unserializable falls back to str() rather than failing -- losing a
    log line to a TypeError while reporting an error is a poor trade.
    """

    def format(self, record: logging.LogRecord) -> str:

        payload: Dict[str, Any] = {
            'timestamp': self.formatTime(record, DATE_FORMAT) + '.{:03.0f}'.format(record.msecs),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'file': record.filename,
            'line': record.lineno,
            }

        for key, value in record.__dict__.items():
            if key not in _RESERVED_RECORD_FIELDS and not key.startswith('_'):
                payload[key] = value

        if record.exc_info:
            payload['exception'] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


class Log:

    def __init__(self, logFile: Optional[Path] = None, level: int = logging.INFO, logFormat: str = 'text') -> None:
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

        logFormat is 'text' (a line per record, for a person) or 'json' (an
        object per record, for a collector). It applies to handlers this
        instance installs; re-instantiating for a destination that already has a
        handler updates its level but leaves its format alone, since the first
        caller chose it deliberately.
        """

        formatter: logging.Formatter = JsonFormatter() if logFormat == 'json' else logging.Formatter(fmt=TEXT_FORMAT, datefmt=DATE_FORMAT)

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
