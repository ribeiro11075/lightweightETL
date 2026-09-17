from __future__ import annotations

import copy
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

LOGGER_NAME = 'understudy_data'

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
        elif record.exc_text:
            payload['exception'] = record.exc_text

        return json.dumps(payload, default=str)


def portableRecord(record: logging.LogRecord) -> logging.LogRecord:
    """A copy of `record` that can be pickled into another process, unformatted.

    Formatting here would bake a text layout into what the receiving process
    may want as JSON, so this only does what pickling requires: resolves the
    message's arguments, and turns the exception into text, since a traceback
    can't cross a process boundary. The receiver's formatter renders both --
    logging.Formatter appends exc_text by itself, and JsonFormatter reads it.
    """

    record = copy.copy(record)
    if record.exc_info:
        record.exc_text = logging.Formatter().formatException(record.exc_info)
    record.msg = record.getMessage()
    record.args = None
    record.exc_info = None

    return record


class ConnectionForwarder(logging.Handler):
    """Sends a job process's records to the main process over its own pipe.

    One pipe per job, owned by that job alone. A queue shared by every job has
    a lock shared by every job too, and a job stopped while holding it -- past
    its timeout, or killed for memory -- would leave every other job unable to
    log or exit. Nothing here outlives the process that owns it.

    `send` shares the handler's lock, so the job's outcome, sent on the same
    pipe, never interleaves with a record being written from another thread.
    """

    def __init__(self, connection: Any) -> None:
        super().__init__()
        self.connection = connection


    def send(self, kind: str, payload: Any) -> None:

        self.acquire()
        try:
            self.connection.send((kind, payload))
        finally:
            self.release()


    def emit(self, record: logging.LogRecord) -> None:

        try:
            self.connection.send(('log', portableRecord(record)))
        except Exception:
            self.handleError(record)


def forwardToConnection(connection: Any, level: int) -> ConnectionForwarder:
    """Routes this process's package records to `connection`, and nowhere else.

    Called in each job process. A forked process inherits its parent's
    handlers; they are detached (not closed -- the parent still owns them), so
    every record reaches a destination exactly once, through the parent.
    """

    logger = logging.getLogger(LOGGER_NAME)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
    forwarder = ConnectionForwarder(connection)
    logger.addHandler(forwarder)
    logger.setLevel(level)
    logger.propagate = False

    return forwarder


def handleForwardedRecord(record: logging.LogRecord) -> None:
    """Writes a record forwarded from a job process with this process's own
    handlers -- so it gets the same destinations, format and levels as
    everything else: a file, stderr, JSON, --quiet.
    """

    logging.getLogger(LOGGER_NAME).handle(record)


class Log:

    def __init__(self, logFile: Optional[Path] = None, level: int = logging.INFO, logFormat: str = 'text') -> None:
        """Configures the package's own named logger.

        The logger is named, with propagate=False, so this package's records stay
        in its own handlers and a host application's logging is left alone.
        Configuring the root logger instead would capture everyone's records.

        Handlers are deduplicated on the resolved file path, because the logger is
        process-wide: the CLI and runDataJobs both build a Log for the same file.
        Re-instantiating for an existing destination updates its level and leaves
        its format alone. Job processes don't build one at all; their records
        come back through handleForwardedRecord.

        logFile is optional so a caller that wants a stream -- the CLI, since a
        container only collects stdout/stderr -- needn't name a file. logFormat is
        'text' for a person or 'json' for a collector. At DEBUG, the runner adds
        resolved columns, adhoc SQL and per-chunk progress.
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
