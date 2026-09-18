from __future__ import annotations

import copy
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from .scrubbing import scrubText

LOGGER_NAME = 'bauta'

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
    """One JSON object per line, for a log collector, carrying each record's
    extra={...} fields. Anything unserializable falls back to str().
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


class ScrubbingFilter(logging.Filter):
    """Removes quoted data values from a record's message and exception text.
    On the logger rather than its handlers, so it covers handlers a caller
    adds and records forwarded from job processes.
    """

    def filter(self, record: logging.LogRecord) -> bool:

        try:
            message = record.getMessage()
        except Exception:
            # A malformed call is the handler's to report, as it would be without this.
            return True

        record.msg = scrubText(message)
        record.args = None

        if record.exc_info:
            record.exc_text = scrubText(logging.Formatter().formatException(record.exc_info))
            record.exc_info = None
        elif record.exc_text:
            record.exc_text = scrubText(record.exc_text)

        return True


def _installScrubbing() -> None:

    logger = logging.getLogger(LOGGER_NAME)
    if not any(isinstance(existing, ScrubbingFilter) for existing in logger.filters):
        logger.addFilter(ScrubbingFilter())


# At import, so a job process -- which imports this and builds no Log -- scrubs
# its records before they are forwarded.
_installScrubbing()


def portableRecord(record: logging.LogRecord) -> logging.LogRecord:
    """A copy of `record` that can be pickled into another process: arguments
    resolved and the exception as text, but unformatted, so the receiver can
    still write JSON.
    """

    record = copy.copy(record)
    if record.exc_info:
        record.exc_text = logging.Formatter().formatException(record.exc_info)
    record.msg = record.getMessage()
    record.args = None
    record.exc_info = None

    return record


class ConnectionForwarder(logging.Handler):
    """Sends a job process's records to the main process over its own pipe --
    not a shared queue, whose lock a killed job could leave held.

    `send` shares the handler's lock, so the outcome never interleaves with a
    record.
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
    Called in each job process. Any existing handlers are detached, not closed.
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
    handlers.
    """

    logging.getLogger(LOGGER_NAME).handle(record)


class Log:

    def __init__(self, logFile: Optional[Path] = None, level: int = logging.INFO, logFormat: str = 'text') -> None:
        """Configures the package's own logger, which doesn't propagate, so a
        host application's logging is left alone.

        Handlers are deduplicated by destination, since the logger is
        process-wide: configuring one again only updates its level. logFormat
        is 'text' or 'json'.
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
        """Send records to a stream as well as, or instead of, a file.
        Deduplicated on the stream.
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
        """The handler already writing to `path`, compared through symlinks."""

        for handler in self.logging.handlers:
            if isinstance(handler, logging.FileHandler) and Path(handler.baseFilename).resolve() == path:
                return handler

        return None
