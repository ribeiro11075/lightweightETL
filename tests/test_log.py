"""Regression coverage for Log's two old root-logger defects.

Every test here restores the package logger afterwards: Log configures a
process-wide named logger, so a handler left attached would leak into whatever
test ran next (and hold an open file handle to a torn-down tmp_path).
"""
import datetime
import json
import logging

import pytest

from understudy_data.log import LOGGER_NAME, Log


def _fileHandlers(logger):
    """Only the handlers Log itself installs.

    pytest's logging plugin attaches its own LogCaptureHandler to this logger
    (it's a StreamHandler, not a FileHandler), so the raw handler list isn't
    ours alone to count -- this mirrors the isinstance filter _findHandler uses
    to decide what it already owns.
    """

    return [handler for handler in logger.handlers if isinstance(handler, logging.FileHandler)]


@pytest.fixture(autouse=True)
def restoreLogger():

    logger = logging.getLogger(LOGGER_NAME)
    originalHandlers = list(logger.handlers)
    originalLevel = logger.level
    originalPropagate = logger.propagate
    logger.handlers = []

    yield

    for handler in logger.handlers:
        handler.close()
    logger.handlers = originalHandlers
    logger.level = originalLevel
    logger.propagate = originalPropagate


def test_log_does_not_configure_the_root_logger(tmp_path):
    """`logging.getLogger()` with no name is the root logger -- configuring it
    made this package silently own logging for the whole process.
    """
    rootHandlersBefore = list(logging.getLogger().handlers)

    log = Log(logFile=tmp_path / 'a.log')

    assert log.logging is not logging.getLogger()
    assert log.logging.name == LOGGER_NAME
    assert logging.getLogger().handlers == rootHandlersBefore


def test_a_host_applications_own_records_do_not_land_in_the_etl_log(tmp_path):
    logPath = tmp_path / 'a.log'
    Log(logFile=logPath)

    logging.getLogger('some.unrelated.app').warning('host application record')

    assert 'host application record' not in logPath.read_text()


def test_repeated_instantiation_for_one_file_writes_each_record_once(tmp_path):
    """The old implementation added a FileHandler per instance, so N instances
    meant every record written N times -- runDataJobs builds one Log and each
    worker process builds another for the same file.
    """
    logPath = tmp_path / 'a.log'
    first = Log(logFile=logPath)
    Log(logFile=logPath)
    Log(logFile=logPath)

    assert len(_fileHandlers(first.logging)) == 1

    first.logging.info('one record')

    assert logPath.read_text().count('one record') == 1


def test_a_relative_and_absolute_path_to_one_file_share_a_handler(tmp_path, monkeypatch):
    """Handlers are matched on the resolved path, so the worker and the parent
    agree even when they name the same file differently.
    """
    monkeypatch.chdir(tmp_path)
    log = Log(logFile=tmp_path / 'a.log')
    Log(logFile='a.log')

    assert len(_fileHandlers(log.logging)) == 1


def test_distinct_files_each_get_their_own_handler(tmp_path):
    log = Log(logFile=tmp_path / 'a.log')
    Log(logFile=tmp_path / 'b.log')

    assert len(_fileHandlers(log.logging)) == 2


def test_reinstantiating_updates_the_level_of_the_existing_handler(tmp_path):
    logPath = tmp_path / 'a.log'
    Log(logFile=logPath, level=logging.INFO)
    log = Log(logFile=logPath, level=logging.DEBUG)

    (handler,) = _fileHandlers(log.logging)

    assert handler.level == logging.DEBUG
    assert log.logging.level == logging.DEBUG

    log.logging.debug('debug record')

    assert 'debug record' in logPath.read_text()


def test_json_format_emits_one_parseable_object_per_record(tmp_path):
    logPath = tmp_path / 'a.log'
    log = Log(logFile=logPath, logFormat='json')

    log.logging.info('Completed loadOrders')

    payload = json.loads(logPath.read_text().strip())

    assert payload['message'] == 'Completed loadOrders'
    assert payload['level'] == 'INFO'
    assert payload['logger'] == LOGGER_NAME
    assert 'timestamp' in payload and 'file' in payload and 'line' in payload


def test_structured_fields_become_top_level_keys(tmp_path):
    """The point of JSON output isn't the encoding, it's that a collector can
    alert on status="failed" or chart rowCount without parsing a message string.
    """
    logPath = tmp_path / 'a.log'
    log = Log(logFile=logPath, logFormat='json')

    log.logging.info('Completed loadOrders',
                      extra={'job': 'loadOrders', 'status': 'completed', 'rowCount': 4200, 'attempts': 2})

    payload = json.loads(logPath.read_text().strip())

    assert payload['job'] == 'loadOrders'
    assert payload['status'] == 'completed'
    assert payload['rowCount'] == 4200
    assert payload['attempts'] == 2


def test_an_exception_is_carried_as_a_field_rather_than_breaking_the_line(tmp_path):
    logPath = tmp_path / 'a.log'
    log = Log(logFile=logPath, logFormat='json')

    try:
        raise RuntimeError('the source went away')
    except RuntimeError as error:
        log.logging.error('Failed loadOrders', exc_info=error)

    payload = json.loads(logPath.read_text().strip())

    assert 'RuntimeError: the source went away' in payload['exception']


def test_an_unserializable_value_does_not_lose_the_record(tmp_path):
    """Losing a log line to a TypeError while reporting an error would be a poor
    trade, so anything unserializable falls back to str().
    """
    logPath = tmp_path / 'a.log'
    log = Log(logFile=logPath, logFormat='json')

    log.logging.info('watermark advanced', extra={'watermark': datetime.datetime(2026, 9, 15, 10, 0, 0)})

    payload = json.loads(logPath.read_text().strip())

    assert payload['watermark'] == '2026-09-15 10:00:00'


def test_text_remains_the_default(tmp_path):
    logPath = tmp_path / 'a.log'
    log = Log(logFile=logPath)

    log.logging.info('a human reads this')

    contents = logPath.read_text()

    assert '[INFO] :: a human reads this' in contents
    assert not contents.startswith('{')


def test_every_record_is_one_line_even_with_a_multiline_message(tmp_path):
    """A collector splits on newlines; a record that spans lines is a record it
    cannot parse.
    """
    logPath = tmp_path / 'a.log'
    log = Log(logFile=logPath, logFormat='json')

    log.logging.info('line one\nline two')

    assert len(logPath.read_text().strip().split('\n')) == 1
