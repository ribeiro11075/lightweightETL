"""Run history and webhook notifications."""
import http.server
import json
import sqlite3
import threading

import pytest

from bauta.configuration import DatabaseConnectionConfig
from bauta.dependencyGraph import JobOutcome, JobStatus
from bauta.reporting import (DATABASE_HISTORY_SCHEMA, DATABASE_MANIFEST_SCHEMA, DatabaseHistory, DatabaseManifests, FileHistory, historyRecords,
                             notificationPayload, notify, renderHistory)
from bauta.runner import RunResult


def _result(*outcomes: JobOutcome, interrupted: bool = False) -> RunResult:
    return RunResult(outcomes=list(outcomes), interrupted=interrupted)


COMPLETED = JobOutcome(job='loadOrders', status=JobStatus.COMPLETED, rowCount=42, startedAt=1_790_000_000.0, finishedAt=1_790_000_012.5)
FAILED = JobOutcome(job='loadCustomers', status=JobStatus.FAILED, error='OperationalError: timeout', attempts=3,
                    startedAt=1_790_000_000.0, finishedAt=1_790_000_001.0)
SKIPPED = JobOutcome(job='loadInvoices', status=JobStatus.SKIPPED, error='predecessor(s) did not complete: loadCustomers')


def test_history_records_describe_each_outcome():
    completed, skipped = historyRecords(_result(COMPLETED, SKIPPED), 'run-1')

    assert completed == {'runId': 'run-1', 'job': 'loadOrders', 'status': 'completed', 'rowCount': 42, 'attempts': 1,
                         'startedAt': '2026-09-21T14:13:20+00:00', 'finishedAt': '2026-09-21T14:13:32+00:00', 'durationSeconds': 12.5,
                         'error': None}
    assert skipped['startedAt'] is None and skipped['error'].startswith('predecessor')


def test_file_history_appends_and_reads_newest_first(tmp_path):
    history = FileHistory(tmp_path / 'logs' / 'history.jsonl')

    history.append(_result(COMPLETED, FAILED), 'run-1')
    history.append(_result(COMPLETED), 'run-2')

    assert [(record['runId'], record['job']) for record in history.read()] == [
        ('run-2', 'loadOrders'), ('run-1', 'loadCustomers'), ('run-1', 'loadOrders')]
    assert [record['runId'] for record in history.read(job='loadOrders', limit=1)] == ['run-2']
    assert len((tmp_path / 'logs' / 'history.jsonl').read_text().splitlines()) == 3


def test_reading_history_that_was_never_written_is_empty(tmp_path):
    assert FileHistory(tmp_path / 'history.jsonl').read() == []


def test_database_history_round_trips(tmp_path):
    path = tmp_path / 'history.db'
    connection = sqlite3.connect(path)
    connection.execute(DATABASE_HISTORY_SCHEMA)
    connection.close()
    history = DatabaseHistory(DatabaseConnectionConfig(type='sqlite', database=str(path)))

    history.append(_result(FAILED), 'run-1')
    history.append(_result(COMPLETED, SKIPPED), 'run-2')

    records = history.read(limit=2)
    assert [(record['runId'], record['job']) for record in records] == [('run-2', 'loadOrders'), ('run-1', 'loadCustomers')]
    assert records[0]['durationSeconds'] == 12.5 and records[1]['error'] == 'OperationalError: timeout'
    assert [record['status'] for record in history.read(job='loadInvoices')] == ['skipped']


def test_history_renders_as_a_table():
    text = renderHistory(historyRecords(_result(FAILED), 'run-1'))

    assert text.splitlines()[1].split()[:5] == ['2026-09-21', '14:13:21', 'loadCustomers', 'failed', '0']
    assert 'OperationalError: timeout' in text


@pytest.fixture
def manifestTable(tmp_path):
    path = tmp_path / 'manifests.db'
    connection = sqlite3.connect(path)
    connection.execute(DATABASE_MANIFEST_SCHEMA)
    connection.close()

    return DatabaseManifests(DatabaseConnectionConfig(type='sqlite', database=str(path))), path


def test_a_manifest_longer_than_a_part_is_stored_in_order_and_read_back_whole(manifestTable):
    manifests, path = manifestTable
    manifest = {'jobs': [{'job': 'j{}'.format(index), 'columns': ['c'] * 40} for index in range(30)]}

    manifests.write(manifest, 'run-1')

    connection = sqlite3.connect(path)
    parts = [row[0] for row in connection.execute("SELECT part FROM bauta_manifest WHERE run_id = 'run-1' ORDER BY part")]
    connection.close()
    assert len(parts) > 1 and parts == list(range(len(parts)))
    assert manifests.read('run-1') == ('run-1', manifest)


def test_the_latest_manifest_is_read_unless_a_run_is_named(manifestTable):
    manifests, _ = manifestTable

    manifests.write({'jobs': ['older']}, 'run-1')
    manifests.write({'jobs': ['newer']}, 'run-2')

    assert manifests.read() == ('run-2', {'jobs': ['newer']})
    assert manifests.read('run-1') == ('run-1', {'jobs': ['older']})


def test_reading_a_manifest_that_is_not_there_is_a_key_error(manifestTable):
    manifests, _ = manifestTable

    with pytest.raises(KeyError, match='holds no manifest'):
        manifests.read()

    manifests.write({'jobs': []}, 'run-1')
    with pytest.raises(KeyError, match='no manifest for run run-9'):
        manifests.read('run-9')


class _Recorder(http.server.BaseHTTPRequestHandler):

    def _record(self):
        body = self.rfile.read(int(self.headers['Content-Length']))
        self.server.requests.append((self.command, self.path, self.headers['Content-Type'], body.decode('utf-8')))
        self.send_response(200)
        self.end_headers()

    do_POST = _record

    def log_message(self, *arguments):
        pass


@pytest.fixture
def webServer():
    server = http.server.HTTPServer(('127.0.0.1', 0), _Recorder)
    server.requests = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    yield server, 'http://127.0.0.1:{}'.format(server.server_port)

    server.shutdown()


def test_a_notification_is_sent_only_when_a_cycle_does_not_succeed(webServer):
    server, url = webServer

    assert notify(url, _result(COMPLETED)) is False
    assert notify(url, _result(COMPLETED, FAILED, SKIPPED)) is True
    assert notify(url, _result(COMPLETED), always=True) is True
    assert notify(url, _result(COMPLETED, interrupted=True)) is True

    assert len(server.requests) == 3
    payload = json.loads(server.requests[0][3])
    assert payload['status'] == 'failed'
    assert payload['summary'] == {'completed': 1, 'failed': 1, 'skipped': 1, 'rows': 42}
    assert '- loadCustomers failed: OperationalError: timeout' in payload['text']
    assert json.loads(server.requests[2][3])['status'] == 'interrupted'


def test_the_notification_text_leads_with_the_outcome():
    payload = notificationPayload(_result(COMPLETED))

    assert payload['text'].startswith('bauta on ')
    assert ': succeeded -- 1 completed, 0 failed, 0 skipped, 42 row(s)' in payload['text']
