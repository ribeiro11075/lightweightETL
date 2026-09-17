"""Run history, Prometheus metrics and webhook notifications."""
import http.server
import json
import sqlite3
import threading

import pytest

from lightweight_etl.configuration import DatabaseConnectionConfig
from lightweight_etl.dependencyGraph import JobOutcome, JobStatus
from lightweight_etl.reporting import (DATABASE_HISTORY_SCHEMA, DatabaseHistory, FileHistory, historyRecords, notificationPayload, notify,
                                       pushMetrics, renderHistory, writeMetricsFile)
from lightweight_etl.runner import RunResult


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


def _samples(text):
    return {line.rsplit(' ', 1)[0]: float(line.rsplit(' ', 1)[1]) for line in text.splitlines() if not line.startswith('#')}


def test_the_metrics_file_describes_the_cycle_and_each_job(tmp_path):
    path = tmp_path / 'lightweight_etl.prom'

    writeMetricsFile(path, _result(COMPLETED, FAILED, SKIPPED), now=1_790_000_100.0)

    samples = _samples(path.read_text())
    assert samples['lightweight_etl_job_last_run_success{job="loadOrders"}'] == 1.0
    assert samples['lightweight_etl_job_last_run_rows{job="loadOrders"}'] == 42.0
    assert samples['lightweight_etl_job_last_success_timestamp_seconds{job="loadOrders"}'] == 1_790_000_012.5
    assert samples['lightweight_etl_job_last_run_success{job="loadCustomers"}'] == 0.0
    assert 'lightweight_etl_job_last_success_timestamp_seconds{job="loadCustomers"}' not in samples
    assert samples['lightweight_etl_job_last_run_skipped{job="loadInvoices"}'] == 1.0
    assert samples['lightweight_etl_cycle_jobs{status="failed"}'] == 1.0
    assert samples['lightweight_etl_cycle_rows'] == 42.0
    assert path.read_text().count('# TYPE lightweight_etl_job_last_run_success gauge') == 1


def test_a_job_outside_the_cycle_keeps_its_last_values(tmp_path):
    """A job inside its refresh window isn't in the cycle; its series must not
    vanish, and its last success must stay what it was.
    """
    path = tmp_path / 'lightweight_etl.prom'
    writeMetricsFile(path, _result(COMPLETED), now=1_790_000_100.0)

    writeMetricsFile(path, _result(FAILED._replace(job='loadOrders', finishedAt=1_790_000_200.0)), now=1_790_000_300.0)
    writeMetricsFile(path, _result(SKIPPED), now=1_790_000_400.0)

    samples = _samples(path.read_text())
    assert samples['lightweight_etl_job_last_run_success{job="loadOrders"}'] == 0.0
    assert samples['lightweight_etl_job_last_success_timestamp_seconds{job="loadOrders"}'] == 1_790_000_012.5
    assert samples['lightweight_etl_job_last_run_skipped{job="loadInvoices"}'] == 1.0
    assert samples['lightweight_etl_cycle_jobs{status="skipped"}'] == 1.0


def test_job_names_are_escaped_in_labels_and_survive_a_rewrite(tmp_path):
    path = tmp_path / 'lightweight_etl.prom'
    odd = COMPLETED._replace(job='say "hi" \\ there')

    writeMetricsFile(path, _result(odd))
    writeMetricsFile(path, _result())

    assert 'lightweight_etl_job_last_run_rows{job="say \\"hi\\" \\\\ there"} 42.0' in path.read_text()


class _Recorder(http.server.BaseHTTPRequestHandler):

    def _record(self):
        body = self.rfile.read(int(self.headers['Content-Length']))
        self.server.requests.append((self.command, self.path, self.headers['Content-Type'], body.decode('utf-8')))
        self.send_response(200)
        self.end_headers()

    do_PUT = do_POST = _record

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


def test_metrics_are_pushed_per_job_and_per_cycle(webServer):
    server, url = webServer

    pushMetrics(url, _result(COMPLETED, FAILED), now=1_790_000_100.0)

    paths = [request[1] for request in server.requests]
    assert paths == ['/metrics/job/lightweight_etl/etl_job@base64/bG9hZE9yZGVycw==',
                     '/metrics/job/lightweight_etl/etl_job@base64/bG9hZEN1c3RvbWVycw==',
                     '/metrics/job/lightweight_etl']
    assert all(request[0] == 'PUT' and request[2].startswith('text/plain') for request in server.requests)
    assert 'lightweight_etl_job_last_run_rows 42.0' in server.requests[0][3]
    assert 'job=' not in server.requests[0][3]
    assert 'lightweight_etl_cycle_rows 42' in server.requests[2][3]


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

    assert payload['text'].startswith('lightweight-etl on ')
    assert ': succeeded -- 1 completed, 0 failed, 0 skipped, 42 row(s)' in payload['text']
