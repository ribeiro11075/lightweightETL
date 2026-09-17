"""What happened, for people and for monitoring: run history, metrics and
notifications.

Each takes a cycle's RunResult -- runDataJobs hands one to its `onCycle`
callback as every cycle ends -- and none of them can stop a load: the runner
logs a reporting failure and carries on.
"""
from __future__ import annotations

import base64
import datetime
import json
import os
import re
import socket
import urllib.request
import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

from .configuration import DatabaseConnectionConfig
from .database import Database
from .dependencyGraph import JobOutcome, JobStatus
from .memory import exclusiveLock
from .runner import RunResult

HTTP_TIMEOUT_SECONDS = 10

ERROR_TEXT_LIMIT = 2000


def _timestamp(seconds: float) -> Optional[str]:

    if not seconds:
        return None

    return datetime.datetime.fromtimestamp(seconds, datetime.timezone.utc).isoformat(timespec='seconds')


def historyRecords(result: RunResult, runId: str) -> List[Dict[str, Any]]:
    """One record per job in the cycle."""

    return [{
        'runId': runId,
        'job': outcome.job,
        'status': outcome.status.value,
        'rowCount': outcome.rowCount,
        'attempts': outcome.attempts,
        'startedAt': _timestamp(outcome.startedAt),
        'finishedAt': _timestamp(outcome.finishedAt),
        'durationSeconds': round(outcome.durationSeconds, 3),
        'error': (outcome.error or None) and outcome.error[:ERROR_TEXT_LIMIT],
        } for outcome in result.outcomes]


class RunHistory(ABC):
    """An append-only record of every job's outcome, run after run.

    Unlike MemoryBackend, nothing reads this to decide what to run: it is for
    people asking what happened last night. Same pickling contract, though
    only the main process uses it.
    """

    @abstractmethod
    def append(self, result: RunResult, runId: str) -> None:
        """Records one cycle's outcomes."""

    @abstractmethod
    def read(self, limit: int = 20, job: Optional[str] = None) -> List[Dict[str, Any]]:
        """The most recent records, newest first."""


class FileHistory(RunHistory):
    """History as JSON lines, one per job outcome, appended under a lock."""

    def __init__(self, historyFile: Union[str, Path]) -> None:
        self.historyFile = Path(historyFile)
        self._lockFile = self.historyFile.with_name(self.historyFile.name + '.lock')


    def append(self, result: RunResult, runId: str) -> None:

        lines = ''.join(json.dumps(record, default=str) + '\n' for record in historyRecords(result, runId))
        if not lines:
            return

        self.historyFile.parent.mkdir(parents=True, exist_ok=True)
        with exclusiveLock(self._lockFile), open(self.historyFile, 'a') as file:
            file.write(lines)
            file.flush()
            os.fsync(file.fileno())


    def read(self, limit: int = 20, job: Optional[str] = None) -> List[Dict[str, Any]]:

        if not self.historyFile.exists():
            return []

        with exclusiveLock(self._lockFile), open(self.historyFile) as file:
            records = [json.loads(line) for line in file if line.strip()]

        matching = [record for record in records if job is None or record['job'] == job]

        return list(reversed(matching))[:limit]


DATABASE_HISTORY_SCHEMA = """CREATE TABLE understudy_history (
    run_id VARCHAR(36) NOT NULL,
    job VARCHAR(255) NOT NULL,
    status VARCHAR(16) NOT NULL,
    row_count NUMERIC(19),
    attempts INT,
    started_at DOUBLE PRECISION,
    finished_at DOUBLE PRECISION,
    error VARCHAR(2000),
    PRIMARY KEY (run_id, job)
    )"""

_HISTORY_COLUMNS = ['run_id', 'job', 'status', 'row_count', 'attempts', 'started_at', 'finished_at', 'error']


class DatabaseHistory(RunHistory):
    """History in a table -- for the same deployments DatabaseMemory is for.

    The table must already exist; see DATABASE_HISTORY_SCHEMA, whose types all
    six dialects accept (Oracle has no BIGINT). Times are epoch seconds, like
    DatabaseMemory's, since that's the one timestamp every dialect stores the
    same way.
    """

    def __init__(self, connectionSettings: DatabaseConnectionConfig, table: str = 'understudy_history') -> None:
        self.connectionSettings = connectionSettings
        self.table = table


    def append(self, result: RunResult, runId: str) -> None:

        rows = [(runId, outcome.job, outcome.status.value, outcome.rowCount, outcome.attempts, outcome.startedAt or None,
                 outcome.finishedAt or None, (outcome.error or None) and outcome.error[:ERROR_TEXT_LIMIT])
                for outcome in result.outcomes]
        if not rows:
            return

        with Database(connectionSettings=self.connectionSettings) as database:
            database.insert(table=self.table, data=rows, columns=_HISTORY_COLUMNS)


    def read(self, limit: int = 20, job: Optional[str] = None) -> List[Dict[str, Any]]:

        with Database(connectionSettings=self.connectionSettings) as database:
            query = 'SELECT {} FROM {}'.format(', '.join(_HISTORY_COLUMNS), self.table)
            parameters = None
            if job is not None:
                query += ' WHERE job = {}'.format(database.dialect.placeholders(1)[0])
                parameters = (job,)
            query += ' ORDER BY finished_at DESC, job'

            _, chunks = database.stream(query=query, chunkSize=limit, parameters=parameters)
            try:
                rows = next(chunks, [])
            finally:
                chunks.close()  # type: ignore[attr-defined]

        records = []
        for runId, name, status, rowCount, attempts, startedAt, finishedAt, error in rows:
            started, finished = float(startedAt or 0), float(finishedAt or 0)
            # NUMERIC comes back as Decimal on some drivers, which JSON can't write.
            records.append({'runId': runId, 'job': name, 'status': status, 'rowCount': int(rowCount or 0), 'attempts': int(attempts or 0),
                            'startedAt': _timestamp(started), 'finishedAt': _timestamp(finished),
                            'durationSeconds': round(max(0.0, finished - started), 3) if started and finished else 0.0, 'error': error})

        return records


def renderHistory(records: Sequence[Mapping[str, Any]]) -> str:

    lines = ['{:<20} {:<28} {:<10} {:>10} {:>9}  {}'.format('FINISHED', 'JOB', 'STATUS', 'ROWS', 'SECONDS', 'ERROR')]
    for record in records:
        lines.append('{:<20} {:<28} {:<10} {:>10} {:>9.1f}  {}'.format(
            (record['finishedAt'] or '-')[:19].replace('T', ' '), record['job'], record['status'], record['rowCount'] or 0,
            record['durationSeconds'] or 0.0, (record['error'] or '')[:80]))

    return '\n'.join(lines) + '\n'


def newRunId() -> str:

    return str(uuid.uuid4())


# Prometheus -----------------------------------------------------------------

JOB_METRICS = (
    ('understudy_job_last_run_success', 'Whether the job completed in its latest run (1), or failed or was skipped (0).'),
    ('understudy_job_last_run_skipped', 'Whether the job was skipped in its latest run.'),
    ('understudy_job_last_run_rows', 'Rows the job loaded in its latest run.'),
    ('understudy_job_last_run_duration_seconds', 'How long the job took in its latest run.'),
    ('understudy_job_last_run_timestamp_seconds', 'When the job last ran.'),
    ('understudy_job_last_success_timestamp_seconds', 'When the job last completed; alert on this to catch stale data.'),
    )

CYCLE_METRICS = (
    ('understudy_cycle_jobs', 'Jobs in the latest cycle, by status.'),
    ('understudy_cycle_rows', 'Rows loaded in the latest cycle.'),
    ('understudy_cycle_timestamp_seconds', 'When the latest cycle finished.'),
    )

_SAMPLE = re.compile(r'^(understudy_job_\w+)\{job="((?:[^"\\]|\\.)*)"\} (\S+)$')


def _escapeLabel(value: str) -> str:

    return value.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')


def _unescapeLabel(value: str) -> str:

    return re.sub(r'\\(.)', lambda match: '\n' if match.group(1) == 'n' else match.group(1), value)


def _jobSamples(outcome: JobOutcome, now: float, previous: Mapping[str, float]) -> Dict[str, float]:

    completed = outcome.status == JobStatus.COMPLETED
    samples = {
        'understudy_job_last_run_success': 1.0 if completed else 0.0,
        'understudy_job_last_run_skipped': 1.0 if outcome.status == JobStatus.SKIPPED else 0.0,
        'understudy_job_last_run_rows': float(outcome.rowCount),
        'understudy_job_last_run_duration_seconds': round(outcome.durationSeconds, 3),
        'understudy_job_last_run_timestamp_seconds': round(outcome.finishedAt or now, 3),
        }
    lastSuccess = round(outcome.finishedAt or now, 3) if completed else previous.get('understudy_job_last_success_timestamp_seconds')
    if lastSuccess is not None:
        samples['understudy_job_last_success_timestamp_seconds'] = lastSuccess

    return samples


def _renderFamilies(families: Mapping[str, str], samples: Mapping[str, List[str]]) -> str:

    lines = []
    for name, description in families.items():
        if samples.get(name):
            lines += ['# HELP {} {}'.format(name, description), '# TYPE {} gauge'.format(name)] + samples[name]

    return '\n'.join(lines) + '\n' if lines else ''


def _cycleText(result: RunResult, now: float) -> Dict[str, List[str]]:

    return {
        'understudy_cycle_jobs': ['understudy_cycle_jobs{{status="{}"}} {}'.format(status, len(outcomes)) for status, outcomes in
                                       (('completed', result.completed), ('failed', result.failed), ('skipped', result.skipped))],
        'understudy_cycle_rows': ['understudy_cycle_rows {}'.format(result.rowCount)],
        'understudy_cycle_timestamp_seconds': ['understudy_cycle_timestamp_seconds {}'.format(round(now, 3))],
        }


def _jobText(state: Mapping[str, Mapping[str, float]]) -> Dict[str, List[str]]:

    samples: Dict[str, List[str]] = {name: [] for name, _ in JOB_METRICS}
    for job in sorted(state):
        for name, value in state[job].items():
            samples[name].append('{}{{job="{}"}} {}'.format(name, _escapeLabel(job), repr(float(value))))

    return samples


def writeMetricsFile(path: Union[str, Path], result: RunResult, now: Optional[float] = None) -> None:
    """A Prometheus text file, for node_exporter's textfile collector.

    A job that wasn't in this cycle -- inside its refresh window, say -- keeps
    the values the file already had for it, so its series don't vanish between
    runs; `..._last_success_timestamp_seconds` says how stale it is. The file
    is replaced atomically, since the collector may read it at any moment.
    """

    path = Path(path)
    now = datetime.datetime.now(datetime.timezone.utc).timestamp() if now is None else now
    state: Dict[str, Dict[str, float]] = {}

    if path.exists():
        for line in path.read_text().splitlines():
            match = _SAMPLE.match(line)
            if match:
                state.setdefault(_unescapeLabel(match.group(2)), {})[match.group(1)] = float(match.group(3))

    for outcome in result.outcomes:
        state[outcome.job] = _jobSamples(outcome, now, state.get(outcome.job, {}))

    families = dict(JOB_METRICS + CYCLE_METRICS)
    text = _renderFamilies(families, {**_jobText(state), **_cycleText(result, now)})

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.tmp')
    temporary.write_text(text)
    os.replace(temporary, path)


def _put(url: str, body: bytes, contentType: str) -> None:

    request = urllib.request.Request(url, data=body, method='PUT', headers={'Content-Type': contentType})
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
        response.read()


def pushMetrics(gatewayUrl: str, result: RunResult, now: Optional[float] = None) -> None:
    """Pushes the cycle to a Prometheus Pushgateway.

    Each job goes in a group of its own, replaced only when that job runs, so a
    job outside this cycle keeps its last values there too. The cycle's totals
    go in the `understudy` group.
    """

    now = datetime.datetime.now(datetime.timezone.utc).timestamp() if now is None else now
    base = gatewayUrl.rstrip('/') + '/metrics/job/understudy'
    families = dict(JOB_METRICS + CYCLE_METRICS)
    contentType = 'text/plain; version=0.0.4'

    for outcome in result.outcomes:
        # Label values in the path must be base64 once they may hold a slash.
        encoded = base64.urlsafe_b64encode(outcome.job.encode('utf-8')).decode('ascii')
        text = _renderFamilies(families, _jobText({outcome.job: _jobSamples(outcome, now, {})}))
        # The job label is in the path; Pushgateway refuses it in the body too.
        _put('{}/etl_job@base64/{}'.format(base, encoded), re.sub(r'\{job="(?:[^"\\]|\\.)*"\}', '', text).encode('utf-8'), contentType)

    _put(base, _renderFamilies(families, _cycleText(result, now)).encode('utf-8'), contentType)


# Notifications ----------------------------------------------------------------

def notificationPayload(result: RunResult) -> Dict[str, Any]:
    """The JSON posted to a webhook: a `text` line that Slack, Mattermost and
    Teams show as it is, plus the details for anything that parses it.
    """

    host = socket.gethostname()
    status = 'interrupted' if result.interrupted else ('succeeded' if result.succeeded else 'failed')
    headline = 'understudy on {}: {} -- {} completed, {} failed, {} skipped, {} row(s)'.format(
        host, status, len(result.completed), len(result.failed), len(result.skipped), result.rowCount)
    problems = ['- {} {}: {}'.format(outcome.job, outcome.status.value, (outcome.error or '')[:300]) for outcome in result.failed + result.skipped]

    return {
        'text': '\n'.join([headline] + problems),
        'status': status,
        'host': host,
        'summary': {'completed': len(result.completed), 'failed': len(result.failed), 'skipped': len(result.skipped), 'rows': result.rowCount},
        'jobs': [{'job': outcome.job, 'status': outcome.status.value, 'rowCount': outcome.rowCount,
                  'durationSeconds': round(outcome.durationSeconds, 3), 'error': outcome.error} for outcome in result.outcomes],
        }


def notify(url: str, result: RunResult, always: bool = False) -> bool:
    """Posts the cycle to a webhook if it didn't succeed, or always. Returns
    whether anything was sent.
    """

    if result.succeeded and not result.interrupted and not always:
        return False

    request = urllib.request.Request(url, data=json.dumps(notificationPayload(result), default=str).encode('utf-8'), method='POST',
                                     headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
        response.read()

    return True
