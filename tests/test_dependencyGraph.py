import threading
import time
from queue import Empty
from typing import Optional

from lightweight_etl.configuration import BaseJobConfig
from lightweight_etl.dependencyGraph import DependencyGraph, JobOutcome, JobStatus


def _job(active: bool = True, predecessors: Optional[list] = None, refresh: Optional[int] = None) -> BaseJobConfig:

    return BaseJobConfig(active=active, predecessors=predecessors or [], refresh=refresh)


def test_job_with_no_predecessors_is_immediately_ready():
    graph = DependencyGraph(jobs={'a': _job()})

    assert graph._isJobReady('a') is True


def test_job_is_not_ready_until_predecessor_completes():
    graph = DependencyGraph(jobs={'a': _job(), 'b': _job(predecessors=['a'])})

    assert graph._isJobReady('b') is False

    graph.completedJobs.append('a')
    assert graph._isJobReady('b') is True


def test_failed_predecessor_is_detected():
    graph = DependencyGraph(jobs={'a': _job(), 'b': _job(predecessors=['a'])})

    assert graph._predecessorFailCheck('b') is False

    graph.failedJobs.append('a')
    assert graph._predecessorFailCheck('b') is True


def test_inactive_jobs_are_excluded_from_the_graph():
    graph = DependencyGraph(jobs={'a': _job(active=False), 'b': _job()})

    assert set(graph.activeJobs.keys()) == {'b'}


def test_predecessor_to_an_inactive_job_is_not_tracked_as_active():
    """'a' is inactive, so 'b' has no *active* predecessors and is ready immediately."""
    graph = DependencyGraph(jobs={'a': _job(active=False), 'b': _job(predecessors=['a'])})

    assert graph._isJobReady('b') is True


def test_refresh_window_excludes_a_recently_run_job():
    graph = DependencyGraph(jobs={'a': _job(refresh=60)}, memory={'a': time.time()})

    assert 'a' not in graph.activeJobs


def test_refresh_window_allows_a_job_run_long_ago():
    graph = DependencyGraph(jobs={'a': _job(refresh=1)}, memory={'a': time.time() - 120})

    assert 'a' in graph.activeJobs


def test_run_schedules_a_job_only_after_its_predecessor_completes(monkeypatch):
    """Patches out the loop's real sleep so this test runs in milliseconds, not seconds."""
    monkeypatch.setattr('lightweight_etl.dependencyGraph.time.sleep', lambda seconds: None)

    graph = DependencyGraph(jobs={'a': _job(), 'b': _job(predecessors=['a'])})
    completionOrder = []

    def fakeWorker():
        while True:
            try:
                job = graph.readyQueue.get(timeout=0.3)
            except Empty:
                return
            completionOrder.append(job)
            graph.completedQueue.put(JobOutcome(job=job, status=JobStatus.COMPLETED))

    workerThread = threading.Thread(target=fakeWorker, daemon=True)
    workerThread.start()
    graph.run()
    workerThread.join(timeout=1)

    assert completionOrder == ['a', 'b']
    assert set(graph.completedJobs) == {'a', 'b'}


def test_run_fails_a_job_whose_predecessor_failed(monkeypatch):
    monkeypatch.setattr('lightweight_etl.dependencyGraph.time.sleep', lambda seconds: None)

    graph = DependencyGraph(jobs={'a': _job(), 'b': _job(predecessors=['a'])})

    def fakeWorker():
        while True:
            try:
                job = graph.readyQueue.get(timeout=0.3)
            except Empty:
                return
            graph.completedQueue.put(JobOutcome(job=job, status=JobStatus.FAILED))

    workerThread = threading.Thread(target=fakeWorker, daemon=True)
    workerThread.start()
    graph.run()
    workerThread.join(timeout=1)

    assert graph.failedJobs == ['a', 'b']


def _runWithFailingWorker(graph: DependencyGraph) -> None:
    """Drives the graph with a worker that fails everything it's handed, so the
    skip cascade downstream is what's actually under test.
    """

    def fakeWorker():
        while True:
            try:
                job = graph.readyQueue.get(timeout=0.3)
            except Empty:
                return
            graph.completedQueue.put(JobOutcome(job=job, status=JobStatus.FAILED, error='RuntimeError: boom'))

    workerThread = threading.Thread(target=fakeWorker, daemon=True)
    workerThread.start()
    graph.run()
    workerThread.join(timeout=1)


def test_a_job_skipped_for_a_failed_predecessor_records_why(monkeypatch):
    """It never reaches a worker, so nothing else can report it. Before outcomes
    existed this left no trace at all -- an operator looking for why a table was
    stale found silence.
    """
    monkeypatch.setattr('lightweight_etl.dependencyGraph.time.sleep', lambda seconds: None)
    graph = DependencyGraph(jobs={'first': _job(), 'second': _job(predecessors=['first'])})

    _runWithFailingWorker(graph)

    skipped = [outcome for outcome in graph.outcomes if outcome.status == JobStatus.SKIPPED]

    assert [outcome.job for outcome in skipped] == ['second']
    assert skipped[0].error == 'predecessor(s) did not complete: first'


def test_a_skipped_job_still_blocks_its_own_dependents(monkeypatch):
    """SKIPPED is terminal non-success: a job whose predecessor was skipped must
    not run either, or the cascade leaks past the first break.
    """
    monkeypatch.setattr('lightweight_etl.dependencyGraph.time.sleep', lambda seconds: None)
    graph = DependencyGraph(jobs={
        'first': _job(),
        'second': _job(predecessors=['first']),
        'third': _job(predecessors=['second']),
        })

    _runWithFailingWorker(graph)

    statuses = {outcome.job: outcome.status for outcome in graph.outcomes}

    assert statuses['first'] == JobStatus.FAILED
    assert statuses['second'] == JobStatus.SKIPPED
    assert statuses['third'] == JobStatus.SKIPPED


def test_a_job_does_not_wait_for_a_predecessor_that_is_inside_its_own_refresh_window():
    """`refresh` decides whether a job is in the cycle at all; `predecessors`
    only orders jobs *within* a cycle. A predecessor filtered out by its own
    refresh window is therefore not waited for.

    This is deliberate: the alternative would make a dependent's `refresh: 5`
    silently behave as its predecessor's `refresh: 60`, since it could never run
    outside that window. It's what lets an hourly dimension load and a 5-minute
    fact load coexist. The cost is that between the two windows the dependent
    reads output up to an hour old -- fine for a durable table, wrong if the
    predecessor produces something transient the dependent consumes.
    """
    now = time.time()
    jobs = {
        'slowPredecessor': _job(refresh=60),
        'fastDependent': _job(refresh=5, predecessors=['slowPredecessor']),
        }
    sixMinutesAgo = {'slowPredecessor': now - 6 * 60, 'fastDependent': now - 6 * 60}

    graph = DependencyGraph(jobs=jobs, memory=sixMinutesAgo)

    assert list(graph.activeJobs) == ['fastDependent']
    assert graph.activePredecessors['fastDependent'] == []
    assert graph._isJobReady('fastDependent') is True


def test_the_dependency_is_enforced_again_once_both_are_due():
    now = time.time()
    jobs = {
        'slowPredecessor': _job(refresh=60),
        'fastDependent': _job(refresh=5, predecessors=['slowPredecessor']),
        }
    anHourAgo = {'slowPredecessor': now - 61 * 60, 'fastDependent': now - 61 * 60}

    graph = DependencyGraph(jobs=jobs, memory=anHourAgo)

    assert set(graph.activeJobs) == {'slowPredecessor', 'fastDependent'}
    assert graph.activePredecessors['fastDependent'] == ['slowPredecessor']
    assert graph._isJobReady('fastDependent') is False
