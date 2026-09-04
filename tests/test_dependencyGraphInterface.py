import threading
import time
from queue import Empty
from typing import Optional

from library.configurationInterface import BaseJobConfig
from library.dependencyGraphInterface import DependencyGraph, JobStatus


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
    monkeypatch.setattr('library.dependencyGraphInterface.time.sleep', lambda seconds: None)

    graph = DependencyGraph(jobs={'a': _job(), 'b': _job(predecessors=['a'])})
    completionOrder = []

    def fakeWorker():
        while True:
            try:
                job = graph.readyQueue.get(timeout=0.3)
            except Empty:
                return
            completionOrder.append(job)
            graph.completedQueue.put({job: JobStatus.COMPLETED})

    workerThread = threading.Thread(target=fakeWorker, daemon=True)
    workerThread.start()
    graph.run()
    workerThread.join(timeout=1)

    assert completionOrder == ['a', 'b']
    assert set(graph.completedJobs) == {'a', 'b'}


def test_run_fails_a_job_whose_predecessor_failed(monkeypatch):
    monkeypatch.setattr('library.dependencyGraphInterface.time.sleep', lambda seconds: None)

    graph = DependencyGraph(jobs={'a': _job(), 'b': _job(predecessors=['a'])})

    def fakeWorker():
        while True:
            try:
                job = graph.readyQueue.get(timeout=0.3)
            except Empty:
                return
            graph.completedQueue.put({job: JobStatus.FAILED})

    workerThread = threading.Thread(target=fakeWorker, daemon=True)
    workerThread.start()
    graph.run()
    workerThread.join(timeout=1)

    assert graph.failedJobs == ['a', 'b']
