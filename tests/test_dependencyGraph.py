import time
from typing import List, Optional

import pytest

from lightweight_etl.configuration import BaseJobConfig, ConfigurationError
from lightweight_etl.dependencyGraph import DependencyGraph, JobOutcome, JobStatus


def _job(active: bool = True, predecessors: Optional[list] = None, refresh: Optional[int] = None) -> BaseJobConfig:

    return BaseJobConfig(active=active, predecessors=predecessors or [], refresh=refresh)


def _drive(graph: DependencyGraph, status: JobStatus = JobStatus.COMPLETED) -> List[str]:
    """Runs the graph to the end as a runner would, finishing every started job
    with `status`, and returns the order jobs were started in.
    """

    started: List[str] = []

    while not graph.finished:
        ready = graph.takeReady()
        assert ready or graph.finished, 'nothing is ready, yet the graph is not finished'
        for job in ready:
            started.append(job)
            graph.finish(JobOutcome(job=job, status=status, error=None if status == JobStatus.COMPLETED else 'RuntimeError: boom'))

    return started


def test_job_with_no_predecessors_is_immediately_ready():
    graph = DependencyGraph(jobs={'a': _job()})

    assert graph.takeReady() == ['a']


def test_job_is_not_ready_until_predecessor_completes():
    graph = DependencyGraph(jobs={'a': _job(), 'b': _job(predecessors=['a'])})

    assert graph.takeReady() == ['a']
    assert graph.takeReady() == []

    graph.finish(JobOutcome(job='a', status=JobStatus.COMPLETED))
    assert graph.takeReady() == ['b']


def test_a_ready_job_is_handed_out_only_once():
    graph = DependencyGraph(jobs={'a': _job()})

    graph.takeReady()

    assert graph.takeReady() == []
    assert not graph.finished


def test_independent_jobs_are_ready_together():
    graph = DependencyGraph(jobs={'a': _job(), 'b': _job(), 'c': _job(predecessors=['a'])})

    assert graph.takeReady() == ['a', 'b']


def test_inactive_jobs_are_excluded_from_the_graph():
    graph = DependencyGraph(jobs={'a': _job(active=False), 'b': _job()})

    assert set(graph.activeJobs.keys()) == {'b'}


def test_predecessor_to_an_inactive_job_is_not_tracked_as_active():
    """'a' is inactive, so 'b' has no *active* predecessors and is ready immediately."""
    graph = DependencyGraph(jobs={'a': _job(active=False), 'b': _job(predecessors=['a'])})

    assert graph.takeReady() == ['b']


def test_refresh_window_excludes_a_recently_run_job():
    graph = DependencyGraph(jobs={'a': _job(refresh=60)}, memory={'a': time.time()})

    assert 'a' not in graph.activeJobs


def test_refresh_window_allows_a_job_run_long_ago():
    graph = DependencyGraph(jobs={'a': _job(refresh=1)}, memory={'a': time.time() - 120})

    assert 'a' in graph.activeJobs


def test_an_empty_graph_is_finished_from_the_start():
    assert DependencyGraph(jobs={'a': _job(active=False)}).finished


def test_jobs_start_only_after_their_predecessors_complete():
    graph = DependencyGraph(jobs={'b': _job(predecessors=['a']), 'a': _job()})

    assert _drive(graph) == ['a', 'b']
    assert {outcome.job: outcome.status for outcome in graph.outcomes} == {'a': JobStatus.COMPLETED, 'b': JobStatus.COMPLETED}


def test_a_job_skipped_for_a_failed_predecessor_records_why():
    """It never reaches a worker, so nothing else can report it. Before outcomes
    existed this left no trace at all -- an operator looking for why a table was
    stale found silence.
    """
    graph = DependencyGraph(jobs={'first': _job(), 'second': _job(predecessors=['first'])})

    assert _drive(graph, status=JobStatus.FAILED) == ['first']

    skipped = [outcome for outcome in graph.outcomes if outcome.status == JobStatus.SKIPPED]

    assert [outcome.job for outcome in skipped] == ['second']
    assert skipped[0].error == 'predecessor(s) did not complete: first'


def test_a_skipped_job_still_blocks_its_own_dependents():
    """SKIPPED is terminal non-success: a job whose predecessor was skipped must
    not run either, or the cascade leaks past the first break.
    """
    graph = DependencyGraph(jobs={
        'first': _job(),
        'second': _job(predecessors=['first']),
        'third': _job(predecessors=['second']),
        })

    _drive(graph, status=JobStatus.FAILED)

    statuses = {outcome.job: outcome.status for outcome in graph.outcomes}

    assert statuses == {'first': JobStatus.FAILED, 'second': JobStatus.SKIPPED, 'third': JobStatus.SKIPPED}
    assert graph.finished


def test_skip_not_started_leaves_running_jobs_alone():
    graph = DependencyGraph(jobs={'a': _job(), 'b': _job(predecessors=['a'])})
    graph.takeReady()

    graph.skipNotStarted('shutting down')

    assert [(outcome.job, outcome.status, outcome.error) for outcome in graph.outcomes] == [('b', JobStatus.SKIPPED, 'shutting down')]
    assert not graph.finished

    graph.finish(JobOutcome(job='a', status=JobStatus.COMPLETED))
    assert graph.finished


def test_a_cycle_among_active_jobs_is_refused():
    """No job in a cycle can ever become ready, so a run containing one would
    wait forever. Refused here too, for callers that skip validateJobGraph.
    """
    with pytest.raises(ConfigurationError, match='a -> b -> a'):
        DependencyGraph(jobs={'a': _job(predecessors=['b']), 'b': _job(predecessors=['a'])})


def test_a_cycle_broken_by_an_inactive_job_is_fine():
    graph = DependencyGraph(jobs={'a': _job(predecessors=['b']), 'b': _job(active=False, predecessors=['a'])})

    assert graph.takeReady() == ['a']


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
    assert graph.takeReady() == ['fastDependent']


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
    assert graph.takeReady() == ['slowPredecessor']


def test_take_ready_starts_no_more_than_the_limit():
    graph = DependencyGraph(jobs={'a': _job(), 'b': _job(), 'c': _job()})

    assert graph.takeReady(limit=2) == ['a', 'b']
    assert graph.takeReady(limit=0) == []

    graph.finish(JobOutcome(job='a', status=JobStatus.COMPLETED))
    assert graph.takeReady(limit=1) == ['c']


def test_skips_cascade_even_when_no_job_may_start():
    graph = DependencyGraph(jobs={'a': _job(), 'b': _job(predecessors=['a']), 'c': _job(predecessors=['b'])})
    graph.takeReady()
    graph.finish(JobOutcome(job='a', status=JobStatus.FAILED))

    assert graph.takeReady(limit=0) == []
    assert graph.finished
