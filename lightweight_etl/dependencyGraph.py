from __future__ import annotations

import time
from enum import Enum
from typing import Any, Dict, List, Mapping, NamedTuple, Optional, Set

from .configuration import BaseJobConfig, ConfigurationError, findCycle


class JobStatus(str, Enum):
    COMPLETED = 'completed'
    FAILED = 'failed'
    SKIPPED = 'skipped'


class JobOutcome(NamedTuple):
    """What became of one job in one cycle.

    Workers return this from another process, so every field has to pickle.
    That's why `error` is a formatted string rather than the exception:
    database drivers raise exception types that don't reliably survive
    pickling, and losing the whole outcome to a pickling error while reporting
    a failure would be a poor trade.

    SKIPPED is distinct from FAILED on purpose. A job whose predecessor failed
    never ran at all, and telling an operator it "failed" sends them looking for
    an error it doesn't have. Both are still terminal non-success, so both count
    against a run's overall success.
    """

    job: str
    status: JobStatus
    rowCount: int = 0
    watermark: Any = None
    error: Optional[str] = None
    attempts: int = 1
    startedAt: float = 0.0
    finishedAt: float = 0.0
    # For a masked job that completed: the policy applied to each column, as
    # plain dicts so it pickles. See masking.buildMaskingManifest.
    masking: Optional[Dict[str, Any]] = None

    @property
    def durationSeconds(self) -> float:

        return max(0.0, self.finishedAt - self.startedAt)


class DependencyGraph:
    """Which of one cycle's jobs may start, given which have finished.

    Bookkeeping only -- no processes, queues or sleeping. The runner takes the
    ready jobs, runs them however it likes, and reports each outcome back with
    finish(); the graph never waits on anything, so it can't hang.

    A cycle among the active jobs raises ConfigurationError here, whether or
    not the caller validated the configuration first: no job in a cycle could
    ever become ready.
    """

    def __init__(self, jobs: Mapping[str, BaseJobConfig], memory: Optional[Dict[str, float]] = None) -> None:
        self.jobs = jobs
        self.memory = memory
        self.activePredecessors: Dict[str, List[str]] = {}
        self.activeJobs = self._getActiveJobsWithActivePredecessors()
        self.outcomes: List[JobOutcome] = []
        self._notStarted: List[str] = list(self.activeJobs)
        self._running: Set[str] = set()
        self._completed: Set[str] = set()
        self._unsuccessful: Set[str] = set()

        cycle = findCycle(self.activePredecessors)
        if cycle:
            raise ConfigurationError('predecessors form a cycle, so none of these jobs could ever start: {}'.format(' -> '.join(cycle)))


    def _getActiveJobsWithActivePredecessors(self) -> Dict[str, BaseJobConfig]:
        """Active jobs, excluding any still inside their `refresh` window per memory."""

        now = time.time()
        jobs = {
            name: job for name, job in self.jobs.items()
            if job.active and not (self.memory and name in self.memory and job.refresh and (now - self.memory[name]) / 60 < job.refresh)
            }

        for name, job in jobs.items():
            self.activePredecessors[name] = [predecessor for predecessor in job.predecessors if predecessor in jobs]

        return jobs


    @property
    def finished(self) -> bool:

        return not self._notStarted and not self._running


    def takeReady(self) -> List[str]:
        """Jobs that may start now, marked as running.

        A job whose predecessor failed or was skipped is recorded as SKIPPED
        instead, and counts as unsuccessful itself, so the skip cascades down
        the graph -- in this same call, since a skip is decided without waiting
        on anything.
        """

        ready: List[str] = []
        changed = True

        while changed:
            changed = False
            for job in list(self._notStarted):
                predecessors = self.activePredecessors[job]
                unsuccessful = [predecessor for predecessor in predecessors if predecessor in self._unsuccessful]

                if unsuccessful:
                    self._skip(job, 'predecessor(s) did not complete: {}'.format(', '.join(unsuccessful)))
                    changed = True
                elif all(predecessor in self._completed for predecessor in predecessors):
                    self._notStarted.remove(job)
                    self._running.add(job)
                    ready.append(job)

        return ready


    def finish(self, outcome: JobOutcome) -> None:

        self._running.discard(outcome.job)
        self.outcomes.append(outcome)
        (self._completed if outcome.status == JobStatus.COMPLETED else self._unsuccessful).add(outcome.job)


    def skipNotStarted(self, reason: str) -> None:
        """Records every job that hasn't started as SKIPPED -- for a run that is
        shutting down and must not start anything new.
        """

        for job in list(self._notStarted):
            self._skip(job, reason)


    def _skip(self, job: str, reason: str) -> None:

        self._notStarted.remove(job)
        self.finish(JobOutcome(job=job, status=JobStatus.SKIPPED, error=reason))
