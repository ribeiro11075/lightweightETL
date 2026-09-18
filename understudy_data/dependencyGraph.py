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
    """What became of one job in one cycle. Crosses processes, so `error` is
    a string: driver exceptions don't reliably pickle.
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
    Bookkeeping only; it never waits. A cycle among the active jobs raises
    ConfigurationError.
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


    def takeReady(self, limit: Optional[int] = None) -> List[str]:
        """Jobs that may start now -- at most `limit` of them -- marked as running.
        A job whose predecessor failed or was skipped is marked SKIPPED, which
        cascades in this same call.
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
                elif (limit is None or len(ready) < limit) and all(predecessor in self._completed for predecessor in predecessors):
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
