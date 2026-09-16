from __future__ import annotations

import multiprocessing as mp
import time
from enum import Enum
from typing import Any, Dict, List, Mapping, NamedTuple, Optional

from .configuration import BaseJobConfig


class JobStatus(str, Enum):
    NOT_STARTED = 'not_started'
    IN_PROGRESS = 'in_progress'
    COMPLETED = 'completed'
    FAILED = 'failed'
    SKIPPED = 'skipped'


class JobOutcome(NamedTuple):
    """What became of one job in one cycle.

    This is what workers put on completedQueue, so it crosses a process
    boundary and every field has to pickle. That's why `error` is a formatted
    string rather than the exception: database drivers raise exception types
    that don't reliably survive pickling, and losing the whole outcome to a
    pickling error while reporting a failure would be a poor trade.

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

    @property
    def durationSeconds(self) -> float:

        return max(0.0, self.finishedAt - self.startedAt)


class DependencyGraph:

    def __init__(self, jobs: Mapping[str, BaseJobConfig], memory: Optional[Dict[str, float]] = None) -> None:
        self.inProgressJobs: List[str] = []
        self.completedJobs: List[str] = []
        self.failedJobs: List[str] = []
        self.outcomes: List[JobOutcome] = []
        self.readyQueue: mp.Queue = mp.Queue()
        self.completedQueue: mp.Queue = mp.Queue()
        self.jobs = jobs
        self.memory = memory
        self.activePredecessors: Dict[str, List[str]] = {}
        self.activeJobs = self._getActiveJobsWithActivePredecessors()
        self.notStartedJobs = list(self.activeJobs.keys())


    def _getActiveJobsWithActivePredecessors(self) -> Dict[str, BaseJobConfig]:
        """Active jobs, excluding any still inside their `refresh` window per memory."""
        deleteKeys = []
        jobs = {key: item for key, item in self.jobs.items() if item.active}

        for key, item in jobs.items():

            if self.memory and key in self.memory and item.refresh:
                duration = (time.time() - self.memory[key]) / 60

                if duration < item.refresh:
                    deleteKeys.append(key)

        for key in deleteKeys:
            del jobs[key]

        for key, item in jobs.items():

            self.activePredecessors[key] = [predecessor for predecessor in item.predecessors if predecessor in jobs and jobs[predecessor].active]

        return jobs


    def _remainingActiveJobsCheck(self) -> bool:

        if set(self.activeJobs.keys()) != set(self.completedJobs + self.failedJobs):
            return True

        return False


    def _emptyCompletedQueue(self) -> None:
        """failedJobs collects every terminal non-success, SKIPPED included, because
        that list is what _isJobReady and _predecessorFailCheck read to decide
        whether a downstream job can start -- a job whose predecessor was itself
        skipped must not run either. The distinction between the two is kept in
        `outcomes`, which is what gets reported rather than scheduled on.
        """

        while not self.completedQueue.empty():
            outcome = self.completedQueue.get()
            self.outcomes.append(outcome)

            if outcome.status == JobStatus.COMPLETED:
                self.completedJobs.append(outcome.job)
            else:
                self.failedJobs.append(outcome.job)

            self.inProgressJobs.remove(outcome.job)


    def _recalculateNotStartedJobs(self) -> None:
        self.notStartedJobs = [job for job in self.notStartedJobs if job not in self.completedJobs + self.failedJobs + self.inProgressJobs]


    def _isJobReady(self, job: str) -> bool:
        return not self.activePredecessors[job] or all(predecessor in self.completedJobs + self.failedJobs for predecessor in self.activePredecessors[job])


    def _predecessorFailCheck(self, job: str) -> bool:
        return any(predecessor in self.failedJobs for predecessor in self.activePredecessors[job])


    def run(self) -> None:
        """Blocks until every active job has completed or failed, polling once a
        second so this doesn't spin at 100% utilization.
        """

        while self._remainingActiveJobsCheck():

            self._emptyCompletedQueue()
            self._recalculateNotStartedJobs()

            for job in self.notStartedJobs:

                if self._predecessorFailCheck(job=job):
                    failedPredecessors = [predecessor for predecessor in self.activePredecessors[job] if predecessor in self.failedJobs]
                    self.failedJobs.append(job)
                    self.outcomes.append(JobOutcome(
                        job=job, status=JobStatus.SKIPPED,
                        error='predecessor(s) did not complete: {}'.format(', '.join(failedPredecessors))))

                elif self._isJobReady(job=job):
                    self.inProgressJobs.append(job)
                    self.readyQueue.put(job)

            time.sleep(1)
