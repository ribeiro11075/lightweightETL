from __future__ import annotations

import multiprocessing as mp
import time
from enum import Enum
from typing import Dict, List, Mapping, Optional

from .configurationInterface import BaseJobConfig


class JobStatus(str, Enum):
    NOT_STARTED = 'not_started'
    IN_PROGRESS = 'in_progress'
    COMPLETED = 'completed'
    FAILED = 'failed'


class DependencyGraph:

    def __init__(self, jobs: Mapping[str, BaseJobConfig], memory: Optional[Dict[str, float]] = None) -> None:
        self.inProgressJobs: List[str] = []
        self.completedJobs: List[str] = []
        self.failedJobs: List[str] = []
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

        while not self.completedQueue.empty():
            completedStatus = self.completedQueue.get()
            job = next(iter(completedStatus))
            self.completedJobs.append(job) if completedStatus[job] == JobStatus.COMPLETED else self.failedJobs.append(job)
            self.inProgressJobs.remove(job)


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
                    self.failedJobs.append(job)

                elif self._isJobReady(job=job):
                    self.inProgressJobs.append(job)
                    self.readyQueue.put(job)

            time.sleep(1)
