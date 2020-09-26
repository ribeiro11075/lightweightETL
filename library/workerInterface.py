import multiprocessing as mp
import time


class WORKER():

	def __init__(self, jobs, previous=None):
		self.inProgressJobs = []
		self.completedJobs = []
		self.failedJobs = []
		self.readyQueue = mp.Queue()
		self.completedQueue = mp.Queue()
		self.jobs = jobs
		self.previous = previous

		self.activeJobs = self._getActiveJobsWithActivePredecessors()
		self.notStartedJobs = [key for key in self.activeJobs.keys()]


	def _getActiveJobsWithActivePredecessors(self):

		# create variable to capture jobs that should not be active based on refresh interval
		# identify active jobs
		deleteKeys = []
		jobs = {key: item for key, item in self.jobs.items() if item['active']}

		# iterate through jobs
		for key, item in jobs.items():

			# check that previous file exists, key exists in previous file, key exists in integration configuration, and key value in integration configuration is not None
			# calculate duration since last refresh
			if self.previous and key in self.previous and 'refresh' in item and item['refresh']:
				duration = (time.time() - self.previous[key]) / 60

				# check if duration is greater than refresh interval
				# capture jobs that should not be active
				if duration < item['refresh']:
					deleteKeys.append(key)

		# iterate through jobs that have been identified for removal
		# remove job from active jobs
		for key in deleteKeys:
			del jobs[key]

		# iterate through jobs
		for key, item in jobs.items():

			# add key to track active predecessors
			jobs[key]['activePredecessors'] = []

			# iterate through predecessors
			for predecessor in item['predecessors']:

				# check if predecessor exists and is active
				# add predeccessor to an active job
				if predecessor in jobs and jobs[predecessor]['active']:
					jobs[key]['activePredecessors'].append(predecessor)

		return jobs


	def _remainingActiveJobsCheck(self):

		# check if all active jobs exist in completed jobs
		if set(self.activeJobs.keys()) != set(self.completedJobs + self.failedJobs):
		    return True

		return False


	def _emptyCompletedQueue(self):

		# continue to loop until the completed queue tracker empty
		# add job to completed or failed tracker
		# remove job from in progress tracker
		while not self.completedQueue.empty():
		    completedStatus = self.completedQueue.get()
		    job = next(iter(completedStatus))
		    self.completedJobs.append(job) if completedStatus[job] else self.failedJobs.append(job)
		    self.inProgressJobs.remove(job)


	def _recalculateNotStartedJobs(self):

		# find remaining uncompleted jobs
		self.notStartedJobs = [job for job in self.notStartedJobs if job not in self.completedJobs + self.failedJobs + self.inProgressJobs]


	def _predecessorRequirementCheck(self, job):

		# check if predecessors do not exist or that all predecessors have been completed
		if not self.activeJobs[job]['activePredecessors'] or all(predecessor in self.completedJobs + self.failedJobs for predecessor in self.activeJobs[job]['activePredecessors']):
		    return False

		return True


	def _predecessorFailCheck(self, job):

		# check if any predecessor has failed
		if any(predecessor in self.failedJobs for predecessor in self.activeJobs[job]['activePredecessors']):
		    return True

		return False


	def run(self):

		# continue to loop until all jobs are completed
		while self._remainingActiveJobsCheck():

			# remove items from completed queues into designated buckets
			# find remaining uncompleted jobs
			self._emptyCompletedQueue()
			self._recalculateNotStartedJobs()

			# iterate through active jobs
			for job in self.notStartedJobs:

				# check if any predecessor has failed
				# create log interface object
				if self._predecessorFailCheck(job=job):
					self.failedJobs.append(job)

				# check if predecessors do not exist or that all predecessors have been completed
				# track jobs underway
				# place job into ready queue
				elif not self._predecessorRequirementCheck(job=job):
					self.inProgressJobs.append(job)
					self.readyQueue.put(job)

			# sleep to ensure program does not run at 100% utilization
			time.sleep(1)
