import sys
import os
import multiprocessing as mp
import time

sys.path.append('../library')
import configurationInterface as ci
import databaseInterface as di
import workerInterface as wi
import transformInterface as ti
import previousInterface as pi
import logInterface as li

# define variables
fileName = os.path.basename(__file__).split('.')[0]
configurationInterface = ci.CONFIGURATION()
databaseConfiguration = configurationInterface.getDatabaseConfiguration()
jobConfiguration = configurationInterface.getJobConfiguration(job=fileName)
previousDirectory = configurationInterface.getHistoryDirectory(historyType='previous', fileName=fileName)
previousInterface = pi.PREVIOUS(previousDirectory=previousDirectory)
logDirectory = configurationInterface.getHistoryDirectory(historyType='log', fileName=fileName)
logInterface = li.LOG(logDirectory=logDirectory)


def worker(readyQueue, completedQueue, activeJobs):

	while True:

		# get job from ready queue
		job = readyQueue.get()
		logInterface.logging.info('Starting {}'.format(job))

		try:

			# assign variables for job from module settings
			sourceDatabase = activeJobs[job]['sourceDatabase']
			targetDatabase = activeJobs[job]['targetDatabase']
			insertStrategy = activeJobs[job]['insertStrategy']
			chunkSize = activeJobs[job]['chunkSize']
			targetTableStage = activeJobs[job]['targetTableStage']
			targetaTableFinal = activeJobs[job]['targetaTableFinal']
			columnTransforms = {} if activeJobs[job]['columnTransforms'] is None else {key: [i for i in item if i is not None] for key, item in activeJobs[job]['columnTransforms'].items() if item is not None}
			preTargetAdhocQueries = [] if activeJobs[job]['preTargetAdhocQueries'] is None else [i for i in activeJobs[job]['preTargetAdhocQueries'] if i is not None]
			postTargetAdhocQueries = [] if activeJobs[job]['postTargetAdhocQueries'] is None else [i for i in activeJobs[job]['postTargetAdhocQueries'] if i is not None]
			sourceQuery = activeJobs[job]['sourceQuery']
			sourceDatabaseConnectionSettings = databaseConfiguration[sourceDatabase]
			targetDatabaseConnectionSettings = databaseConfiguration[targetDatabase]

			# get data from source
			databaseInterface = di.DATABASE(connectionSettings=sourceDatabaseConnectionSettings)
			data = databaseInterface.query(query=sourceQuery)
			databaseInterface.close()

			# get column names
			databaseInterface = di.DATABASE(connectionSettings=targetDatabaseConnectionSettings)
			columns = databaseInterface.getAllColumnNames(table=targetaTableFinal)
			databaseInterface.close()

			# transform data
			transformInterface = ti.TRANSFORM(data=data, columns=columns, columnTransforms=columnTransforms)
			data = transformInterface.transform()

			# truncate staging table
			# insert data into staging table
			databaseInterface = di.DATABASE(connectionSettings=targetDatabaseConnectionSettings)
			databaseInterface.truncate(table=targetTableStage) if targetTableStage else None
			databaseInterface.insert(table=targetTableStage, data=data, chunkSize=chunkSize) if targetTableStage else None
			databaseInterface.close()

			# iterate through pre adhoc queries to be run
			# run adhoc queries
			for preTargetAdhocQuery in preTargetAdhocQueries:
				databaseInterface = di.DATABASE(connectionSettings=targetDatabaseConnectionSettings)
				databaseInterface.alter(preTargetAdhocQuery)
				databaseInterface.close()

			# check if target table should be entirely replaced
			# swap staging table with target table
			if insertStrategy == 'swap':
				databaseInterface = di.DATABASE(connectionSettings=targetDatabaseConnectionSettings)
				databaseInterface.swap(targetTable=targetaTableFinal, stageTable=targetTableStage)
				databaseInterface.close()

			# check if target table should be appended to
			# append data from stagin table to target table
			if insertStrategy == 'upsert':
				databaseInterface = di.DATABASE(connectionSettings=targetDatabaseConnectionSettings)
				databaseInterface.upsert(table=targetaTableFinal, data=data, chunkSize=chunkSize) if not targetTableStage else databaseInterface.upsertFromStage(targetTable=targetaTableFinal, stageTable=targetTableStage)
				databaseInterface.close()

			# iterate through post adhoc queries to be run
			# run adhoc queries
			for postTargetAdhocQuery in postTargetAdhocQueries:
				databaseInterface = di.DATABASE(connectionSettings=targetDatabaseConnectionSettings)
				databaseInterface.alter(postTargetAdhocQuery)
				databaseInterface.close()

			status = True
			logInterface.logging.info('Completed {}'.format(job))

		except Exception as e:
			status = False
			error = str(e)
			logInterface.logging.error('Failed to complete {} due to error {}'.format(job, error))

		# track completed jobs
		completedQueue.put({job: status})
		previousInterface.addPrevious(job=job)


if __name__ == '__main__':

	logInterface.logging.info('Initiated script/data.py')

	while True:
		
		databaseConfiguration = configurationInterface.getDatabaseConfiguration()
		jobConfiguration = configurationInterface.getJobConfiguration(job=fileName)
		previousDirectory = configurationInterface.getHistoryDirectory(historyType='previous', fileName=fileName)

		# get previous
		previousInterface = pi.PREVIOUS(previousDirectory=previousDirectory)
		previous = previousInterface.previous

		# create object to manage processes
		# create pool of processes
		workerInterface = wi.WORKER(jobs=jobConfiguration['jobs'], previous=previous)
		pool = mp.Pool(jobConfiguration['workers'], worker, (workerInterface.readyQueue, workerInterface.completedQueue, workerInterface.activeJobs))
		workerInterface.run()

		# sleep to ensure program does not run at 100% utilization
		pool.close()
		time.sleep(.5)
