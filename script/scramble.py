import sys
import os
import multiprocessing as mp

sys.path.append('../library')
import configurationInterface as ci
import databaseInterface as di
import scrambleInterface as si
import workerInterface as wi
import logInterface as li

# define variables
fileName = os.path.basename(__file__).split('.')[0]
configurationInterface = ci.CONFIGURATION()
databaseConfiguration = configurationInterface.getDatabaseConfiguration()
jobConfiguration = configurationInterface.getJobConfiguration(job=fileName)
logDirectory = configurationInterface.getHistoryDirectory(historyType='log', fileName=fileName)
logInterface = li.LOG(logDirectory=logDirectory)

def worker(readyQueue, completedQueue, activeJobs):

    while True:

        # get job from ready queue
        job = readyQueue.get()
        logInterface.logging.info('Starting {}'.format(job))

        try:
            # define variables for job configuration
            database = activeJobs[job]['database']
            table = activeJobs[job]['table']
            defaultColumnValues = {} if activeJobs[job]['defaultColumnValues'] is None else {key: [i for i in item if i is not None] for key, item in activeJobs[job]['defaultColumnValues'].items() if item is not None}
            identifierColumns = [] if activeJobs[job]['identifierColumns'] is None else [i for i in activeJobs[job]['identifierColumns'] if i is not None]
            scrambleColumns = [] if activeJobs[job]['scrambleColumns'] is None else [i for i in activeJobs[job]['scrambleColumns'] if i is not None]
            randomColumns = [] if activeJobs[job]['randomColumns'] is None else [i for i in activeJobs[job]['randomColumns'] if i is not None]
            allDataRandom = activeJobs[job]['allDataRandom']
            randomSalt = 	activeJobs[job]['randomSalt']
            preTargetAdhocQueries = [] if activeJobs[job]['preTargetAdhocQueries'] is None else [i for i in activeJobs[job]['preTargetAdhocQueries'] if i is not None]
            postTargetAdhocQueries = [] if activeJobs[job]['postTargetAdhocQueries'] is None else [i for i in activeJobs[job]['postTargetAdhocQueries'] if i is not None]
            connectionSettings = databaseConfiguration[database]

			# iterate through pre adhoc queries to be run
			# run adhoc queries
			for preTargetAdhocQuery in preTargetAdhocQueries:
				databaseInterface = di.DATABASE(connectionSettings=connectionSettings)
				databaseInterface.alter(preTargetAdhocQuery)
				databaseInterface.close()

            # define dynamic SQL to get all data
            # get data, column names, and column types
            dataQuery = "select * from {}".format(table)
            databaseInterface = di.DATABASE(connectionSettings=connectionSettings)
            data = databaseInterface.query(query=dataQuery)
            columns = databaseInterface.getAllColumnNames(table=table)
            dataTypes = databaseInterface.getAllColumnTypes(table=table)
            databaseInterface.close()

            # check if any data exists
            if data:

                scrambleInterface = si.SCRAMBLE(job=job, data=data, columns=columns, dataTypes=dataTypes, defaultColumnValues=defaultColumnValues,
                                                            identifierColumns=identifierColumns, scrambleColumns=scrambleColumns, randomColumns=randomColumns,
                                                            allDataRandom=allDataRandom, randomSalt=randomSalt)

                # scramble data
                # define database interface connection configuration
                scrambleInterface.scramble()

				# truncate tables
                # insert data into target table
                databaseInterface = di.DATABASE(connectionSettings=connectionSettings)
                databaseInterface.truncate(table)
                databaseInterface.insert(table=table, data=scrambleInterface.dataScrambled, chunkSize=5000)
                databaseInterface.close()

                # iterate through post adhoc queries to be run
			    # run adhoc queries
			    for postTargetAdhocQuery in postTargetAdhocQueries:
				    databaseInterface = di.DATABASE(connectionSettings=connectionSettings)
				    databaseInterface.alter(postTargetAdhocQuery)
				    databaseInterface.close()

            status = True
            logInterface.logging.info('Completed {}'.format(job))

        except Exception as e:
            status = False
            error = str(e)
            logInterface.logging.error('Failed to complete {} due to error {}'.format(job, error))

        # place completed job in completed queue
        completedQueue.put({job: status})


if __name__ == '__main__':

    logInterface.logging.info('Initiated script/scramble.py to scramble tables')

    # create object to manage processes
    # create pool of processes
    workerInterface = wi.WORKER(jobs=jobConfiguration['jobs'])
    pool = mp.Pool(jobConfiguration['workers'], worker, (workerInterface.readyQueue, workerInterface.completedQueue, workerInterface.activeJobs))
    workerInterface.run()
    pool.close()

    logInterface.logging.info('Finished script/scramble.py to scramble tables')
