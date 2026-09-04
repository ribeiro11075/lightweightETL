import pickle

from library.configurationInterface import Configuration, DataJobsFile, ScrambleJobsFile
from library.runner import _dataJobWorker, _scrambleJobWorker, runDataJobs, runScrambleJobs


def test_worker_functions_are_picklable():
    """multiprocessing's spawn start method (macOS/Windows default) unpickles the
    Pool initializer by module+qualname in the child process -- a closure or
    nested function wouldn't survive that. This is the trickiest part of moving
    the worker functions into a library module rather than the entry script.
    """
    pickle.dumps(_dataJobWorker)
    pickle.dumps(_scrambleJobWorker)


def test_run_scramble_jobs_completes_with_zero_active_jobs(tmp_path):
    """Exercises a real Pool spawn + teardown without any network I/O, since no
    job is ever active enough to be pulled off the queue.
    """
    raw = {'workers': 2, 'jobs': {'noop': {'active': False, 'database': 'x', 'table': 'y', 'randomSalt': 's'}}}
    jobsFile = Configuration.validateJobConfiguration(raw, ScrambleJobsFile)

    runScrambleJobs(jobsFile=jobsFile, databaseConfiguration={}, logDirectory=tmp_path / 'runner.log', runForever=False)


def test_run_data_jobs_completes_with_zero_active_jobs_when_not_forever(tmp_path):
    raw = {'workers': 2, 'jobs': {'noop': {'active': False, 'sourceDatabase': 'x', 'targetDatabase': 'y', 'insertStrategy': 'upsert',
                                            'chunkSize': 1, 'targetTableFinal': 't', 'sourceQuery': 'select 1'}}}
    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)

    runDataJobs(jobsFile=jobsFile, databaseConfiguration={}, logDirectory=tmp_path / 'runner.log',
                memoryDirectory=tmp_path / 'runner.yaml', runForever=False)
