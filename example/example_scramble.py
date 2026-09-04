"""Loads + validates configuration, then hands it to the library -- no worker
function or process pool to write here.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import yaml

exampleDirectory = Path(__file__).resolve().parent
sys.path.append(str(exampleDirectory.parent))

from library import Configuration, ScrambleJobsFile, runScrambleJobs

fileName = Path(__file__).stem
databaseConfigurationPath = exampleDirectory / 'configuration' / 'database.yaml'
jobConfigurationPath = exampleDirectory / 'configuration' / 'scramble.yaml'
logDirectory = exampleDirectory / 'log' / (fileName + '.log')


def loadYaml(path: Path) -> Any:

    with open(path) as file:
        return yaml.load(file, Loader=yaml.FullLoader)


databaseConfiguration = Configuration.validateDatabaseConfiguration(loadYaml(databaseConfigurationPath))
jobsFile = Configuration.validateJobConfiguration(loadYaml(jobConfigurationPath), ScrambleJobsFile)
Configuration.validateJobGraph(jobsFile.jobs, databaseAliases=set(databaseConfiguration.keys()))

if __name__ == '__main__':
    runScrambleJobs(jobsFile=jobsFile, databaseConfiguration=databaseConfiguration, logDirectory=logDirectory, runForever=False)
