"""The example YAML under example/configuration/ is documentation that can rot.

Running example/example_jobs.py used to validate it as a side effect; that
script is gone now that the CLI is the supported entry point, so this takes over
the job. It keeps the README's field reference honest -- a field renamed in
configuration.py without the sample being updated fails here rather than in
somebody's first five minutes with the tool.
"""
from pathlib import Path

import pytest
import yaml

from lightweight_etl.configuration import Configuration, DataJobsFile, ScrambleJobsFile
from lightweight_etl.transform import resolveTransformer

CONFIGURATION_DIRECTORY = Path(__file__).resolve().parents[1] / 'example' / 'configuration'


def _load(name: str):
    with open(CONFIGURATION_DIRECTORY / name) as file:
        return yaml.load(file, Loader=yaml.FullLoader)


@pytest.fixture
def databaseAliases():
    return set(Configuration.validateDatabaseConfiguration(_load('database.yaml')))


def test_the_shipped_database_configuration_validates(databaseAliases):
    assert databaseAliases


def test_the_shipped_jobs_configuration_validates(databaseAliases):
    jobsFile = Configuration.validateJobConfiguration(_load('jobs.yaml'), DataJobsFile)
    Configuration.validateJobGraph(jobsFile.jobs, databaseAliases=databaseAliases)

    assert jobsFile.jobs


def test_the_shipped_scramble_configuration_validates():
    jobsFile = Configuration.validateJobConfiguration(_load('scramble.yaml'), ScrambleJobsFile)
    Configuration.validateJobGraph(jobsFile.jobs)

    assert jobsFile.jobs


def test_every_transformer_the_sample_references_actually_resolves():
    """The sample points at lightweight_etl.builtinTransforms now that those ship
    with the package -- so a rename there breaks this rather than a user's config.
    """
    jobsFile = Configuration.validateJobConfiguration(_load('jobs.yaml'), DataJobsFile)
    references = [reference for job in jobsFile.jobs.values() for references in job.sourceQueryColumnTransforms.values() for reference in references]

    assert references

    for reference in references:
        assert callable(resolveTransformer(reference)), reference
