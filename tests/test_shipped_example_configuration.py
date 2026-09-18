"""The example YAML under example/ is documentation that can rot.

This keeps it honest: a field renamed in configuration.py without the samples
being updated fails here rather than in somebody's first five minutes with the
tool. The starter configuration is what people copy; each demo's configuration
is what its script runs.
"""
from pathlib import Path

import pytest
import yaml

from understudy_data.configuration import Configuration, ConfigurationError, DataJobsFile, expandEnvironmentVariables
from understudy_data.transform import resolveTransformer

EXAMPLE_DIRECTORY = Path(__file__).resolve().parents[1] / 'example'
CONFIGURATION_DIRECTORY = EXAMPLE_DIRECTORY / 'starter' / 'configuration'
DEMO_CONFIGURATION_DIRECTORIES = sorted(path for path in EXAMPLE_DIRECTORY.glob('*/configuration') if path != CONFIGURATION_DIRECTORY)

# The sample reads credentials from the environment, so validating it means
# supplying them the way a deployment would. This list doubles as a check that
# the documented variable names don't drift away from the file.
SAMPLE_SECRETS = {'SOURCE_DB_PASSWORD': 'sourceSecret', 'TARGET_DB_PASSWORD': 'targetSecret', 'MASKING_KEY': 'a-sample-masking-key'}


@pytest.fixture(autouse=True)
def sampleSecrets(monkeypatch):
    for name, value in SAMPLE_SECRETS.items():
        monkeypatch.setenv(name, value)


def _load(name: str):
    with open(CONFIGURATION_DIRECTORY / name) as file:
        return expandEnvironmentVariables(yaml.load(file, Loader=yaml.FullLoader))


@pytest.fixture
def databaseAliases():
    return set(Configuration.validateDatabaseConfiguration(_load('database.yaml')))


def test_the_shipped_database_configuration_validates(databaseAliases):
    assert databaseAliases


def test_the_shipped_jobs_configuration_validates(databaseAliases):
    jobsFile = Configuration.validateJobConfiguration(_load('jobs.yaml'), DataJobsFile)
    Configuration.validateJobGraph(jobsFile.jobs, databaseAliases=databaseAliases)

    assert jobsFile.jobs
    assert jobsFile.memory == '../transaction/memory.yaml', 'the starter keeps run state out of its configuration'


def test_the_sample_masked_jobs_share_a_domain_and_read_their_key_from_the_environment(databaseAliases):
    jobsFile = Configuration.validateJobConfiguration(_load('jobs.yaml'), DataJobsFile)
    customers = jobsFile.jobs['loadCustomersMasked'].masking
    orders = jobsFile.jobs['loadOrdersMasked'].masking

    assert customers.columns['id']['domain'] == orders.columns['customerId']['domain']

    raw = yaml.load(open(CONFIGURATION_DIRECTORY / 'jobs.yaml'), Loader=yaml.FullLoader)
    keys = [job['masking']['key'] for job in raw['jobs'].values() if job.get('masking')]

    assert keys
    for key in keys:
        assert key.startswith('${') and ':-' not in key, 'a masking key belongs in the environment, with no default: {}'.format(key)


def test_every_transformer_the_sample_references_actually_resolves():
    """The sample points at understudy_data.builtinTransforms now that those ship
    with the package -- so a rename there breaks this rather than a user's config.
    """
    jobsFile = Configuration.validateJobConfiguration(_load('jobs.yaml'), DataJobsFile)
    references = [reference for job in jobsFile.jobs.values() for references in job.sourceQueryColumnTransforms.values() for reference in references]

    assert references

    for reference in references:
        assert callable(resolveTransformer(reference)), reference


def test_the_sample_reads_every_password_from_the_environment():
    """People copy examples far more reliably than they read field references, so
    a literal password here quietly undoes the feature. This fails if anyone
    "simplifies" the sample back to plaintext.
    """
    raw = yaml.load(open(CONFIGURATION_DIRECTORY / 'database.yaml'), Loader=yaml.FullLoader)

    passwords = [alias['password'] for alias in raw.values() if alias.get('password') is not None]

    assert passwords
    for password in passwords:
        assert password.startswith('${') and password.endswith('}'), password
        assert ':-' not in password, 'a default password puts the credential back in the file: {}'.format(password)


def test_the_sample_refuses_to_load_when_its_secrets_are_absent(monkeypatch):
    """The safety property, demonstrated on the shipped file rather than a
    synthetic one: no secret in the environment means no run, not an empty
    password handed to the driver.
    """
    for name in SAMPLE_SECRETS:
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(ConfigurationError, match='SOURCE_DB_PASSWORD'):
        _load('database.yaml')


def test_every_demo_has_its_configuration():
    assert {path.parent.name for path in DEMO_CONFIGURATION_DIRECTORIES} == {'incremental', 'masking', 'native-masking', 'walkthrough'}


@pytest.mark.parametrize('demoDirectory', DEMO_CONFIGURATION_DIRECTORIES, ids=lambda path: path.parent.name)
def test_each_demo_configuration_validates(demoDirectory):
    """Each demo's configuration/ is laid out as `--config DIR` expects. The
    walkthrough ships only database.yaml, since `subset` generates its jobs.
    """
    with open(demoDirectory / 'database.yaml') as file:
        databases = Configuration.validateDatabaseConfiguration(expandEnvironmentVariables(yaml.load(file, Loader=yaml.FullLoader)))

    if demoDirectory.parent.name == 'walkthrough':
        assert not (demoDirectory / 'jobs.yaml').exists()
        return

    with open(demoDirectory / 'jobs.yaml') as file:
        jobsFile = Configuration.validateJobConfiguration(expandEnvironmentVariables(yaml.load(file, Loader=yaml.FullLoader)), DataJobsFile)

    Configuration.validateJobGraph(jobsFile.jobs, databaseAliases=set(databases))

    assert jobsFile.jobs
    if 'masking' in demoDirectory.parent.name:
        assert all(job.masking is not None for job in jobsFile.jobs.values())
