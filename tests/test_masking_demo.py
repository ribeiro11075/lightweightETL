"""Keeps example/masking_demo.py from rotting, as test_incremental_demo.py does
for the other demo: it runs the script into a temporary directory and checks
that each thing it claims to show actually happened.
"""
import importlib.util
import os
import sys
from pathlib import Path

import pytest

from understudy_data.dependencyGraph import JobStatus

DEMO_PATH = Path(__file__).resolve().parents[1] / 'example' / 'masking_demo.py'
ENVIRONMENT = ('MASKING_DEMO_PROD_PATH', 'MASKING_DEMO_STAGING_PATH', 'MASKING_KEY')


@pytest.fixture(scope='module')
def demoRun(tmp_path_factory):
    """Runs the demo once. The variables it sets are restored afterwards, so a
    temporary path or the demo key never leaks into a later test.
    """
    specification = importlib.util.spec_from_file_location('masking_demo', DEMO_PATH)
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules['masking_demo'] = module
    previous = {name: os.environ.get(name) for name in ENVIRONMENT}
    workingDirectory = tmp_path_factory.mktemp('masking_demo')

    try:
        specification.loader.exec_module(module)
        os.environ.pop('MASKING_KEY', None)
        yield module.main(workingDirectory=workingDirectory), workingDirectory
    finally:
        del sys.modules['masking_demo']
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def test_the_first_run_masks_and_keeps_joins(demoRun):
    observed, _ = demoRun

    assert observed['firstRun'] is True
    assert observed['joinedOrders'] == 7


def test_the_manifest_is_written_without_the_key(demoRun):
    observed, workingDirectory = demoRun

    manifest = (workingDirectory / 'manifest.json').read_text()

    assert [job['job'] for job in observed['manifest']['jobs']] == ['maskCustomers', 'maskOrders']
    assert 'masking-demo-key-not-for-real-use' not in manifest


def test_an_uncovered_column_stops_the_second_run_before_it_writes(demoRun):
    observed, _ = demoRun

    failure = observed['secondRun']
    assert failure.status == JobStatus.FAILED
    assert 'ssn' in failure.error
    assert observed['stagingCustomersAfterFailure'] == 5


def test_discovery_proposes_a_policy_for_the_new_column(demoRun):
    observed, _ = demoRun

    assert observed['proposal']['ssn'] == {'strategy': 'key'}
    assert observed['proposal']['email'] == {'strategy': 'email'}


def test_the_subset_follows_the_foreign_key(demoRun):
    observed, _ = demoRun

    assert observed['subset'] == {'customers': 3, 'orders': 5}


def test_the_demo_writes_only_inside_the_directory_it_is_given(demoRun):
    _, workingDirectory = demoRun

    assert {path.name for path in workingDirectory.iterdir()} == {'prod.db', 'staging.db', 'memory.yaml', 'memory.yaml.lock', 'masking.log', 'manifest.json'}
