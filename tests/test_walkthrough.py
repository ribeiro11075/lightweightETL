"""Keeps example/walkthrough/demo.py -- the end-to-end demonstration -- honest: it
runs the whole session and checks each thing it shows actually happened.
"""
import importlib.util
import os
import re
import sys
from pathlib import Path

import pytest

WALKTHROUGH_PATH = Path(__file__).resolve().parents[1] / 'example' / 'walkthrough' / 'demo.py'
ENVIRONMENT = ('MASKING_KEY', 'UNDERSTUDY_MANIFEST_KEY')


@pytest.fixture(scope='module')
def session(tmp_path_factory):
    specification = importlib.util.spec_from_file_location('walkthrough', WALKTHROUGH_PATH)
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules['walkthrough'] = module
    previous = {name: os.environ.get(name) for name in ENVIRONMENT}
    workingDirectory = tmp_path_factory.mktemp('walkthrough')

    try:
        for name in ENVIRONMENT:
            os.environ.pop(name, None)
        specification.loader.exec_module(module)
        yield module.main(workingDirectory=workingDirectory), workingDirectory
    finally:
        del sys.modules['walkthrough']
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def test_every_command_succeeds(session):
    observed, _ = session

    assert observed['exitCodes'] == {step: 0 for step in ('discover', 'subset', 'schema', 'audit', 'run', 'verify-manifest', 'synthesize',
                                                          'history')}


def test_the_copy_is_the_subset_and_referentially_complete(session):
    observed, _ = session

    assert observed['subsetCustomers'] == observed['expectedCustomers'] == 24
    assert observed['foreignKeyViolations'] == []
    assert observed['ordersJoined'] == observed['orders'] > 0


def test_nothing_identifying_reaches_staging(session):
    observed, _ = session
    productionTickets = ' '.join(observed['productionTickets'])
    phones = set(re.findall(r'\+351 9\d\d \d{3} \d{3}', productionTickets))
    emails = set(re.findall(r'\S+@mail\.example', productionTickets))

    assert observed['productionEmailsInStaging'] == 0
    assert observed['stagingIds'] != observed['productionIds']
    assert phones and emails
    assert not any(phone in ticket or email.rstrip('.') in ticket for ticket in observed['stagingTickets'] for phone in phones for email in emails)


def test_payment_cards_are_synthetic(session):
    observed, _ = session

    generated = observed['stagingCards'] - {None}

    assert observed['syntheticCards'] == 40
    assert len(generated) > 30
    assert not generated & observed['productionCards']


def test_the_session_is_written_down_with_its_limitation(session):
    _, workingDirectory = session
    report = (workingDirectory / 'walkthrough.md').read_text()

    assert '$ understudy subset --databases ' in report
    assert 'intact, and signed with key' in report
    assert 'the name is still there' in report
    assert str(workingDirectory) not in report.split('This session was written to')[0]
