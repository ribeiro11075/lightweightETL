"""Keeps example/native-masking/demo.py from rotting: it runs the comparison on a
small table, and checks every run produced the same copy of its table -- all
eight runs where the Rust extension is installed, the two Python ones where it
isn't.
"""
import importlib.util
import os
import sqlite3
import sys
from pathlib import Path

import pytest

DEMO_PATH = Path(__file__).resolve().parents[1] / 'example' / 'native-masking' / 'demo.py'
PREVIOUS: dict = {}
ENVIRONMENT = ('NATIVE_DEMO_PRODUCTION_PATH', 'NATIVE_DEMO_STAGING_PATH', 'BAUTA_NATIVE', 'BAUTA_PIPELINE', 'BAUTA_MASKING_THREADS', 'MASKING_KEY')


@pytest.fixture(scope='module')
def demoRun(tmp_path_factory):
    specification = importlib.util.spec_from_file_location('native_masking_demo', DEMO_PATH)
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules['native_masking_demo'] = module
    previous = {name: os.environ.get(name) for name in ENVIRONMENT}
    PREVIOUS.update(previous)
    workingDirectory = tmp_path_factory.mktemp('native_masking_demo')

    try:
        specification.loader.exec_module(module)
        os.environ.pop('MASKING_KEY', None)
        yield module.main(workingDirectory=workingDirectory, rows=2000, wideRows=3000), workingDirectory
    finally:
        del sys.modules['native_masking_demo']
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def test_the_python_run_masks_every_row(demoRun):
    observed, workingDirectory = demoRun
    connection = sqlite3.connect(workingDirectory / 'staging-python-in-turn.db')

    try:
        assert connection.execute('SELECT count(*) FROM customers').fetchone()[0] == observed['rows'] == 2000
        assert connection.execute("SELECT count(*) FROM customers WHERE email LIKE '%@corp.example'").fetchone()[0] == 0
    finally:
        connection.close()


def test_every_run_makes_the_same_copy(demoRun):
    observed, _ = demoRun
    python = {'python-in-turn', 'python-overlapped'}

    assert observed['identical'] is True
    if observed['nativeVersion'] is None:
        assert set(observed['seconds']) == python
    else:
        assert set(observed['seconds']) == python | {'rust-in-turn', 'rust-overlapped', 'wide-rust-in-turn', 'wide-rust-overlapped',
                                                     'wide-rust-in-turn-cores', 'wide-rust-overlapped-cores'}


def test_the_demo_restores_the_variables_it_sets(demoRun):
    assert os.environ.get('BAUTA_PIPELINE') == PREVIOUS['BAUTA_PIPELINE']
    assert os.environ.get('BAUTA_NATIVE') == PREVIOUS['BAUTA_NATIVE']
    assert os.environ.get('BAUTA_MASKING_THREADS') == PREVIOUS['BAUTA_MASKING_THREADS']
