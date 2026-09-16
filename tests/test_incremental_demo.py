"""Keeps example/incremental_demo.py from rotting.

The sample YAML config is validated by test_shipped_example_configuration.py;
this applies the same standard to the other half of example/. A showcase script
that silently breaks on a refactor is worse than no showcase -- you find out
when you run it in front of someone.

It's loaded by file path rather than imported, because example/ is deliberately
not a package: nothing in the library imports from it, so it carries no
__init__.py.
"""
import importlib.util
import os
import sqlite3
import sys
from pathlib import Path

import pytest

DEMO_PATH = Path(__file__).resolve().parents[1] / 'example' / 'incremental_demo.py'


@pytest.fixture(scope='module')
def demo():
    specification = importlib.util.spec_from_file_location('incremental_demo', DEMO_PATH)
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules['incremental_demo'] = module
    specification.loader.exec_module(module)

    yield module

    del sys.modules['incremental_demo']


@pytest.fixture(scope='module')
def demoRun(demo, tmp_path_factory):
    """Runs the demo once into a temporary directory, not the source tree.

    The demo sets DEMO_DB_PATH itself -- it's how its database.yaml finds the
    file -- so the previous value is restored afterwards rather than leaking a
    temporary path into every test that runs later.
    """
    workingDirectory = tmp_path_factory.mktemp('incremental_demo')
    previous = os.environ.get('DEMO_DB_PATH')

    try:
        yield demo.main(workingDirectory=workingDirectory), workingDirectory
    finally:
        if previous is None:
            os.environ.pop('DEMO_DB_PATH', None)
        else:
            os.environ['DEMO_DB_PATH'] = previous


def test_the_demo_runs_end_to_end(demoRun):
    watermarks, _ = demoRun

    assert len(watermarks) == 3


def test_the_watermark_advances_then_holds(demoRun):
    """Exactly what the script claims to show: the first run takes everything,
    the second takes only what is new, the third finds nothing and leaves the
    stored watermark where it is.
    """
    watermarks, _ = demoRun

    assert watermarks == ['2026-01-03T00:00:00', '2026-01-04T00:00:00', '2026-01-04T00:00:00']


def test_the_edited_row_is_not_re_extracted(demoRun):
    """The discriminator the demo is built around. Row 1's name changes in the
    source between runs without its updatedAt moving, so a full re-extract would
    pick the change up and an incremental one cannot. Row counts alone prove
    nothing here, since upsert is idempotent.
    """
    _, workingDirectory = demoRun

    connection = sqlite3.connect(str(workingDirectory / 'demo.db'))
    try:
        rows = dict(connection.execute('SELECT id, name FROM ordersTarget').fetchall())
    finally:
        connection.close()

    assert rows[1] == 'first'
    assert rows[4] == 'fourth'
    assert len(rows) == 4


def test_the_demo_loads_its_job_from_the_shipped_configuration(demo):
    """The point of moving it out of an inline dict: the demo exercises the same
    YAML-plus-environment path a real deployment does.
    """
    jobs = demo.loadConfiguration('jobs.yaml')

    assert jobs['jobs']['loadOrders']['watermarkColumn'] == 'updatedAt'


def test_the_demo_writes_only_inside_the_directory_it_is_given(demoRun):
    """It defaults to example/memory/, so a caller that supplies a directory must
    get everything there -- otherwise running the tests litters the source tree.
    """
    _, workingDirectory = demoRun

    assert {path.name for path in workingDirectory.iterdir()} == {'demo.db', 'memory.yaml', 'memory.yaml.lock', 'incremental.log'}
