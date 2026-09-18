"""The reference vectors the Rust port is checked against.

mask-rs/vectors/reference.json records what the Python implementation produces.
A Rust build that disagrees with it is a silent key change, so the file is a
contract in both directions: this test fails if Python drifts from the vectors,
which means either the change was unintended, or masks really are changing and
the file should be regenerated deliberately -- as a release that says so.
"""
import json
import os
import subprocess
import sys

import pytest

pytest.importorskip('cryptography')

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GENERATOR = os.path.join(ROOT, 'mask-rs', 'generate_vectors.py')
VECTORS = os.path.join(ROOT, 'mask-rs', 'vectors', 'reference.json')


def test_the_vectors_still_describe_what_masking_produces():
    recorded = json.load(open(VECTORS))

    sys.path.insert(0, os.path.join(ROOT, 'mask-rs'))
    try:
        import generate_vectors
        regenerated = {
            'key': generate_vectors.KEY,
            'domain': generate_vectors.DOMAIN,
            'keyedHash': generate_vectors.hashVectors(),
            'strategies': generate_vectors.strategyVectors(),
            }
    finally:
        sys.path.remove(os.path.join(ROOT, 'mask-rs'))
        sys.modules.pop('generate_vectors', None)

    for section in ('key', 'domain'):
        assert recorded[section] == regenerated[section]

    for section in ('keyedHash', 'strategies'):
        for name, entries in regenerated[section].items():
            assert name in recorded[section], 'the generator grew {!r}; regenerate the vectors'.format(name)
            assert entries == recorded[section][name], (
                '{} vectors changed. If that was deliberate, every already-masked value has changed too: '
                'regenerate with python3 mask-rs/generate_vectors.py and say so in the release.'.format(name))


def test_the_generator_is_runnable_from_the_repository_root():
    """The header says to run it from the root, so a contributor regenerating
    the file after an intended change doesn't have to guess.
    """

    completed = subprocess.run([sys.executable, GENERATOR], cwd=ROOT, capture_output=True, text=True)

    assert completed.returncode == 0, completed.stderr
    assert 'vectors' in completed.stdout
