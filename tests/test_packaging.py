"""Keeps pyproject.toml's dependency ranges and the files that pin them in step.

The package takes ranges, so it can install beside other tools. CI tests the
bottom of each range with constraints/lowest.txt, and a pinned set near the
top with constraints/image.txt; either drifting from pyproject.toml would make
that testing describe something nobody installs.
"""
import sys
from pathlib import Path

import pytest
from packaging.requirements import Requirement  # installed with `build`, a dev dependency
from packaging.version import Version

if sys.version_info >= (3, 11):
    import tomllib
else:
    tomllib = pytest.importorskip('tomli')

ROOT = Path(__file__).resolve().parents[1]
PROJECT = tomllib.loads((ROOT / 'pyproject.toml').read_text())['project']


def _requirements():
    """Runtime and driver requirements -- everything but the dev tools and the
    extras that only name other extras.
    """
    extras = PROJECT['optional-dependencies']
    texts = PROJECT['dependencies'] + [text for name, group in extras.items() if name != 'dev' for text in group]
    requirements = {}
    for text in texts:
        requirement = Requirement(text)
        if requirement.name != 'bauta':
            requirements[requirement.name] = requirement
    return requirements


def _pins(name):
    pins = {}
    for line in (ROOT / 'constraints' / name).read_text().splitlines():
        line = line.split('#')[0].strip()
        if line:
            requirement = Requirement(line)
            (specifier,) = requirement.specifier
            assert specifier.operator == '==', line
            pins[requirement.name] = Version(specifier.version)
    return pins


REQUIREMENTS = _requirements()


@pytest.mark.parametrize('name', sorted(REQUIREMENTS))
def test_requirements_are_ranges_with_a_lower_bound(name):
    operators = {specifier.operator for specifier in REQUIREMENTS[name].specifier}

    assert '>=' in operators, '{} needs a lower bound'.format(REQUIREMENTS[name])
    assert not operators & {'==', '~=', '==='}, '{} is pinned; pin in constraints/ instead'.format(REQUIREMENTS[name])


def test_the_lowest_constraints_are_exactly_the_lower_bounds():
    lowest = {name: Version(next(specifier.version for specifier in requirement.specifier if specifier.operator == '>='))
              for name, requirement in REQUIREMENTS.items()}

    assert _pins('lowest.txt') == lowest


def test_the_image_pins_everything_within_the_ranges():
    pins = _pins('image.txt')

    for name, requirement in REQUIREMENTS.items():
        assert name in pins, '{} is not pinned in image.txt'.format(name)
        assert requirement.specifier.contains(pins[name]), '{}=={} is outside {}'.format(name, pins[name], requirement)
