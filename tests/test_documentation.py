"""Keeps the documentation honest.

Prose is the one artifact nothing else checks, and the split into docs/ is
exactly where it rots: a heading renamed without its links, or a configuration
field added to a model without being written up. These are cheap to check, so
they're checked.
"""
import re
from pathlib import Path

import pytest

from lightweight_etl.cli import _buildParser
from lightweight_etl.configuration import Configuration, DatabaseConnectionConfig, DataJobConfig, DataJobsFile, MaskingConfig, expandEnvironmentVariables
from lightweight_etl.masking import STRATEGIES

ROOT = Path(__file__).resolve().parents[1]
DOCUMENTS = [ROOT / 'README.md', ROOT / 'example' / 'README.md'] + sorted((ROOT / 'docs').glob('*.md'))
CONFIGURATION_DOC = ROOT / 'docs' / 'configuration.md'


def _anchors(path: Path) -> set:
    """GitHub's heading slugs: lowercase, punctuation dropped, spaces to hyphens."""
    slugs = set()
    for line in path.read_text().splitlines():
        match = re.match(r'^#{1,6}\s+(.*)$', line)
        if match:
            slugs.add(re.sub(r'[^a-z0-9 _-]', '', match.group(1).strip().lower()).replace(' ', '-'))
    return slugs


def _internalLinks():
    for document in DOCUMENTS:
        for target, anchor in re.findall(r'\]\(([^)#\s]*)(?:#([^)\s]+))?\)', document.read_text()):
            if not target.startswith('http'):
                yield document, target, anchor


@pytest.mark.parametrize('document,target,anchor', list(_internalLinks()),
                         ids=lambda value: str(value.relative_to(ROOT)) if isinstance(value, Path) else value)
def test_every_internal_link_resolves(document, target, anchor):
    resolved = (document.parent / target).resolve() if target else document.resolve()

    assert resolved.exists(), '{} links to missing {}'.format(document.relative_to(ROOT), target)

    if anchor and resolved.suffix == '.md':
        assert anchor in _anchors(resolved), '{} links to missing heading #{} in {}'.format(
            document.relative_to(ROOT), anchor, resolved.relative_to(ROOT))


MASKING_DOC = ROOT / 'docs' / 'masking.md'


@pytest.mark.parametrize('model,document', [(DatabaseConnectionConfig, CONFIGURATION_DOC), (DataJobConfig, CONFIGURATION_DOC),
                                            (DataJobsFile, CONFIGURATION_DOC), (MaskingConfig, MASKING_DOC)],
                         ids=lambda value: getattr(value, '__name__', None) or value.name)
def test_every_configuration_field_is_documented(model, document):
    """A field added to a model without a line in the reference is the most
    common way a field reference goes stale, and the hardest to notice.
    """
    reference = document.read_text()

    undocumented = [name for name in model.model_fields if '`{}`'.format(name) not in reference]

    assert not undocumented, '{} field(s) missing from {}: {}'.format(model.__name__, document.name, ', '.join(undocumented))


def test_every_masking_strategy_and_option_is_documented():
    reference = MASKING_DOC.read_text()

    for name, strategy in STRATEGIES.items():
        assert '`{}`'.format(name) in reference, 'strategy {} is missing from docs/masking.md'.format(name)
        for option in strategy.OPTIONS:
            assert '`{}`'.format(option) in reference, 'option {} of {} is missing from docs/masking.md'.format(option, name)


def test_the_masked_job_in_the_masking_guide_is_valid_configuration(monkeypatch):
    import yaml

    monkeypatch.setenv('MASKING_KEY', 'a-documentation-masking-key')
    firstBlock = MASKING_DOC.read_text().split('```yaml\n', 1)[1].split('```', 1)[0]

    jobsFile = Configuration.validateJobConfiguration({'workers': 1, 'jobs': expandEnvironmentVariables(yaml.safe_load(firstBlock))}, DataJobsFile)

    assert jobsFile.jobs['maskCustomers'].masking.columns['notes'] == {'strategy': 'null'}


def test_every_documented_subcommand_exists():
    """The README lists the commands; the parser is what actually runs."""
    readme = (ROOT / 'README.md').read_text()
    documented = set(re.findall(r'^lightweight-etl ([a-z][a-z-]*)', readme, flags=re.MULTILINE))

    subparsers = next(action for action in _buildParser()._actions if action.dest == 'command')
    real = set(subparsers.choices)

    assert documented, 'the README documents no subcommands'
    assert documented == real, 'README documents {}, the CLI has {}'.format(sorted(documented), sorted(real))


def test_every_documented_flag_exists():
    readme = (ROOT / 'README.md').read_text()
    documented = set(re.findall(r'\| `(--[a-z-]+)', readme))

    real = set()
    for action in _buildParser()._actions:
        if action.dest == 'command':
            for subparser in action.choices.values():
                for option in subparser._actions:
                    real.update(option.option_strings)

    missing = documented - real

    assert documented
    assert not missing, 'README documents flags the CLI does not have: {}'.format(sorted(missing))
