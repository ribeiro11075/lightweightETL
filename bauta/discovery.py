"""Proposes masking policies from a live schema, for `bauta discover`.

A proposal is for review: each suggestion carries its reason, rendered beside
the column. Classification uses column names, then sampled values, which are
only examined in memory -- never printed, logged or written.
"""
from __future__ import annotations

import datetime
import decimal
import re
from typing import Any, Callable, Dict, FrozenSet, Iterable, List, Mapping, NamedTuple, Optional, Sequence, Tuple

import yaml

from . import builtinDiscovery
from .builtinDiscovery import isIsoDate
from .configuration import DiscoveryRulesFile
from .databaseDialects import ColumnCategory, ForeignKey

DEFAULT_SAMPLE_SIZE = 1000

# A share of sampled values that must match a rule before it's trusted.
VALUE_MATCH_THRESHOLD = 0.8

FREE_TEXT_AVERAGE_LENGTH = 60

DISCOVERY_FILE = 'discovery.yaml'


class NameRule(NamedTuple):
    """Column-name words, any of which suggests `policy`. `name` is a built-in
    rule's, for leaving it out; None for one of your own.
    """

    name: Optional[str]
    words: FrozenSet[str]
    policy: Dict[str, Any]
    reason: str


class ValueRule(NamedTuple):
    """A test enough sampled values must pass for `policy` to apply."""

    name: Optional[str]
    test: Callable[[str], bool]
    policy: Dict[str, Any]
    reason: str


class DiscoveryRules(NamedTuple):
    """The rules in the order they're tried: your own, then the built-in ones
    left in. The first that matches wins.
    """

    names: Tuple[NameRule, ...]
    values: Tuple[ValueRule, ...]
    personalTables: FrozenSet[str]

    def matchName(self, column: str) -> Optional[NameRule]:

        words = nameWords(column)

        return next((rule for rule in self.names if words & rule.words), None)


def nameWords(name: str) -> FrozenSet[str]:
    """A name's words, lower-cased, plus the whole name run together:
    `first_name`, `firstName` and `FIRSTNAME` all give `firstname`.
    """

    split = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
    words = [word for word in re.split(r'[^a-z0-9]+', split.lower()) if word]

    return frozenset(words) | {''.join(words)}


def ruleWord(word: str) -> str:
    """A rule's word as names are matched against it: run together, so a rule
    written `first_name` matches a `FirstName` column too.
    """

    return max(nameWords(word), key=len)


BUILTIN_RULES = DiscoveryRules(
    names=tuple(NameRule(name, frozenset(words), policy, reason) for name, words, policy, reason in builtinDiscovery.NAME_RULES),
    values=tuple(ValueRule(name, test, policy, reason) for name, test, policy, reason in builtinDiscovery.VALUE_RULES),
    personalTables=builtinDiscovery.PERSONAL_TABLE_WORDS,
    )


def _fullMatch(pattern: str) -> Callable[[str], bool]:

    compiled = re.compile(pattern)

    return lambda text: compiled.fullmatch(text) is not None


def discoveryRules(rulesFile: Optional[DiscoveryRulesFile] = None) -> DiscoveryRules:
    """The rules to discover with: a discovery.yaml's own, ahead of the
    built-in ones it leaves in. The built-in ones alone without a file.
    """

    if rulesFile is None:
        return BUILTIN_RULES

    excluded = set(rulesFile.exclude)
    keeps = (lambda name: name not in excluded) if rulesFile.builtins else (lambda name: False)

    return DiscoveryRules(
        names=tuple(NameRule(None, frozenset(ruleWord(word) for word in rule.words), dict(rule.policy),
                             rule.reason or 'name matches a rule in {}'.format(DISCOVERY_FILE)) for rule in rulesFile.names)
              + tuple(rule for rule in BUILTIN_RULES.names if keeps(rule.name)),
        values=tuple(ValueRule(None, _fullMatch(rule.pattern), dict(rule.policy),
                               rule.reason or 'sampled values match /{}/ from {}'.format(rule.pattern, DISCOVERY_FILE))
                     for rule in rulesFile.values)
               + tuple(rule for rule in BUILTIN_RULES.values if keeps(rule.name)),
        personalTables=frozenset(ruleWord(word) for word in rulesFile.personalTables)
                       | (BUILTIN_RULES.personalTables if rulesFile.builtins else frozenset()),
        )


class Suggestion(NamedTuple):

    column: str
    policy: Dict[str, Any]
    reason: str


class TableProposal(NamedTuple):

    table: str
    columns: List[Suggestion]


def personalDataHint(column: str, rules: DiscoveryRules = BUILTIN_RULES) -> Optional[str]:
    """Why a column's name alone suggests personal data, or None.

    The same name rules discovery proposes policies from, without the sampled
    values -- what `audit` uses to question a column that is kept as it is. A
    rule of your own that says `keep` answers None.
    """

    rule = rules.matchName(column)

    return rule.reason if rule is not None and rule.policy['strategy'] != 'keep' else None


def _inferCategory(values: Sequence[Any]) -> Optional[ColumnCategory]:
    """For drivers that don't report column types -- SQLite reports none."""

    present = [value for value in values if value is not None]

    if not present:
        return None
    if all(isinstance(value, (int, float, decimal.Decimal)) and not isinstance(value, bool) for value in present):
        return ColumnCategory.NUMBER
    if all(isinstance(value, datetime.date) for value in present):
        return ColumnCategory.DATE
    if all(isinstance(value, str) for value in present):
        return ColumnCategory.TEXT

    return None


def _share(values: Sequence[str], test: Callable[[str], bool]) -> float:

    return sum(1 for value in values if test(value)) / len(values) if values else 0.0


def _classifyValues(values: Sequence[Any], category: Optional[ColumnCategory], rules: DiscoveryRules) -> Optional[Tuple[Dict[str, Any], str]]:
    """What sampled values look like, if they look like anything in particular.

    Built-in rules read text columns only. Your own read integers too, as
    their digits -- a tax number is often stored as one -- where the policy
    suits the column.
    """

    present = [value for value in values if value is not None]
    if not present:
        return None

    texts = [value.strip() for value in present] if all(isinstance(value, str) for value in present) else []
    digits = [str(value) for value in present] if all(isinstance(value, (str, int)) and not isinstance(value, bool) for value in present) else []

    for rule in rules.values:
        if rule.name is None:
            candidates = [text.strip() for text in digits]
            if candidates and _share(candidates, rule.test) >= VALUE_MATCH_THRESHOLD and _compatible(rule.policy['strategy'], values, category):
                return dict(rule.policy), rule.reason
        elif texts and _share(texts, rule.test) >= VALUE_MATCH_THRESHOLD:
            return dict(rule.policy), rule.reason

    if texts and sum(len(text) for text in present) / len(present) > FREE_TEXT_AVERAGE_LENGTH:
        return {'strategy': 'null'}, 'sampled values are long free text, which can hold PII anywhere'

    return None


def keyDomain(table: str, column: str, primaryKey: Sequence[str]) -> str:
    """The domain a key column masks in. A single-column primary key is named
    after its table, so a foreign key pointing at it can use the same domain.
    """

    if len(primaryKey) == 1 and primaryKey[0].upper() == column.upper():
        return table.lower()

    return '{}.{}'.format(table.lower(), column.lower())


def _isNumeric(values: Sequence[Any], category: Optional[ColumnCategory]) -> bool:

    return (category or _inferCategory(values)) == ColumnCategory.NUMBER


TEXT_STRATEGIES = {'email', 'hash', 'fakeFirstName', 'fakeLastName', 'fakeName', 'fakeCity', 'fakeCompany', 'fakeStreetAddress'}


def _compatible(strategy: str, values: Sequence[Any], category: Optional[ColumnCategory]) -> bool:
    """Whether a name-based guess fits what the column holds (`token_count` is
    not a credential). With nothing to contradict it, the name stands.
    """

    present = [value for value in values if value is not None]
    kind = category or _inferCategory(present)

    if kind is None and not present:
        return True
    if strategy in TEXT_STRATEGIES:
        return kind in (ColumnCategory.TEXT, None) and all(isinstance(value, str) for value in present)
    if strategy == 'number':
        return kind == ColumnCategory.NUMBER
    if strategy == 'dateShift':
        return kind == ColumnCategory.DATE or (kind != ColumnCategory.NUMBER and bool(present)
                                                and all(isinstance(value, str) and isIsoDate(value) for value in present))
    if strategy in ('digits', 'key'):
        return kind != ColumnCategory.DATE and all(
            isinstance(value, (str, int)) and not isinstance(value, bool)
            or isinstance(value, decimal.Decimal) and value == value.to_integral_value()
            for value in present)

    return True


def suggestColumn(table: str, column: str, category: Optional[ColumnCategory], values: Sequence[Any],
                  keyReference: Optional[Tuple[str, bool]] = None, rules: DiscoveryRules = BUILTIN_RULES) -> Suggestion:
    """One column's proposed policy.

    `keyReference`, for a key column, is the domain it shares with the other
    end and whether that end is numeric. Keys are decided first, since both
    ends must agree: `keep` for a numeric surrogate, `key` for text. Then the
    name rules, then the value rules, each in `rules`' order.
    """

    if keyReference is not None:
        domain, numeric = keyReference
        if numeric:
            return Suggestion(column, {'strategy': 'keep'}, 'numeric key (domain {}); use key if the ids themselves are meaningful'.format(domain))
        return Suggestion(column, {'strategy': 'key', 'domain': domain}, 'text key; masked one-to-one so references still match')

    words = nameWords(column)
    for rule in rules.names:
        if words & rule.words and _compatible(rule.policy['strategy'], values, category):
            return Suggestion(column, dict(rule.policy), rule.reason)

    if 'name' in words and nameWords(table) & rules.personalTables and _compatible('fakeName', values, category):
        return Suggestion(column, {'strategy': 'fakeName'}, 'a name column in a table that looks like it holds people')

    classified = _classifyValues(values, category, rules)
    if classified is not None:
        return Suggestion(column, classified[0], classified[1])

    if 'name' in words:
        return Suggestion(column, {'strategy': 'keep'}, 'a name column -- review whether it names a person')

    return Suggestion(column, {'strategy': 'keep'}, 'no sign of personal data -- review')


def keyReferences(table: str, columns: Sequence[str], primaryKey: Sequence[str], foreignKeys: Iterable[ForeignKey],
                  primaryKeys: Mapping[str, Sequence[str]]) -> Dict[str, str]:
    """Column -> key domain, for every key column of `table`.

    Covers its primary key, any column another table's foreign key references,
    and its own foreign-key columns (which take the referenced column's domain).
    `primaryKeys` maps upper-cased table names to their primary keys.
    """

    folded = {column.upper(): column for column in columns}
    domains: Dict[str, str] = {}

    for column in primaryKey:
        if column.upper() in folded:
            domains[folded[column.upper()]] = keyDomain(table, column, primaryKey)

    for foreignKey in foreignKeys:
        if foreignKey.referencedTable.upper() == table.upper():
            for referencedColumn in foreignKey.referencedColumns:
                if referencedColumn.upper() in folded:
                    domains[folded[referencedColumn.upper()]] = keyDomain(table, referencedColumn, primaryKey)

    for foreignKey in foreignKeys:
        if foreignKey.table.upper() != table.upper():
            continue
        referencedPrimaryKey = primaryKeys.get(foreignKey.referencedTable.upper(), [])
        for column, referencedColumn in zip(foreignKey.columns, foreignKey.referencedColumns):
            if column.upper() in folded:
                domains[folded[column.upper()]] = keyDomain(foreignKey.referencedTable, referencedColumn, referencedPrimaryKey)

    return domains


def proposeTable(database: Any, table: str, sampleSize: int = DEFAULT_SAMPLE_SIZE, foreignKeys: Optional[List[ForeignKey]] = None,
                 primaryKeys: Optional[Mapping[str, Sequence[str]]] = None, rules: DiscoveryRules = BUILTIN_RULES) -> TableProposal:
    """Samples `table` and suggests a policy for each of its columns.

    `database` is a bauta Database. foreignKeys and primaryKeys can be
    passed in when proposing several tables, so the schema is read once.
    `rules` is discoveryRules(), with a discovery.yaml's rules or without.
    """

    columns, rows = database.sample('SELECT * FROM {}'.format(table), sampleSize)
    types = database.getAllColumnTypes(table=table)
    primaryKey = database.getPrimaryColumnNames(table=table)

    if foreignKeys is None:
        try:
            foreignKeys = database.getForeignKeys()
        except NotImplementedError:
            foreignKeys = []

    knownPrimaryKeys = dict(primaryKeys or {})
    knownPrimaryKeys.setdefault(table.upper(), primaryKey)
    for foreignKey in foreignKeys:
        referenced = foreignKey.referencedTable.upper()
        if foreignKey.table.upper() == table.upper() and referenced not in knownPrimaryKeys:
            knownPrimaryKeys[referenced] = database.getPrimaryColumnNames(table=foreignKey.referencedTable)

    domains = keyReferences(table, columns, primaryKey, foreignKeys, knownPrimaryKeys)
    suggestions = []

    for index, column in enumerate(columns):
        values = [row[index] for row in rows]
        category = database.dialect.columnCategory(types[index]) if index < len(types) else None
        keyReference = (domains[column], _isNumeric(values, category)) if column in domains else None
        suggestions.append(suggestColumn(table, column, category, values, keyReference, rules))

    return TableProposal(table=table, columns=suggestions)


def jobName(table: str) -> str:

    return 'mask' + table[:1].upper() + table[1:]


def _scalar(value: Any) -> str:

    return yaml.safe_dump(value, default_flow_style=True, width=10 ** 6).strip().removesuffix('...').strip()


def _flow(policy: Mapping[str, Any]) -> str:

    return yaml.safe_dump(dict(policy), default_flow_style=True, sort_keys=False, width=10 ** 6).strip()


class JobDraft(NamedTuple):
    """A data job to render: its source query and where it loads."""

    table: str
    sourceQuery: str
    predecessors: List[str]
    proposal: Optional[TableProposal]


def renderJobs(drafts: Sequence[JobDraft], sourceDatabase: str, targetDatabase: str, heading: Sequence[str],
               keyVariable: str = 'MASKING_KEY', chunkSize: int = 5000) -> str:
    """A jobs.yaml document, with each suggestion's reason as a comment, which
    yaml.dump can't emit. Masking in place swaps, since upserting a masked key
    would add rows rather than replace them.
    """

    inPlace = sourceDatabase == targetDatabase
    lines = ['# ' + line if line else '#' for line in heading]
    lines += ['workers: 2', 'jobs:']

    for draft in drafts:
        lines.append('  {}:'.format(_scalar(jobName(draft.table))))
        lines.append('    active: true')
        if draft.predecessors:
            lines.append('    predecessors:')
            lines += ['    - {}'.format(_scalar(jobName(predecessor))) for predecessor in draft.predecessors]
        lines.append('    sourceDatabase: {}'.format(_scalar(sourceDatabase)))

        if '\n' in draft.sourceQuery:
            lines.append('    sourceQuery: |-')
            lines += ['      ' + line for line in draft.sourceQuery.splitlines()]
        else:
            lines.append('    sourceQuery: {}'.format(_scalar(draft.sourceQuery)))

        lines.append('    targetDatabase: {}'.format(_scalar(targetDatabase)))
        if inPlace:
            lines.append('    # Masking in place: rows load into the stage table, which is then swapped')
            lines.append('    # with the original. Create it first, with the same shape.')
            lines.append('    targetTableStage: {}'.format(_scalar(draft.table + '_masked_stage')))
            lines.append('    targetTableFinal: {}'.format(_scalar(draft.table)))
            lines.append('    insertStrategy: swap')
        else:
            lines.append('    targetTableFinal: {}'.format(_scalar(draft.table)))
            lines.append('    insertStrategy: upsert')
        lines.append('    chunkSize: {}'.format(chunkSize))

        if draft.proposal is not None:
            lines.append('    masking:')
            lines.append('      key: ${{{}}}'.format(keyVariable))
            lines.append('      columns:')
            for suggestion in draft.proposal.columns:
                lines.append('        {}: {}  # {}'.format(_scalar(suggestion.column), _flow(suggestion.policy), suggestion.reason))

    return '\n'.join(lines) + '\n'
