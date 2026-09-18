"""Proposes masking policies from a live schema, for `bauta discover`.

A proposal is for review: each suggestion carries its reason, rendered beside
the column. Classification uses column names, then sampled values, which are
only examined in memory -- never printed, logged or written.
"""
from __future__ import annotations

import datetime
import decimal
import re
from typing import Any, Dict, Iterable, List, Mapping, NamedTuple, Optional, Sequence, Set, Tuple

import yaml

from .databaseDialects import ColumnCategory, ForeignKey

DEFAULT_SAMPLE_SIZE = 1000

# A share of sampled values that must match a pattern before it's trusted.
VALUE_MATCH_THRESHOLD = 0.8

EMAIL = re.compile(r'^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$')
PHONE = re.compile(r'^\+?[\d\s().-]{7,}$')
NATIONAL_ID = re.compile(r'^\d{3}-\d{2}-\d{4}$')
CARD = re.compile(r'^[\d -]{13,23}$')
IPV4 = re.compile(r'^(\d{1,3}\.){3}\d{1,3}$')
UUID_TEXT = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')

PERSONAL_TABLE_WORDS = {'customer', 'customers', 'user', 'users', 'person', 'people', 'employee', 'employees', 'contact',
                        'contacts', 'member', 'members', 'patient', 'patients', 'client', 'clients', 'account', 'accounts',
                        'student', 'students', 'applicant', 'applicants', 'guest', 'guests'}

FREE_TEXT_AVERAGE_LENGTH = 60

# (words any of which must be in the column name, policy, reason). First match
# wins, so specific entries come first. Names are matched split on underscores
# and camelCase and joined whole, so `first_name` and `FIRSTNAME` both match.
NAME_RULES: Tuple[Tuple[Tuple[str, ...], Dict[str, Any], str], ...] = (
    (('email', 'emailaddress', 'mail'), {'strategy': 'email'}, 'name suggests an email address'),
    (('password', 'passwd', 'pwd', 'secret', 'token', 'apikey', 'salt'), {'strategy': 'hash'}, 'name suggests a credential'),
    (('ssn', 'socialsecurity', 'socialsecuritynumber', 'nationalid', 'taxid', 'tin', 'passport', 'passportnumber',
      'licensenumber', 'licencenumber', 'driverslicense'),
     {'strategy': 'key'}, 'name suggests a government identifier; key keeps it unique and shaped'),
    (('creditcard', 'cardnumber', 'ccnumber', 'pan'), {'strategy': 'digits', 'keepTrailing': 4}, 'name suggests a card number'),
    (('iban', 'accountnumber', 'routingnumber', 'bankaccount', 'sortcode'), {'strategy': 'digits'}, 'name suggests a bank account'),
    (('phone', 'phonenumber', 'mobile', 'cell', 'fax', 'telephone', 'tel'), {'strategy': 'digits'}, 'name suggests a phone number'),
    (('firstname', 'givenname', 'forename'), {'strategy': 'fakeFirstName'}, 'name suggests a first name'),
    (('lastname', 'surname', 'familyname'), {'strategy': 'fakeLastName'}, 'name suggests a last name'),
    (('fullname', 'contactname', 'customername', 'displayname', 'personname', 'employeename'), {'strategy': 'fakeName'},
     'name suggests a person\'s name'),
    (('username', 'login', 'handle', 'screenname'), {'strategy': 'key'}, 'name suggests a user name; key keeps it unique'),
    (('company', 'companyname', 'employer', 'organization', 'organisation'), {'strategy': 'fakeCompany'}, 'name suggests a company'),
    (('ip', 'ipaddress', 'ipaddr'), {'strategy': 'hash'}, 'name suggests an IP address'),
    (('street', 'address', 'addressline', 'addr', 'line1', 'line2', 'streetaddress'), {'strategy': 'fakeStreetAddress'},
     'name suggests a street address'),
    (('city', 'town'), {'strategy': 'fakeCity'}, 'name suggests a city'),
    (('zip', 'zipcode', 'postal', 'postalcode', 'postcode'), {'strategy': 'digits'}, 'name suggests a postal code'),
    (('birth', 'birthdate', 'dob', 'birthday', 'dateofbirth'), {'strategy': 'dateShift', 'maxDays': 30}, 'name suggests a date of birth'),
    (('salary', 'income', 'wage', 'wages', 'compensation', 'bonus'), {'strategy': 'number', 'variance': 0.1},
     'name suggests compensation'),
    (('latitude', 'longitude', 'lat', 'lng', 'lon'), {'strategy': 'number', 'variance': 0.01}, 'name suggests a coordinate'),
    (('gender', 'sex', 'race', 'ethnicity', 'religion', 'nationality'), {'strategy': 'shuffle'},
     'name suggests a sensitive attribute; shuffle keeps the distribution but is not anonymization'),
    (('note', 'notes', 'comment', 'comments', 'description', 'remarks', 'memo', 'bio', 'message', 'body', 'freetext'),
     {'strategy': 'null'}, 'name suggests free text, which can hold PII anywhere; redact keeps the text but only removes identifiers with a known shape'),
    )


class Suggestion(NamedTuple):

    column: str
    policy: Dict[str, Any]
    reason: str


class TableProposal(NamedTuple):

    table: str
    columns: List[Suggestion]


def nameWords(name: str) -> Set[str]:
    """A column name's words, lower-cased, plus the whole name run together:
    `first_name`, `firstName` and `FIRSTNAME` all give `firstname`.
    """

    split = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
    words = [word for word in re.split(r'[^a-z0-9]+', split.lower()) if word]

    return set(words) | {''.join(words)}


def personalDataHint(column: str) -> Optional[str]:
    """Why a column's name alone suggests personal data, or None.

    The same name rules discovery proposes policies from, without the sampled
    values -- what `audit` uses to question a column that is kept as it is.
    """

    words = nameWords(column)

    return next((reason for ruleWords, policy, reason in NAME_RULES if policy['strategy'] != 'keep' and words & set(ruleWords)), None)


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


def _luhn(digits: str) -> bool:

    total = 0
    for position, character in enumerate(reversed(digits)):
        digit = int(character)
        if position % 2:
            digit = digit * 2 - 9 if digit > 4 else digit * 2
        total += digit

    return total % 10 == 0


def _share(values: Sequence[str], test: Any) -> float:

    return sum(1 for value in values if test(value)) / len(values) if values else 0.0


def _classifyValues(texts: Sequence[str]) -> Optional[Tuple[Dict[str, Any], str]]:
    """What sampled text looks like, if it looks like anything in particular."""

    if not texts:
        return None

    stripped = [text.strip() for text in texts]

    def isCard(text: str) -> bool:
        digits = re.sub(r'\D', '', text)
        return bool(CARD.match(text)) and 13 <= len(digits) <= 19 and _luhn(digits)

    checks = (
        (lambda text: bool(EMAIL.match(text)), {'strategy': 'email'}, 'sampled values look like email addresses'),
        (lambda text: bool(NATIONAL_ID.match(text)), {'strategy': 'key', 'charset': 'digits'}, 'sampled values look like national identifiers'),
        (isCard, {'strategy': 'digits', 'keepTrailing': 4}, 'sampled values look like card numbers'),
        (lambda text: bool(IPV4.match(text)), {'strategy': 'hash'}, 'sampled values look like IP addresses'),
        (lambda text: bool(UUID_TEXT.match(text)), {'strategy': 'keep'}, 'sampled values are UUIDs, usually surrogate keys -- review'),
        # Before phone numbers, which ISO dates would otherwise pass for.
        (_isIsoDate, {'strategy': 'keep'}, 'sampled values are dates -- review whether they identify anyone'),
        (lambda text: bool(PHONE.match(text)) and len(re.sub(r'\D', '', text)) >= 7 and not text.isdigit(), {'strategy': 'digits'},
         'sampled values look like phone numbers'),
        )

    for test, policy, reason in checks:
        if _share(stripped, test) >= VALUE_MATCH_THRESHOLD:
            return dict(policy), reason

    if sum(len(text) for text in texts) / len(texts) > FREE_TEXT_AVERAGE_LENGTH:
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


def _isIsoDate(text: str) -> bool:

    try:
        datetime.datetime.fromisoformat(text.strip())
    except ValueError:
        return False

    return True


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
                                                and all(isinstance(value, str) and _isIsoDate(value) for value in present))
    if strategy in ('digits', 'key'):
        return kind != ColumnCategory.DATE and all(
            isinstance(value, (str, int)) and not isinstance(value, bool)
            or isinstance(value, decimal.Decimal) and value == value.to_integral_value()
            for value in present)

    return True


def suggestColumn(table: str, column: str, category: Optional[ColumnCategory], values: Sequence[Any],
                  keyReference: Optional[Tuple[str, bool]] = None) -> Suggestion:
    """One column's proposed policy.

    `keyReference`, for a key column, is the domain it shares with the other
    end and whether that end is numeric. Keys are decided first, since both
    ends must agree: `keep` for a numeric surrogate, `key` for text.
    """

    if keyReference is not None:
        domain, numeric = keyReference
        if numeric:
            return Suggestion(column, {'strategy': 'keep'}, 'numeric key (domain {}); use key if the ids themselves are meaningful'.format(domain))
        return Suggestion(column, {'strategy': 'key', 'domain': domain}, 'text key; masked one-to-one so references still match')

    words = nameWords(column)
    for ruleWords, policy, reason in NAME_RULES:
        if words & set(ruleWords) and _compatible(policy['strategy'], values, category):
            return Suggestion(column, dict(policy), reason)

    if 'name' in words and nameWords(table) & PERSONAL_TABLE_WORDS and _compatible('fakeName', values, category):
        return Suggestion(column, {'strategy': 'fakeName'}, 'a name column in a table that looks like it holds people')

    texts = [value for value in values if isinstance(value, str)]
    if texts and len(texts) == len([value for value in values if value is not None]):
        classified = _classifyValues(texts)
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
                 primaryKeys: Optional[Mapping[str, Sequence[str]]] = None) -> TableProposal:
    """Samples `table` and suggests a policy for each of its columns.

    `database` is a bauta Database. foreignKeys and primaryKeys can be
    passed in when proposing several tables, so the schema is read once.
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
        suggestions.append(suggestColumn(table, column, category, values, keyReference))

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
