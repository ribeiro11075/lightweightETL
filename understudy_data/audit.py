"""What a set of jobs does with data, for a reviewer -- `understudy audit`.

A policy that passes validation can still be a poor one: a column named `email`
kept as it is, a defaultStrategy of `keep` that lets any new column through, a
job that copies from a production database without masking while its
neighbours mask. None of that is an error, so nothing else reports it. This
does, as findings a person reviews -- and, with --strict, a CI gate.

Everything here works on plain data. The CLI supplies what needs a connection:
the columns each masked query really returns, the columns of each target, the
foreign keys, and whether each connection is encrypted.
"""
from __future__ import annotations

import datetime
from typing import Any, Dict, Iterable, List, Mapping, NamedTuple, Optional, Sequence, Tuple

from .configuration import DataJobConfig
from .databaseDialects import ForeignKey, unqualifiedName
from .discovery import personalDataHint
from .masking import MaskingError, MaskingPlan, keyFingerprint, resolveStrategy

SEVERITIES = ('error', 'warning', 'info')


class Finding(NamedTuple):

    severity: str
    job: Optional[str]
    message: str


class _Usage(NamedTuple):
    """How one job masks one column: what has to agree for masks to match.

    `domain` is None for a strategy that doesn't use the key, and `policy` is
    None for a column copied as it is -- `keep`, or a job that doesn't mask.
    """

    job: str
    column: str
    domain: Optional[str]
    policy: Optional[str]
    keyFingerprint: Optional[str]

    @property
    def label(self) -> str:

        return '{}.{}'.format(self.job, self.column)

    @property
    def signature(self) -> Tuple[Optional[str], Optional[str], Optional[str]]:

        return self.domain, self.policy, self.keyFingerprint

    def describe(self) -> str:

        if self.policy is None:
            return 'not masked'
        if self.domain is None:
            return 'masked with {}'.format(self.policy)

        return 'masked with {} in domain {} under key {}'.format(self.policy, self.domain, self.keyFingerprint)


def _describePolicy(policy: Mapping[str, Any]) -> str:
    """A strategy and its options, as a reviewer would compare them."""

    options = ', '.join('{}: {}'.format(name, policy[name]) for name in sorted(policy) if name not in ('strategy', 'domain'))

    return '{} ({})'.format(policy['strategy'], options) if options else policy['strategy']


def _usages(name: str, plan: MaskingPlan, columns: Sequence[Mapping[str, Any]]) -> List[_Usage]:

    fingerprint = keyFingerprint(plan.key)
    declared = {column.upper(): policy for column, policy in plan.columns.items()}
    usages = []

    for entry in columns:
        policy = declared.get(entry['column'].upper()) if entry['source'] == 'column' else plan.defaultStrategy
        assert policy is not None
        if policy['strategy'] == 'keep':
            usages.append(_Usage(name, entry['column'], None, None, None))
        elif entry['domain'] is None:
            usages.append(_Usage(name, entry['column'], None, _describePolicy(policy), None))
        else:
            usages.append(_Usage(name, entry['column'], entry['domain'], _describePolicy(policy), fingerprint))

    return usages


def _labels(usages: Iterable[_Usage]) -> str:

    return ', '.join(sorted(usage.label for usage in usages))


def _auditDomains(target: str, usages: Sequence[_Usage], findings: List[Finding]) -> None:
    """Masks agree only between columns masked in the same domain, the same way,
    under the same key. A domain shared in one target database is a promise that
    they do, so each difference is reported. Copies in different target
    databases may deliberately use different keys, so they aren't compared.
    """

    byDomain: Dict[str, List[_Usage]] = {}
    for usage in usages:
        if usage.domain is not None:
            byDomain.setdefault(usage.domain, []).append(usage)

    for domain, shared in sorted(byDomain.items()):
        for what, attribute, noun in (('under {} different keys', 'keyFingerprint', 'key {}'), ('{} different ways', 'policy', '{}')):
            variants: Dict[str, List[_Usage]] = {}
            for usage in shared:
                variants.setdefault(getattr(usage, attribute), []).append(usage)
            if len(variants) < 2:
                continue
            findings.append(Finding('warning', None, 'in {}, domain {} is masked {}, so its masks cannot match across them: {}. '
                                    'Mask the domain one way, or give columns that should not match a domain of their own'.format(
                                        target, domain, what.format(len(variants)),
                                        '; '.join('{} for {}'.format(noun.format(variant), _labels(group)) for variant, group in sorted(variants.items())))))


def _auditForeignKeys(target: str, jobs: Mapping[str, DataJobConfig], usagesByJob: Mapping[str, Sequence[_Usage]],
                      targetColumns: Mapping[str, Sequence[str]], foreignKeys: Sequence[ForeignKey], findings: List[Finding]) -> None:
    """A foreign key survives masking only if its columns are masked exactly as
    the columns they reference. Jobs are matched to a key's tables by their
    targetTableFinal's name, and a masked job's target columns to its query's
    by position, as the load matches them.
    """

    # (table, column) -> how each job loading that table fills that column
    filled: Dict[Tuple[str, str], List[_Usage]] = {}
    for name, job in sorted(jobs.items()):
        table = unqualifiedName(job.targetTableFinal).upper()
        if job.masking is None:
            filled.setdefault((table, '*'), []).append(_Usage(name, '*', None, None, None))
            continue
        columns = targetColumns.get(name)
        usages = usagesByJob.get(name)
        if columns is None or usages is None or len(columns) != len(usages):
            continue
        for column, usage in zip(columns, usages):
            filled.setdefault((table, column.upper()), []).append(usage)

    def lookup(table: str, column: str) -> List[_Usage]:
        return filled.get((table.upper(), column.upper()), []) + filled.get((table.upper(), '*'), [])

    for foreignKey in foreignKeys:
        for column, referencedColumn in zip(foreignKey.columns, foreignKey.referencedColumns):
            for child in lookup(foreignKey.table, column):
                for parent in lookup(foreignKey.referencedTable, referencedColumn):
                    # A NULL reference points at nothing, so it can't break.
                    if child.signature == parent.signature or child.policy == 'null':
                        continue
                    findings.append(Finding('warning', child.job, 'in {}, {}.{} is {}, but {}.{}, which it references, is {} (by {}), '
                                            'so the copied references will not match'.format(
                                                target, foreignKey.table, column, child.describe(),
                                                foreignKey.referencedTable, referencedColumn, parent.describe(), parent.job)))


def _declaredColumns(plan: MaskingPlan) -> List[Dict[str, Any]]:
    """The policy as written, for a job whose query wasn't run."""

    columns = []
    for column, policy in plan.columns.items():
        keyed = resolveStrategy(policy['strategy']).KEYED
        columns.append({'column': column, 'strategy': policy['strategy'], 'domain': policy.get('domain', column.lower()) if keyed else None,
                        'source': 'column'})

    return columns


def _auditMaskedJob(name: str, job: DataJobConfig, returned: Optional[Sequence[str]], findings: List[Finding],
                    usages: Dict[str, List[_Usage]]) -> Dict[str, Any]:

    assert job.masking is not None
    plan = MaskingPlan(key=job.masking.key.get_secret_value(), columns=job.masking.columns, defaultStrategy=job.masking.defaultStrategy)
    columns = _declaredColumns(plan)
    resolved = False

    if returned is not None:
        try:
            columns = [entry._asdict() for entry in plan.bind(returned).manifest]
            resolved = True
        except MaskingError as error:
            findings.append(Finding('error', name, 'the policy does not match what sourceQuery returns: {}'.format(error)))

    for entry in columns:
        hint = personalDataHint(entry['column'])
        entry['personalDataHint'] = hint
        if entry['strategy'] == 'keep' and hint:
            findings.append(Finding('warning', name, 'column {} is kept unmasked, but its {}'.format(entry['column'], hint)))

    defaultStrategy = plan.defaultStrategy
    if defaultStrategy is not None:
        if defaultStrategy['strategy'] == 'keep':
            findings.append(Finding('warning', name, 'defaultStrategy is keep, so any column added to the source later is copied unmasked'))
        fallen = [entry['column'] for entry in columns if entry['source'] == 'defaultStrategy']
        if fallen:
            findings.append(Finding('info', name, '{} column(s) fall to defaultStrategy {}: {}'.format(
                len(fallen), defaultStrategy['strategy'], ', '.join(fallen))))

    redacted = sorted(column for column, policy in plan.columns.items() if policy['strategy'] == 'redact')
    if redacted:
        findings.append(Finding('info', name, 'redact on {}: identifiers with a recognisable shape are removed, names are not'.format(
            ', '.join(redacted))))

    lenient = sorted(column for column, policy in plan.columns.items() if policy['strategy'] == 'fpe' and not policy.get('strict'))
    if lenient:
        findings.append(Finding('info', name, 'fpe without strict on {}: values too short for FF1 are masked with key instead'.format(
            ', '.join(lenient))))

    usages[name] = _usages(name, plan, columns)

    if job.watermarkColumn and any(entry['strategy'] == 'shuffle' for entry in columns):
        findings.append(Finding('warning', name, 'shuffle on an incremental job: its small chunks leave values on or near their own rows'))

    return {
        'keyFingerprint': keyFingerprint(job.masking.key.get_secret_value()),
        'defaultStrategy': defaultStrategy,
        'columnsResolved': resolved,
        'columns': columns,
        }


def auditJobs(jobs: Mapping[str, DataJobConfig], returnedColumns: Optional[Mapping[str, Sequence[str]]] = None,
              encryption: Optional[Mapping[str, Optional[bool]]] = None, unreachable: Optional[Mapping[str, str]] = None,
              targetColumns: Optional[Mapping[str, Sequence[str]]] = None, foreignKeys: Optional[Mapping[str, Sequence[ForeignKey]]] = None,
              generatedAt: Optional[datetime.datetime] = None) -> Dict[str, Any]:
    """The audit report, as a JSON-ready dict.

    `returnedColumns` maps a masked job to the columns its query returns, so
    each column's actual policy can be shown -- defaultStrategy included --
    rather than only the declared ones. `encryption` maps a database alias to
    whether its connection is encrypted (None: couldn't tell). `unreachable`
    maps a job to why its query couldn't be checked. `targetColumns` maps a
    masked job to its target's columns in load order, and `foreignKeys` maps a
    target database alias to the foreign keys that apply to its tables, for
    checking that references still match once masked. All of these come from
    connecting, and all are optional.
    """

    returnedColumns = returnedColumns or {}
    encryption = encryption or {}
    unreachable = unreachable or {}
    findings: List[Finding] = []
    usages: Dict[str, List[_Usage]] = {}
    maskedSources = {job.sourceDatabase for job in jobs.values() if job.masking is not None}
    report = []

    for name, job in sorted(jobs.items()):
        entry: Dict[str, Any] = {'job': name, 'active': job.active, 'sourceDatabase': job.sourceDatabase, 'targetDatabase': job.targetDatabase,
                                 'targetTable': job.targetTableFinal, 'masked': job.masking is not None}

        if name in unreachable:
            findings.append(Finding('error', name, 'sourceQuery could not be checked: {}'.format(unreachable[name])))

        if job.masking is not None:
            entry.update(_auditMaskedJob(name, job, returnedColumns.get(name), findings, usages))
            if encryption.get(job.sourceDatabase) is False:
                findings.append(Finding('warning', name, 'reads unmasked data from {} over a connection that is not encrypted'.format(job.sourceDatabase)))
        elif job.sourceDatabase in maskedSources and job.sourceDatabase != job.targetDatabase:
            findings.append(Finding('warning', name, 'copies from {} without masking, though other jobs mask what they read from it'.format(
                job.sourceDatabase)))

        report.append(entry)

    for target in sorted({job.targetDatabase for job in jobs.values()}):
        targetJobs = {name: job for name, job in jobs.items() if job.targetDatabase == target}
        _auditDomains(target, [usage for name in sorted(targetJobs) for usage in usages.get(name, [])], findings)
        if foreignKeys and foreignKeys.get(target):
            _auditForeignKeys(target, targetJobs, usages, targetColumns or {}, foreignKeys[target], findings)

    for alias, encrypted in sorted(encryption.items()):
        if encrypted is None and alias in maskedSources:
            findings.append(Finding('info', None, 'could not tell whether the connection to {} is encrypted'.format(alias)))

    findings.sort(key=lambda finding: (SEVERITIES.index(finding.severity), finding.job or '', finding.message))

    return {
        'generatedAt': (generatedAt or datetime.datetime.now(datetime.timezone.utc)).isoformat(timespec='seconds'),
        'jobs': report,
        'connections': {alias: {'encrypted': encrypted} for alias, encrypted in sorted(encryption.items())},
        'findings': [finding._asdict() for finding in findings],
        'summary': {severity: sum(1 for finding in findings if finding.severity == severity) for severity in SEVERITIES},
        }


def renderAudit(report: Mapping[str, Any]) -> str:
    """The report for a terminal: each job's columns, then the findings."""

    lines = []

    for job in report['jobs']:
        state = '' if job['active'] else ' (inactive)'
        lines.append('{}{}: {} -> {}.{}'.format(job['job'], state, job['sourceDatabase'], job['targetDatabase'], job['targetTable']))

        if not job['masked']:
            lines.append('  not masked')
            lines.append('')
            continue

        scope = 'as returned by sourceQuery' if job['columnsResolved'] else 'as declared (run with --connect to resolve)'
        lines.append('  masked under key {}, columns {}:'.format(job['keyFingerprint'], scope))
        for column in job['columns']:
            domain = ' in domain {}'.format(column['domain']) if column['domain'] else ''
            origin = ' (defaultStrategy)' if column['source'] == 'defaultStrategy' else ''
            lines.append('    {:<28} {}{}{}'.format(column['column'], column['strategy'], domain, origin))
        if job['defaultStrategy'] and not job['columnsResolved']:
            lines.append('    {:<28} {} (defaultStrategy)'.format('any other column', job['defaultStrategy']['strategy']))
        lines.append('')

    if report['connections']:
        lines.append('Connections:')
        for alias, connection in report['connections'].items():
            encrypted = {True: 'encrypted', False: 'NOT encrypted', None: 'encryption unknown'}[connection['encrypted']]
            lines.append('  {:<28} {}'.format(alias, encrypted))
        lines.append('')

    summary = report['summary']
    lines.append('Findings: {} error(s), {} warning(s), {} note(s)'.format(summary['error'], summary['warning'], summary['info']))
    for finding in report['findings']:
        lines.append('  {:<8} {}{}'.format(finding['severity'].upper(), '{}: '.format(finding['job']) if finding['job'] else '', finding['message']))

    return '\n'.join(lines) + '\n'
