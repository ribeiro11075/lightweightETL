"""What a set of jobs does with data, for a reviewer -- `lightweight-etl audit`.

A policy that passes validation can still be a poor one: a column named `email`
kept as it is, a defaultStrategy of `keep` that lets any new column through, a
job that copies from a production database without masking while its
neighbours mask. None of that is an error, so nothing else reports it. This
does, as findings a person reviews -- and, with --strict, a CI gate.

Everything here works on plain data. The CLI supplies what needs a connection:
the columns each masked query really returns, and whether each connection is
encrypted.
"""
from __future__ import annotations

import datetime
from typing import Any, Dict, List, Mapping, NamedTuple, Optional, Sequence

from .configuration import DataJobConfig
from .discovery import personalDataHint
from .masking import MaskingError, MaskingPlan, keyFingerprint, resolveStrategy

SEVERITIES = ('error', 'warning', 'info')


class Finding(NamedTuple):

    severity: str
    job: Optional[str]
    message: str


def _declaredColumns(plan: MaskingPlan) -> List[Dict[str, Any]]:
    """The policy as written, for a job whose query wasn't run."""

    columns = []
    for column, policy in plan.columns.items():
        keyed = resolveStrategy(policy['strategy']).KEYED
        columns.append({'column': column, 'strategy': policy['strategy'], 'domain': policy.get('domain', column.lower()) if keyed else None,
                        'source': 'column'})

    return columns


def _auditMaskedJob(name: str, job: DataJobConfig, returned: Optional[Sequence[str]], findings: List[Finding]) -> Dict[str, Any]:

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
              generatedAt: Optional[datetime.datetime] = None) -> Dict[str, Any]:
    """The audit report, as a JSON-ready dict.

    `returnedColumns` maps a masked job to the columns its query returns, so
    each column's actual policy can be shown -- defaultStrategy included --
    rather than only the declared ones. `encryption` maps a database alias to
    whether its connection is encrypted (None: couldn't tell). `unreachable`
    maps a job to why its query couldn't be checked. All three come from
    connecting, and all are optional.
    """

    returnedColumns = returnedColumns or {}
    encryption = encryption or {}
    unreachable = unreachable or {}
    findings: List[Finding] = []
    maskedSources = {job.sourceDatabase for job in jobs.values() if job.masking is not None}
    report = []

    for name, job in sorted(jobs.items()):
        entry: Dict[str, Any] = {'job': name, 'active': job.active, 'sourceDatabase': job.sourceDatabase, 'targetDatabase': job.targetDatabase,
                                 'targetTable': job.targetTableFinal, 'masked': job.masking is not None}

        if name in unreachable:
            findings.append(Finding('error', name, 'sourceQuery could not be checked: {}'.format(unreachable[name])))

        if job.masking is not None:
            entry.update(_auditMaskedJob(name, job, returnedColumns.get(name), findings))
            if encryption.get(job.sourceDatabase) is False:
                findings.append(Finding('warning', name, 'reads unmasked data from {} over a connection that is not encrypted'.format(job.sourceDatabase)))
        elif job.sourceDatabase in maskedSources and job.sourceDatabase != job.targetDatabase:
            findings.append(Finding('warning', name, 'copies from {} without masking, though other jobs mask what they read from it'.format(
                job.sourceDatabase)))

        report.append(entry)

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
