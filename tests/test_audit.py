"""The audit report: what it shows for each job, and what it flags."""
from typing import Any

from lightweight_etl.audit import auditJobs, renderAudit
from lightweight_etl.configuration import DataJobConfig

KEY = 'an-audit-test-masking-key'


def _job(**overrides: Any) -> DataJobConfig:
    fields = dict(active=True, sourceDatabase='prod', sourceQuery='select * from customers', targetDatabase='staging',
                  targetTableFinal='customers', insertStrategy='upsert', chunkSize=100)
    fields.update(overrides)
    return DataJobConfig(**fields)


def _masked(columns, **overrides: Any) -> DataJobConfig:
    masking = {'key': KEY, 'columns': columns}
    if 'defaultStrategy' in overrides:
        masking['defaultStrategy'] = overrides.pop('defaultStrategy')
    return _job(masking=masking, **overrides)


def _messages(report, severity=None):
    return [(finding['job'], finding['message']) for finding in report['findings'] if severity in (None, finding['severity'])]


def test_a_clean_policy_has_no_findings():
    report = auditJobs({'maskCustomers': _masked({'id': 'keep', 'email': 'email', 'notes': 'null'})})

    assert report['findings'] == []
    assert report['summary'] == {'error': 0, 'warning': 0, 'info': 0}
    (job,) = report['jobs']
    assert job['masked'] and not job['columnsResolved']
    assert [(column['column'], column['strategy']) for column in job['columns']] == [('id', 'keep'), ('email', 'email'), ('notes', 'null')]


def test_a_kept_column_whose_name_suggests_personal_data_is_flagged():
    report = auditJobs({'maskCustomers': _masked({'id': 'keep', 'phone_number': 'keep'})})

    assert _messages(report, 'warning') == [('maskCustomers', 'column phone_number is kept unmasked, but its name suggests a phone number')]


def test_a_default_strategy_of_keep_is_flagged():
    report = auditJobs({'maskCustomers': _masked({'email': 'email'}, defaultStrategy='keep')})

    assert ('maskCustomers', 'defaultStrategy is keep, so any column added to the source later is copied unmasked') in _messages(report, 'warning')


def test_resolved_columns_show_what_falls_to_the_default_strategy():
    report = auditJobs({'maskCustomers': _masked({'email': 'email'}, defaultStrategy='null')},
                       returnedColumns={'maskCustomers': ['EMAIL', 'notes', 'ssn']})

    (job,) = report['jobs']
    assert job['columnsResolved']
    assert [(column['column'], column['strategy'], column['source']) for column in job['columns']] == [
        ('EMAIL', 'email', 'column'), ('notes', 'null', 'defaultStrategy'), ('ssn', 'null', 'defaultStrategy')]
    assert _messages(report, 'info') == [('maskCustomers', '2 column(s) fall to defaultStrategy null: notes, ssn')]


def test_a_policy_that_does_not_cover_the_query_is_an_error():
    report = auditJobs({'maskCustomers': _masked({'email': 'email'})}, returnedColumns={'maskCustomers': ['email', 'ssn']})

    assert report['summary']['error'] == 1
    assert 'not in the masking policy: ssn' in report['findings'][0]['message']


def test_an_unmasked_copy_from_a_source_other_jobs_mask_is_flagged():
    report = auditJobs({
        'maskCustomers': _masked({'email': 'email'}),
        'copyOrders': _job(sourceQuery='select * from orders', targetTableFinal='orders'),
        'rollUp': _job(sourceDatabase='staging', targetDatabase='staging', targetTableFinal='summary'),
        })

    assert _messages(report, 'warning') == [
        ('copyOrders', 'copies from prod without masking, though other jobs mask what they read from it')]


def test_an_unencrypted_source_connection_is_flagged_for_masked_jobs():
    report = auditJobs({'maskCustomers': _masked({'email': 'email'})}, encryption={'prod': False, 'staging': True})

    assert _messages(report, 'warning') == [('maskCustomers', 'reads unmasked data from prod over a connection that is not encrypted')]
    assert report['connections'] == {'prod': {'encrypted': False}, 'staging': {'encrypted': True}}


def test_shuffle_on_an_incremental_job_is_flagged():
    job = _masked({'id': 'keep', 'updatedAt': 'keep', 'gender': 'shuffle'}, watermarkColumn='updatedAt', watermarkInitial='1970-01-01',
                  sourceQuery='select * from customers where updatedAt > {{ watermark }}')

    report = auditJobs({'incremental': job})

    assert any('shuffle on an incremental job' in message for _, message in _messages(report, 'warning'))


def test_findings_are_ordered_errors_first():
    report = auditJobs({'maskCustomers': _masked({'email': 'keep'}, defaultStrategy='null')},
                       returnedColumns={'maskCustomers': ['email', 'notes']}, unreachable={'other': 'boom'})

    assert [finding['severity'] for finding in report['findings']] == ['warning', 'info']


def test_the_text_report_names_every_column_and_finding():
    text = renderAudit(auditJobs({
        'maskCustomers': _masked({'email': 'keep'}, defaultStrategy='null'),
        'copyOrders': _job(active=False),
        }, encryption={'prod': None}))

    assert 'maskCustomers: prod -> staging.customers' in text
    assert 'email                        keep' in text
    assert 'any other column             null (defaultStrategy)' in text
    assert 'copyOrders (inactive): prod -> staging.customers\n  not masked' in text
    assert 'prod                         encryption unknown' in text
    assert 'WARNING  maskCustomers: column email is kept unmasked, but its name suggests an email address' in text
