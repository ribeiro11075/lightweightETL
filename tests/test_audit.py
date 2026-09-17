"""The audit report: what it shows for each job, and what it flags."""
from typing import Any

from understudy_data.audit import auditJobs, renderAudit
from understudy_data.configuration import DataJobConfig

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


def test_fpe_without_strict_is_noted():
    report = auditJobs({'maskCustomers': _masked({'id': 'fpe', 'ssn': {'strategy': 'fpe', 'strict': True}})})

    assert _messages(report, 'info') == [('maskCustomers', 'fpe without strict on id: values too short for FF1 are masked with key instead')]


def _customersAndOrders(customerId, orderCustomerId, orderKey=KEY, orderTarget='staging'):
    return {
        'maskCustomers': _masked({'id': customerId, 'email': 'email'}, sourceQuery='select id, email from customers'),
        'maskOrders': _job(sourceQuery='select id, customer_id from orders', targetTableFinal='orders', targetDatabase=orderTarget,
                           masking={'key': orderKey, 'columns': {'id': 'keep', 'customer_id': orderCustomerId}}),
        }


def _crossJob(report):
    return [message for job, message in _messages(report, 'warning') if job is None]


def test_a_domain_masked_one_way_under_one_key_has_no_findings():
    customer = {'strategy': 'key', 'domain': 'customer'}

    assert auditJobs(_customersAndOrders(customer, customer))['findings'] == []


def test_a_domain_masked_two_ways_is_flagged():
    report = auditJobs(_customersAndOrders({'strategy': 'key', 'domain': 'customer'}, {'strategy': 'hash', 'domain': 'customer'}))

    assert _crossJob(report) == [
        'in staging, domain customer is masked 2 different ways, so its masks cannot match across them: '
        'hash for maskOrders.customer_id; key for maskCustomers.id. '
        'Mask the domain one way, or give columns that should not match a domain of their own']


def test_a_domain_masked_with_different_options_is_flagged():
    report = auditJobs(_customersAndOrders({'strategy': 'hash', 'domain': 'customer'}, {'strategy': 'hash', 'domain': 'customer', 'length': 20}))

    (message,) = _crossJob(report)
    assert 'hash for maskCustomers.id; hash (length: 20) for maskOrders.customer_id' in message


def test_a_domain_masked_under_two_keys_is_flagged():
    customer = {'strategy': 'key', 'domain': 'customer'}
    report = auditJobs(_customersAndOrders(customer, customer, orderKey='another-audit-masking-key'))

    (message,) = _crossJob(report)
    assert message.startswith('in staging, domain customer is masked under 2 different keys')
    assert 'maskCustomers.id' in message and 'maskOrders.customer_id' in message


def test_copies_in_different_target_databases_may_use_different_keys():
    customer = {'strategy': 'key', 'domain': 'customer'}

    assert _crossJob(auditJobs(_customersAndOrders(customer, customer, orderKey='another-audit-masking-key', orderTarget='vendor'))) == []


def test_default_domains_are_compared_too():
    report = auditJobs({
        'maskCustomers': _masked({'name': 'fakeName'}),
        'maskCompanies': _masked({'name': 'fakeCompany'}, targetTableFinal='companies'),
        })

    (message,) = _crossJob(report)
    assert 'domain name is masked 2 different ways' in message


def _foreignKey():
    from understudy_data.databaseDialects import ForeignKey

    return ForeignKey('orders', ('customer_id',), 'customers', ('id',), 'fk_orders_customers')


def _connected(jobs, **overrides):
    arguments = dict(
        returnedColumns={'maskCustomers': ['id', 'email'], 'maskOrders': ['id', 'customer_id']},
        targetColumns={'maskCustomers': ['id', 'email'], 'maskOrders': ['id', 'customer_id']},
        foreignKeys={'staging': [_foreignKey()]})
    arguments.update(overrides)
    return auditJobs(jobs, **arguments)


def test_a_reference_masked_like_its_key_has_no_findings():
    customer = {'strategy': 'key', 'domain': 'customer'}

    assert _connected(_customersAndOrders(customer, customer))['findings'] == []


def test_a_reference_left_in_its_default_domain_is_flagged():
    report = _connected(_customersAndOrders('key', 'key'))
    fingerprint = report['jobs'][0]['keyFingerprint']

    assert _messages(report, 'warning') == [(
        'maskOrders',
        'in staging, orders.customer_id is masked with key in domain customer_id under key {0}, but customers.id, which it references, '
        'is masked with key in domain id under key {0} (by maskCustomers), so the copied references will not match'.format(fingerprint))]


def test_a_reference_copied_as_it_is_to_a_masked_key_is_flagged():
    jobs = _customersAndOrders('key', 'keep')

    (message,) = [message for _, message in _messages(_connected(jobs), 'warning')]
    assert message.startswith('in staging, orders.customer_id is not masked, but customers.id, which it references, is masked with key')


def _references(report):
    return [message for _, message in _messages(report, 'warning') if 'which it references' in message]


def test_a_job_without_masking_counts_as_copying_every_column_as_it_is():
    jobs = _customersAndOrders('keep', 'keep')
    jobs['maskOrders'] = _job(sourceQuery='select id, customer_id from orders', targetTableFinal='orders')

    assert _references(_connected(jobs)) == []

    jobs['maskCustomers'] = _masked({'id': 'key', 'email': 'email'}, sourceQuery='select id, email from customers')
    (message,) = _references(_connected(jobs))
    assert 'orders.customer_id is not masked' in message


def test_a_nulled_reference_cannot_break():
    assert _connected(_customersAndOrders('key', 'null'))['findings'] == []


def test_target_columns_are_matched_to_the_query_by_position():
    customer = {'strategy': 'key', 'domain': 'customer'}
    jobs = _customersAndOrders(customer, customer)
    jobs['maskOrders'] = _job(sourceQuery='select id, owner from orders', targetTableFinal='app.ORDERS',
                              masking={'key': KEY, 'columns': {'id': 'keep', 'owner': {'strategy': 'hash', 'domain': 'customer'}}})

    report = _connected(jobs, returnedColumns={'maskCustomers': ['id', 'email'], 'maskOrders': ['id', 'owner']})

    assert any('orders.customer_id is masked with hash in domain customer' in message for _, message in _messages(report, 'warning'))


def test_a_job_whose_columns_do_not_line_up_is_not_guessed_at():
    report = _connected(_customersAndOrders('key', 'key'), targetColumns={'maskCustomers': ['id', 'email'], 'maskOrders': ['customer_id']})

    assert _messages(report, 'warning') == []
