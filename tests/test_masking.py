"""The masking engine, without a database.

Most of what matters about masking is a property rather than an example --
deterministic, consistent within a domain, one-to-one for keys, type-preserving
for numbers -- so these tests state the property and check it over many values.
"""
import datetime
import decimal
import re
import uuid

import pytest

from understudy_data.configuration import Configuration, ConfigurationError, DataJobsFile
from understudy_data.dependencyGraph import JobOutcome, JobStatus
from understudy_data.masking import (FIRST_NAMES, LAST_NAMES, STRATEGIES, KeyedHash, MaskingError, MaskingPlan, buildMaskingManifest,
                                     keyFingerprint, validateColumnPolicy)

KEY = 'a-test-key-that-is-long-enough'
OTHER_KEY = 'a-different-key-also-long-enough'


def strategy(name, key=KEY, domain='test', **options):
    return STRATEGIES[name](KeyedHash(key, domain), STRATEGIES[name].validateOptions(options))


def maskOne(name, value, **options):
    return strategy(name, **options).maskColumn([value], 0)[0]


# --- the keyed hash ---------------------------------------------------------

def test_the_same_value_masks_the_same_way_every_time():
    assert maskOne('hash', 'alice') == maskOne('hash', 'alice')


def test_the_same_value_masks_the_same_way_in_the_same_domain_across_columns():
    """The property referential consistency rests on: domain, not column, decides."""
    first = strategy('key', domain='customer').maskColumn([41, 42], 0)
    second = strategy('key', domain='customer').maskColumn([42, 41], 0)

    assert first == list(reversed(second))


def test_different_domains_mask_differently():
    assert strategy('hash', domain='a').maskColumn(['alice'], 0) != strategy('hash', domain='b').maskColumn(['alice'], 0)


def test_a_different_key_changes_every_mask():
    values = ['alice', 'bob', 'carol']

    assert all(left != right for left, right in zip(strategy('hash').maskColumn(values, 0), strategy('hash', key=OTHER_KEY).maskColumn(values, 0)))


def test_an_integer_and_its_decimal_and_text_forms_mask_identically():
    """Drivers disagree on numeric types -- int, Decimal, a whole float -- and some schemas store ids as text."""
    masked = {maskOne('hash', 42), maskOne('hash', decimal.Decimal('42')), maskOne('hash', decimal.Decimal('42.00')), maskOne('hash', 42.0), maskOne('hash', '42')}

    assert len(masked) == 1


@pytest.mark.parametrize('size', [1, 2, 3, 10, 17, 100, 256, 1000])
def test_permute_is_a_bijection(size):
    keyedHash = KeyedHash(KEY, 'permute')

    assert sorted(keyedHash.permute(size, value) for value in range(size)) == list(range(size))


def test_permute_handles_domains_wider_than_one_digest():
    keyedHash = KeyedHash(KEY, 'wide')
    size = 62 ** 60

    outputs = {keyedHash.permute(size, value) for value in range(200)}

    assert len(outputs) == 200
    assert all(0 <= output < size for output in outputs)


def test_the_key_fingerprint_is_stable_and_does_not_reveal_the_key():
    assert keyFingerprint(KEY) == keyFingerprint(KEY)
    assert keyFingerprint(KEY) != keyFingerprint(OTHER_KEY)
    assert len(keyFingerprint(KEY)) == 12
    assert KEY not in keyFingerprint(KEY)


# --- NULLs and the simple strategies ----------------------------------------

@pytest.mark.parametrize('name', ['hash', 'email', 'digits', 'number', 'dateShift', 'fakeName', 'key', 'keep'])
def test_null_passes_through(name):
    assert strategy(name).maskColumn([None], 0) == [None]


def test_keep_leaves_values_alone():
    assert strategy('keep').maskColumn(['a', 1, None], 0) == ['a', 1, None]


def test_null_replaces_everything():
    assert strategy('null').maskColumn(['a', 1, None], 0) == [None, None, None]


def test_constant_replaces_nulls_too():
    assert strategy('constant', value='redacted').maskColumn(['a', None], 0) == ['redacted', 'redacted']


def test_constant_requires_a_value():
    with pytest.raises(ValueError, match='requires option'):
        validateColumnPolicy({'strategy': 'constant'})


# --- hash ---------------------------------------------------------------------

def test_hash_honours_length_and_prefix():
    masked = maskOne('hash', 'alice', length=20, prefix='cust_')

    assert masked.startswith('cust_')
    assert len(masked) == 25
    assert int(masked[5:], 16) >= 0


def test_hash_refuses_a_length_too_short_to_stay_unique():
    with pytest.raises(ValueError, match='between 12 and 64'):
        validateColumnPolicy({'strategy': 'hash', 'length': 8})


# --- email -------------------------------------------------------------------

def test_email_stays_shaped_like_an_email():
    masked = maskOne('email', 'Alice.Smith@corp.com')

    local, domain = masked.split('@')
    assert domain == 'example.test'
    assert local.startswith('u') and len(local) == 13


def test_email_is_case_insensitive():
    assert maskOne('email', 'Alice@Corp.com') == maskOne('email', 'alice@corp.com ')


def test_email_can_keep_or_replace_the_domain():
    assert maskOne('email', 'alice@corp.com', keepDomain=True).endswith('@corp.com')
    assert maskOne('email', 'alice@corp.com', mailDomain='masked.invalid').endswith('@masked.invalid')


def test_email_rejects_both_domain_options():
    with pytest.raises(ValueError, match='not both'):
        validateColumnPolicy({'strategy': 'email', 'keepDomain': True, 'mailDomain': 'x.test'})


def test_email_rejects_a_non_text_value_without_echoing_it():
    with pytest.raises(MaskingError) as error:
        maskOne('email', 5551234)

    assert '5551234' not in str(error.value)


# --- digits ------------------------------------------------------------------

def test_digits_keeps_the_format():
    masked = maskOne('digits', '+1 (555) 010-9999')

    assert len(masked) == len('+1 (555) 010-9999')
    assert [character.isdigit() for character in masked] == [character.isdigit() for character in '+1 (555) 010-9999']
    assert masked[0] == '+' and masked[3] == '(' and masked[7:9] == ') '
    assert masked != '+1 (555) 010-9999'


def test_digits_masks_the_same_number_the_same_way_however_it_is_formatted():
    plain = maskOne('digits', '5550109999')
    formatted = maskOne('digits', '(555) 010-9999')

    assert ''.join(character for character in formatted if character.isdigit()) == plain


def test_digits_can_keep_leading_and_trailing_digits():
    masked = maskOne('digits', '4111 1111 1111 1234', keepLeading=1, keepTrailing=4)

    assert masked.startswith('4')
    assert masked.endswith('1234')


def test_digits_keeps_an_integers_digit_count_and_sign():
    for value in (7, 10, 5550109999, -12345):
        masked = maskOne('digits', value)
        assert isinstance(masked, int)
        assert len(str(abs(masked))) == len(str(abs(value)))
        assert (masked < 0) == (value < 0)


def test_digits_leaves_text_without_digits_alone():
    assert maskOne('digits', 'n/a') == 'n/a'


# --- number ------------------------------------------------------------------

def test_number_keeps_an_int_an_int():
    assert isinstance(maskOne('number', 1234), int)


def test_number_keeps_a_decimals_precision():
    """NUMERIC columns arrive as Decimal from PostgreSQL, MySQL and SQL Server;
    the old random masking crashed on them, and on floats.
    """
    masked = maskOne('number', decimal.Decimal('1234.56'))

    assert isinstance(masked, decimal.Decimal)
    assert masked.as_tuple().exponent == -2


def test_number_handles_an_integral_decimal():
    masked = maskOne('number', decimal.Decimal('1234'))

    assert isinstance(masked, decimal.Decimal)
    assert masked == masked.to_integral_value()


def test_number_keeps_a_float_a_float():
    assert isinstance(maskOne('number', 12.5), float)


def test_number_stays_within_its_variance():
    masked = strategy('number', variance=0.1).maskColumn([decimal.Decimal('200.00')] + [decimal.Decimal(value) for value in range(100, 200)], 0)

    assert decimal.Decimal('180') <= masked[0] <= decimal.Decimal('220')


def test_number_stays_within_its_range():
    masked = strategy('number', min=18, max=90).maskColumn(list(range(1000)), 0)

    assert all(18 <= value <= 90 for value in masked)
    assert len(set(masked)) > 30


def test_number_respects_a_range_whose_bounds_are_off_the_values_grid():
    masked = strategy('number', min=0.005, max=0.015).maskColumn([decimal.Decimal('1.00')] * 1 + [decimal.Decimal(value) / 7 for value in range(1, 50)], 0)

    assert all(decimal.Decimal('0.005') <= value <= decimal.Decimal('0.015') for value in masked[:1])


def test_number_decimals_overrides_the_precision():
    masked = maskOne('number', 12.3456, decimals=1)

    assert masked == round(masked, 1)


def test_number_passes_non_finite_values_through():
    assert maskOne('number', decimal.Decimal('NaN')).is_nan()
    assert maskOne('number', float('inf')) == float('inf')


@pytest.mark.parametrize('value', ['12', True, datetime.date(2026, 1, 1)])
def test_number_rejects_what_is_not_a_number(value):
    with pytest.raises(MaskingError, match='needs a number'):
        maskOne('number', value)


@pytest.mark.parametrize('options,message', [
    ({'min': 1}, 'both min and max'),
    ({'min': 5, 'max': 5}, 'min below max'),
    ({'min': 1, 'max': 5, 'variance': 0.1}, 'not both'),
    ({'variance': 2}, 'at most 1'),
    ({'variance': 'lots'}, 'must be a number'),
    ])
def test_number_rejects_incoherent_options(options, message):
    with pytest.raises(ValueError, match=message):
        validateColumnPolicy(dict(options, strategy='number'))


# --- dateShift ---------------------------------------------------------------

def test_date_shift_moves_a_date_by_a_bounded_nonzero_number_of_days():
    dates = [datetime.date(1990, 1, 1) + datetime.timedelta(days=offset) for offset in range(500)]
    masked = strategy('dateShift', maxDays=10).maskColumn(dates, 0)

    shifts = {(after - before).days for before, after in zip(dates, masked)}

    assert 0 not in shifts
    assert shifts <= set(range(-10, 11))
    assert len(shifts) > 10


def test_date_shift_keeps_a_timestamps_time_of_day():
    value = datetime.datetime(2026, 3, 4, 13, 14, 15, 161718)
    masked = maskOne('dateShift', value)

    assert isinstance(masked, datetime.datetime)
    assert masked.time() == value.time()


@pytest.mark.parametrize('text', ['2026-03-04', '2026-03-04 13:14:15', '2026-03-04T13:14:15', '2026-03-04 13:14:15.000000', '2026-03-04 13:14'])
def test_date_shift_writes_iso_text_back_in_the_same_shape(text):
    masked = maskOne('dateShift', text)

    assert len(masked) == len(text)
    assert masked[10:] == text[10:]
    assert masked != text


def test_date_shift_rejects_text_that_is_not_a_date_without_echoing_it():
    with pytest.raises(MaskingError) as error:
        maskOne('dateShift', 'Springfield')

    assert 'Springfield' not in str(error.value)


# --- fake --------------------------------------------------------------------

def test_fake_values_come_from_the_bundled_lists():
    first, last = maskOne('fakeName', 'Real Person').split(' ')

    assert first in FIRST_NAMES and last in LAST_NAMES
    assert maskOne('fakeFirstName', 'Real') in FIRST_NAMES


def test_fake_values_respect_max_length():
    assert len(maskOne('fakeStreetAddress', '1 Real Street', maxLength=5)) <= 5


@pytest.mark.parametrize('name', ['fakeCity', 'fakeCompany', 'fakeLastName', 'fakeStreetAddress'])
def test_fake_values_are_deterministic(name):
    assert maskOne(name, 'input') == maskOne(name, 'input')


# --- key ---------------------------------------------------------------------

@pytest.mark.parametrize('values', [range(10), range(10, 100), range(100, 1000)])
def test_key_permutes_each_digit_length_onto_itself(values):
    masked = strategy('key').maskColumn(list(values), 0)

    assert sorted(masked) == list(values)


def test_key_keeps_the_sign_of_an_integer():
    masked = strategy('key').maskColumn(list(range(-500, 500)), 0)

    assert len(set(masked)) == 1000
    assert all((after < 0) == (before < 0) for before, after in zip(range(-500, 500), masked))


def test_key_preserves_an_integral_decimals_type():
    masked = maskOne('key', decimal.Decimal('123456'))

    assert isinstance(masked, decimal.Decimal)
    assert len(str(masked)) == 6


def test_key_rejects_a_fractional_decimal():
    with pytest.raises(MaskingError, match='whole number'):
        maskOne('key', decimal.Decimal('1.5'))


def test_key_is_one_to_one_on_short_codes():
    codes = ['{}{}{}'.format(first, second, third) for first in 'aB' for second in '0123456789' for third in 'xyzXYZ']
    masked = strategy('key').maskColumn(codes, 0)

    assert len(set(masked)) == len(codes)
    for before, after in zip(codes, masked):
        assert [character.isdigit() for character in before] == [character.isdigit() for character in after]
        assert [character.isupper() for character in before] == [character.isupper() for character in after]


def test_key_keeps_characters_it_does_not_mask():
    masked = maskOne('key', 'AB-1234/x')

    assert masked[2] == '-' and masked[7] == '/'


def test_key_with_the_digits_charset_leaves_letters_alone():
    masked = maskOne('key', 'SSN 123-45-6789', charset='digits')

    assert masked.startswith('SSN ')
    assert masked[7] == '-' and masked[10] == '-'


def test_key_with_the_hex_charset_keeps_a_uuid_a_uuid():
    value = str(uuid.uuid4())
    masked = maskOne('key', value, charset='hex')

    assert uuid.UUID(masked)
    assert masked != value


def test_key_keeps_a_uuid_object_a_uuid():
    value = uuid.uuid4()

    assert isinstance(maskOne('key', value), uuid.UUID)


def test_key_masks_a_uuid_object_and_its_text_consistently():
    value = uuid.uuid4()

    assert str(maskOne('key', value)) == maskOne('key', str(value), charset='hex')


def test_key_rejects_bool_and_float():
    for value in (True, 1.5):
        with pytest.raises(MaskingError):
            maskOne('key', value)


def test_key_is_consistent_between_the_two_ends_of_a_foreign_key():
    parents = strategy('key', domain='customer').maskColumn([1001, 1002, 1003], 0)
    children = strategy('key', domain='customer').maskColumn([1003, 1001, 1001], 0)

    assert children == [parents[2], parents[0], parents[0]]


# --- shuffle -----------------------------------------------------------------

def test_shuffle_keeps_the_values_and_is_reproducible_per_chunk():
    values = list(range(50))

    first = strategy('shuffle').maskColumn(values, 0)
    again = strategy('shuffle').maskColumn(values, 0)
    nextChunk = strategy('shuffle').maskColumn(values, 1)

    assert sorted(first) == values
    assert first == again
    assert first != values
    assert first != nextChunk


# --- policies ----------------------------------------------------------------

def test_a_policy_may_be_just_a_strategy_name():
    assert validateColumnPolicy('email') == {'strategy': 'email'}


@pytest.mark.parametrize('policy,message', [
    ('scramble', 'unknown strategy'),
    ({'strategy': ['hash']}, 'unknown strategy'),
    ({'strategy': 'hash', 'size': 3}, 'does not take option'),
    ({'strategy': 'hash', 'domain': ''}, 'domain'),
    (42, 'strategy name or a mapping'),
    ({'strategy': 'key', 'charset': 'emoji'}, 'must be one of'),
    ({'strategy': 'dateShift', 'maxDays': 0}, 'between 1'),
    ])
def test_invalid_policies_are_rejected(policy, message):
    with pytest.raises(ValueError, match=message):
        validateColumnPolicy(policy)


def test_a_short_key_is_rejected():
    with pytest.raises(ValueError, match='at least 16'):
        MaskingPlan(key='short', columns={})


def test_binding_fails_on_a_column_the_policy_does_not_cover():
    plan = MaskingPlan(key=KEY, columns={'id': 'keep'})

    with pytest.raises(MaskingError, match='not in the masking policy: ssn'):
        plan.bind(['id', 'ssn'])


def test_binding_fails_on_a_policy_column_the_query_does_not_return():
    plan = MaskingPlan(key=KEY, columns={'id': 'keep', 'emial': 'email'})

    with pytest.raises(MaskingError, match='does not return: emial'):
        plan.bind(['id'])


def test_default_strategy_covers_unlisted_columns():
    bound = MaskingPlan(key=KEY, columns={'id': 'keep'}, defaultStrategy='null').bind(['id', 'notes'])

    assert bound.apply([(1, 'secret')]) == [(1, None)]
    assert [entry.source for entry in bound.manifest] == ['column', 'defaultStrategy']


def test_columns_match_case_insensitively():
    """Oracle reports unquoted identifiers in upper case."""
    bound = MaskingPlan(key=KEY, columns={'id': 'keep', 'email': 'email'}).bind(['ID', 'EMAIL'])

    assert bound.apply([(1, 'a@b.com')])[0][1].endswith('@example.test')


def test_a_policy_naming_one_column_twice_in_different_case_is_rejected():
    with pytest.raises(ValueError, match='differ only in case'):
        MaskingPlan(key=KEY, columns={'email': 'email', 'EMAIL': 'keep'})


def test_the_domain_defaults_to_the_lower_cased_column_name():
    bound = MaskingPlan(key=KEY, columns={'Email': 'email', 'id': 'keep'}).bind(['Email', 'id'])

    assert [entry.domain for entry in bound.manifest] == ['email', None]


def test_a_masking_error_names_the_column_but_not_the_value():
    bound = MaskingPlan(key=KEY, columns={'amount': 'number'}).bind(['amount'])

    with pytest.raises(MaskingError) as error:
        bound.apply([('4111111111111111',)])

    assert 'amount' in str(error.value)
    assert '4111111111111111' not in str(error.value)


def test_an_all_keep_policy_returns_rows_unchanged():
    bound = MaskingPlan(key=KEY, columns={'a': 'keep', 'b': 'keep'}).bind(['a', 'b'])

    assert bound.apply([(1, 2)]) == [(1, 2)]


# --- configuration -----------------------------------------------------------

def _jobsFile(masking):
    return {'workers': 1, 'jobs': {'job': {
        'active': True, 'sourceDatabase': 's', 'sourceQuery': 'select * from t', 'targetDatabase': 't',
        'targetTableFinal': 't', 'insertStrategy': 'upsert', 'chunkSize': 10, 'masking': masking,
        }}}


def test_a_masked_job_validates_and_normalizes_its_policy():
    jobsFile = Configuration.validateJobConfiguration(_jobsFile({'key': KEY, 'columns': {'id': 'keep', 'total': {'strategy': 'number', 'min': 1, 'max': 9}}}), DataJobsFile)

    masking = jobsFile.jobs['job'].masking
    assert masking.columns['id'] == {'strategy': 'keep'}
    assert masking.columns['total']['min'] == decimal.Decimal(1)


def test_the_key_never_appears_in_a_repr_or_a_validation_error():
    jobsFile = Configuration.validateJobConfiguration(_jobsFile({'key': KEY, 'columns': {'id': 'keep'}}), DataJobsFile)

    assert KEY not in repr(jobsFile)

    with pytest.raises(ConfigurationError) as error:
        Configuration.validateJobConfiguration(_jobsFile({'key': 'tooShortSecret', 'columns': {'id': 'bogus'}}), DataJobsFile)

    assert 'tooShortSecret' not in str(error.value)
    assert 'at least 16' in str(error.value)
    assert 'unknown strategy' in str(error.value)


# --- manifest ----------------------------------------------------------------

def test_the_manifest_records_completed_failed_and_skipped_masked_jobs():
    declared = {name: {'targetTable': name, 'keyFingerprint': keyFingerprint(KEY)} for name in ('done', 'broken', 'waiting')}
    applied = {'columns': [{'column': 'id', 'strategy': 'key', 'domain': 'customer', 'source': 'column'}]}
    outcomes = [
        JobOutcome(job='done', status=JobStatus.COMPLETED, rowCount=5, masking=applied),
        JobOutcome(job='broken', status=JobStatus.FAILED),
        JobOutcome(job='waiting', status=JobStatus.SKIPPED),
        JobOutcome(job='unmasked', status=JobStatus.COMPLETED, rowCount=9),
        ]

    manifest = buildMaskingManifest(outcomes, declared, generatedAt=datetime.datetime(2026, 9, 16, tzinfo=datetime.timezone.utc))

    assert manifest['generatedAt'] == '2026-09-16T00:00:00+00:00'
    assert [(job['job'], job['status'], job['rowCount']) for job in manifest['jobs']] == [('done', 'completed', 5), ('broken', 'failed', 0), ('waiting', 'skipped', 0)]
    assert manifest['jobs'][0]['columns'] == applied['columns']
    assert manifest['jobs'][1]['columns'] == []
    assert KEY not in str(manifest)


def _manifest():
    return {'generatedAt': '2026-09-16T00:00:00+00:00', 'jobs': [{'job': 'maskCustomers', 'status': 'completed', 'rowCount': 3,
                                                                 'columns': [{'column': 'email', 'strategy': 'email'}]}]}


def test_a_sealed_manifest_verifies_until_it_is_changed():
    import json
    from understudy_data.masking import sealManifest, verifyManifest

    sealed = json.loads(json.dumps(sealManifest(_manifest()), indent=4))

    assert verifyManifest(sealed) == (True, False, False, None)

    sealed['jobs'][0]['rowCount'] = 4
    assert verifyManifest(sealed).digestValid is False


def test_a_signed_manifest_verifies_only_with_its_key():
    from understudy_data.masking import keyFingerprint, sealManifest, verifyManifest

    key = 'a-manifest-signing-key'
    sealed = sealManifest(_manifest(), signingKey=key)

    assert sealed['integrity']['signingKeyFingerprint'] == keyFingerprint(key)
    assert verifyManifest(sealed, signingKey=key) == (True, True, True, keyFingerprint(key))
    assert verifyManifest(sealed, signingKey='another-signing-key').signatureValid is False
    assert verifyManifest(sealed).signatureValid is False


def test_a_forged_signature_does_not_verify():
    """Anyone can recompute a digest after editing; only the key can re-sign."""
    import hashlib
    from understudy_data.masking import _canonicalManifest, sealManifest, verifyManifest

    key = 'a-manifest-signing-key'
    sealed = sealManifest(_manifest(), signingKey=key)
    sealed['jobs'][0]['status'] = 'failed'
    sealed['integrity']['digest'] = hashlib.sha256(_canonicalManifest(sealed)).hexdigest()

    verification = verifyManifest(sealed, signingKey=key)

    assert verification.digestValid is True
    assert verification.signatureValid is False


def test_a_manifest_without_an_integrity_section_cannot_be_verified():
    from understudy_data.masking import verifyManifest

    with pytest.raises(ValueError, match='no integrity section'):
        verifyManifest(_manifest())


def test_a_signing_key_must_be_long_enough():
    from understudy_data.masking import sealManifest

    with pytest.raises(ValueError, match='at least 16'):
        sealManifest(_manifest(), signingKey='short')


GOLDEN_KEY = 'a-golden-value-masking-key'
FAKE_STRATEGIES = ['fakeFirstName', 'fakeLastName', 'fakeName', 'fakeCity', 'fakeCompany', 'fakeStreetAddress']


def test_fake_values_without_a_locale_are_unchanged_by_locale_support():
    """Captured before locales existed. A mask that changes between versions
    breaks every copy already loaded with it.
    """
    bound = MaskingPlan(GOLDEN_KEY, {name: name for name in FAKE_STRATEGIES}).bind(FAKE_STRATEGIES)

    assert bound.apply([('ann@example.test',) * 6]) == [('Elena', 'Singh', 'Quinn Becker', 'Elmstead', 'Acorn Holdings', '1137 Spring Avenue')]


@pytest.mark.parametrize('locale', ['de_DE', 'fr_FR', 'es_ES', 'pt_BR', 'it_IT', 'nl_NL', 'en_US', 'en_GB'])
def test_a_locale_draws_from_its_own_lists(locale):
    from understudy_data.masking import LOCALES

    policy = {name: {'strategy': name, 'locale': locale} for name in FAKE_STRATEGIES}
    bound = MaskingPlan(GOLDEN_KEY, policy).bind(FAKE_STRATEGIES)
    lists = LOCALES[locale]

    for value in ('ann@example.test', 'bo@example.test', 42):
        first, last, full, city, company, address = bound.apply([(value,) * 6])[0]
        assert first in lists.firstNames and last in lists.lastNames and city in lists.cities
        assert any(full == '{} {}'.format(a, b) for a in lists.firstNames for b in lists.lastNames)
        assert any(company.endswith(' ' + suffix) for suffix in lists.companySuffixes)
        assert any(street in address for street in lists.streets) and any(character.isdigit() for character in address)


def test_locale_address_layouts_differ():
    policy = {'de': {'strategy': 'fakeStreetAddress', 'locale': 'de_DE'}, 'fr': {'strategy': 'fakeStreetAddress', 'locale': 'fr_FR'}}
    german, french = MaskingPlan(GOLDEN_KEY, policy).bind(['de', 'fr']).apply([('x', 'x')])[0]

    assert german.split()[-1].isdigit()
    assert french.split()[0].isdigit()


def test_an_unknown_locale_is_refused():
    with pytest.raises(ValueError, match='locale: must be one of'):
        validateColumnPolicy({'strategy': 'fakeName', 'locale': 'xx_XX'})


def _fpe(policy, values):
    pytest.importorskip('cryptography')
    return [row[0] for row in MaskingPlan(GOLDEN_KEY, {'c': policy}).bind(['c']).apply([(value,) for value in values])]


def test_fpe_is_one_to_one_and_keeps_an_integers_digit_count():
    values = list(range(999_000, 1_001_000)) + list(range(-1_000_500, -999_500))
    masked = _fpe('fpe', values)

    assert len(set(masked)) == len(values)
    assert all(len(str(abs(a))) == len(str(abs(b))) and (a < 0) == (b < 0) for a, b in zip(values, masked))


def test_fpe_masks_values_too_short_for_ff1_one_to_one_too():
    values = list(range(-999, 1000))
    masked = _fpe('fpe', values)

    assert len(set(masked)) == len(values)
    assert all(len(str(abs(a))) == len(str(abs(b))) for a, b in zip(values, masked))


def test_fpe_keeps_a_texts_shape():
    phone, code, token = _fpe({'strategy': 'fpe', 'charset': 'digits'}, ['+1 (555) 010-9999']) + _fpe('fpe', ['AB-12cd9']) + \
        _fpe({'strategy': 'fpe', 'charset': 'hex'}, ['DEADBEEF-00'])

    assert re.fullmatch(r'\+\d \(\d{3}\) \d{3}-\d{4}', phone) and phone != '+1 (555) 010-9999'
    assert re.fullmatch(r'[0-9A-Za-z]{2}-[0-9A-Za-z]{5}', code) and code != 'AB-12cd9'
    assert re.fullmatch(r'[0-9A-F]{8}-[0-9A-F]{2}', token)


def test_fpe_keeps_a_uuid_a_uuid():
    value = uuid.UUID('12345678-1234-5678-1234-567812345678')
    (masked,) = _fpe('fpe', [value])

    assert isinstance(masked, uuid.UUID) and masked != value


def test_fpe_is_deterministic_and_depends_on_the_domain():
    pytest.importorskip('cryptography')
    plan = MaskingPlan(GOLDEN_KEY, {'a': {'strategy': 'fpe', 'domain': 'customer'}, 'b': {'strategy': 'fpe', 'domain': 'customer'},
                                    'c': {'strategy': 'fpe', 'domain': 'order'}})
    a, b, c = plan.bind(['a', 'b', 'c']).apply([(1234567, 1234567, 1234567)])[0]

    assert a == b != c


@pytest.mark.parametrize('value', [True, 1.5, decimal.Decimal('1.5'), datetime.date(2026, 1, 1)])
def test_fpe_refuses_what_it_cannot_shape(value):
    with pytest.raises(MaskingError, match='fpe'):
        _fpe('fpe', [value])


def test_a_custom_strategy_is_named_by_module_and_class():
    bound = MaskingPlan(GOLDEN_KEY, {'name': {'strategy': 'tests.customStrategies:Initials', 'separator': '-'}}).bind(['name'])

    (masked,) = bound.apply([('Ann Lee',)])[0]

    assert re.fullmatch(r'A-L-[0-9a-f]{4}', masked)
    assert bound.manifest[0].strategy == 'tests.customStrategies:Initials'
    assert bound.manifest[0].domain == 'name'


@pytest.mark.parametrize('reference,message', [
    ('tests.customStrategies:Missing', 'could not be imported'),
    ('tests.noSuchModule:Initials', 'could not be imported'),
    ('tests.customStrategies:NotAStrategy', 'is not a subclass'),
    ])
def test_a_bad_custom_strategy_reference_is_refused(reference, message):
    with pytest.raises(ValueError, match=message):
        validateColumnPolicy(reference)


def test_a_custom_strategy_checks_its_own_options():
    with pytest.raises(ValueError, match='strategy "tests.customStrategies:Initials" does not take option'):
        validateColumnPolicy({'strategy': 'tests.customStrategies:Initials', 'colour': 'red'})


def test_strict_fpe_refuses_a_value_too_short_for_ff1():
    strict = {'strategy': 'fpe', 'strict': True}

    assert len(str(_fpe(strict, [1234567])[0])) == 7

    with pytest.raises(MaskingError, match='strict, and FF1 needs at least 6 digits') as excinfo:
        _fpe(strict, [12345])
    assert '12345' not in str(excinfo.value)

    with pytest.raises(MaskingError, match='at least 4 alphanumeric characters'):
        _fpe(strict, ['ab-1'])


def test_strict_must_be_a_boolean():
    with pytest.raises(ValueError, match='strict: must be true or false'):
        validateColumnPolicy({'strategy': 'fpe', 'strict': 'yes'})


SAMPLE_TEXT = ('Called Ann at +1 (555) 010-9999, email Ann.Lee@corp.example.com; card 4111 1111 1111 1111 '
               'SSN 123-45-6789 IBAN GB82 WEST 1234 5698 7654 32 from 192.168.1.20. '
               'Order 2026-01-02 or 02/01/2026, qty 12, v1.2.3.')


def _redact(policy, values):
    return [row[0] for row in MaskingPlan(GOLDEN_KEY, {'notes': policy}).bind(['notes']).apply([(value,) for value in values])]


def test_redact_labels_each_identifier_and_keeps_the_rest():
    (redacted,) = _redact('redact', [SAMPLE_TEXT])

    assert redacted == ('Called Ann at [PHONE], email [EMAIL]; card [CARD] SSN [SSN] IBAN [IBAN] from [IP]. '
                        'Order 2026-01-02 or 02/01/2026, qty 12, v1.2.3.')


def test_redact_mask_mode_writes_consistent_values_of_the_same_shape():
    first, second = _redact({'strategy': 'redact', 'replacement': 'mask'}, [SAMPLE_TEXT, 'reach me at ann.lee@CORP.example.com'])

    for secret in ('555', '010-9999', 'Ann.Lee', '4111 1111 1111 1111', '123-45-6789', '1234 5698', '192.168'):
        assert secret not in first
    assert re.search(r'\+\d \(\d{3}\) \d{3}-\d{4}', first)
    assert re.search(r'card \d{4} \d{4} \d{4} 1111 ', first)
    assert re.search(r'SSN \d{3}-\d{2}-\d{4} ', first)
    assert re.search(r'IBAN GB[0-9A-Z]{2} [0-9A-Z]{4} ', first)
    assert re.search(r'from 10\.\d+\.\d+\.\d+\.', first)
    assert first.split('email ')[1].split(';')[0] == second.split('at ')[1]
    assert 'Order 2026-01-02 or 02/01/2026, qty 12, v1.2.3.' in first


@pytest.mark.parametrize('text', [
    'card 4111 1111 1111 1112',     # fails the Luhn check
    'IBAN GB82 WEST 1234 5698 7654 33',  # fails the mod-97 check
    'address 999.1.1.1',
    'build 20260102',
    'call 12345',
    ])
def test_redact_leaves_lookalikes_that_fail_their_checks(text):
    (redacted,) = _redact({'strategy': 'redact', 'detect': ['card', 'iban', 'ip', 'ssn', 'email']}, [text])

    assert redacted == text


def test_redact_can_be_narrowed_and_extended():
    (redacted,) = _redact({'strategy': 'redact', 'detect': ['email'], 'patterns': [r'ACC-\d{6}']},
                          ['ann@example.com called about ACC-123456 from +1 555 010 9999'])

    assert redacted == '[EMAIL] called about [REDACTED] from +1 555 010 9999'


@pytest.mark.parametrize('policy,message', [
    ({'strategy': 'redact', 'detect': ['names']}, 'detect: must be a non-empty list of'),
    ({'strategy': 'redact', 'patterns': ['(unclosed']}, 'is not a valid regular expression'),
    ({'strategy': 'redact', 'replacement': 'blank'}, 'replacement: must be one of'),
    ])
def test_redact_options_are_checked(policy, message):
    with pytest.raises(ValueError, match=message):
        validateColumnPolicy(policy)


def test_redact_needs_text():
    with pytest.raises(MaskingError, match='redact strategy needs text, got int'):
        _redact('redact', [5])
