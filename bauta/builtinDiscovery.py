"""Discovery rules that ship with the package: what personal data looks like,
by column name and by sampled value.

`discover` proposes masking policies from them, `audit` questions an unmasked
column with them, and `synthesize` chooses realistic values by them. A
discovery.yaml adds rules of your own, checked before these, and can leave
any of these out by name -- see discovery.py, which combines the two.

Each rule has a name, shared by its name and value forms, so leaving out
`phone` leaves out both. The first rule that matches wins, so specific rules
come first.
"""
from __future__ import annotations

import datetime
import re
from typing import Any, Callable, Dict, Tuple

EMAIL = re.compile(r'^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$')
PHONE = re.compile(r'^\+?[\d\s().-]{7,}$')
NATIONAL_ID = re.compile(r'^\d{3}-\d{2}-\d{4}$')
CARD = re.compile(r'^[\d -]{13,23}$')
IPV4 = re.compile(r'^(\d{1,3}\.){3}\d{1,3}$')
UUID_TEXT = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')


def _luhn(digits: str) -> bool:

    total = 0
    for position, character in enumerate(reversed(digits)):
        digit = int(character)
        if position % 2:
            digit = digit * 2 - 9 if digit > 4 else digit * 2
        total += digit

    return total % 10 == 0


def isCard(text: str) -> bool:

    digits = re.sub(r'\D', '', text)

    return bool(CARD.match(text)) and 13 <= len(digits) <= 19 and _luhn(digits)


def isIsoDate(text: str) -> bool:

    try:
        datetime.datetime.fromisoformat(text.strip())
    except ValueError:
        return False

    return True


def isPhone(text: str) -> bool:

    return bool(PHONE.match(text)) and len(re.sub(r'\D', '', text)) >= 7 and not text.isdigit()


# (name, words any of which must be in the column name, policy, reason).
# Column names are matched split on underscores and camelCase and joined
# whole, so `first_name` and `FIRSTNAME` both match `firstname`.
NAME_RULES: Tuple[Tuple[str, Tuple[str, ...], Dict[str, Any], str], ...] = (
    ('email', ('email', 'emailaddress', 'mail'), {'strategy': 'email'}, 'name suggests an email address'),
    ('credential', ('password', 'passwd', 'pwd', 'secret', 'token', 'apikey', 'salt'), {'strategy': 'hash'}, 'name suggests a credential'),
    ('nationalId', ('ssn', 'socialsecurity', 'socialsecuritynumber', 'nationalid', 'taxid', 'tin', 'passport', 'passportnumber',
                    'licensenumber', 'licencenumber', 'driverslicense'),
     {'strategy': 'key'}, 'name suggests a government identifier; key keeps it unique and shaped'),
    ('card', ('creditcard', 'cardnumber', 'ccnumber', 'pan'), {'strategy': 'digits', 'keepTrailing': 4}, 'name suggests a card number'),
    ('bankAccount', ('iban', 'accountnumber', 'routingnumber', 'bankaccount', 'sortcode'), {'strategy': 'digits'}, 'name suggests a bank account'),
    ('phone', ('phone', 'phonenumber', 'mobile', 'cell', 'fax', 'telephone', 'tel'), {'strategy': 'digits'}, 'name suggests a phone number'),
    ('firstName', ('firstname', 'givenname', 'forename'), {'strategy': 'fakeFirstName'}, 'name suggests a first name'),
    ('lastName', ('lastname', 'surname', 'familyname'), {'strategy': 'fakeLastName'}, 'name suggests a last name'),
    ('fullName', ('fullname', 'contactname', 'customername', 'displayname', 'personname', 'employeename'), {'strategy': 'fakeName'},
     'name suggests a person\'s name'),
    ('userName', ('username', 'login', 'handle', 'screenname'), {'strategy': 'key'}, 'name suggests a user name; key keeps it unique'),
    ('company', ('company', 'companyname', 'employer', 'organization', 'organisation'), {'strategy': 'fakeCompany'}, 'name suggests a company'),
    ('ip', ('ip', 'ipaddress', 'ipaddr'), {'strategy': 'hash'}, 'name suggests an IP address'),
    ('streetAddress', ('street', 'address', 'addressline', 'addr', 'line1', 'line2', 'streetaddress'), {'strategy': 'fakeStreetAddress'},
     'name suggests a street address'),
    ('city', ('city', 'town'), {'strategy': 'fakeCity'}, 'name suggests a city'),
    ('postalCode', ('zip', 'zipcode', 'postal', 'postalcode', 'postcode'), {'strategy': 'digits'}, 'name suggests a postal code'),
    ('birthDate', ('birth', 'birthdate', 'dob', 'birthday', 'dateofbirth'), {'strategy': 'dateShift', 'maxDays': 30},
     'name suggests a date of birth'),
    ('compensation', ('salary', 'income', 'wage', 'wages', 'compensation', 'bonus'), {'strategy': 'number', 'variance': 0.1},
     'name suggests compensation'),
    ('coordinate', ('latitude', 'longitude', 'lat', 'lng', 'lon'), {'strategy': 'number', 'variance': 0.01}, 'name suggests a coordinate'),
    ('sensitiveAttribute', ('gender', 'sex', 'race', 'ethnicity', 'religion', 'nationality'), {'strategy': 'shuffle'},
     'name suggests a sensitive attribute; shuffle keeps the distribution but is not anonymization'),
    ('freeText', ('note', 'notes', 'comment', 'comments', 'description', 'remarks', 'memo', 'bio', 'message', 'body', 'freetext'),
     {'strategy': 'null'}, 'name suggests free text, which can hold PII anywhere; redact keeps the text but only removes identifiers with a known shape'),
    )

# (name, test on one stripped text value, policy, reason). A column matches when
# enough of its sampled text values pass.
VALUE_RULES: Tuple[Tuple[str, Callable[[str], bool], Dict[str, Any], str], ...] = (
    ('email', lambda text: bool(EMAIL.match(text)), {'strategy': 'email'}, 'sampled values look like email addresses'),
    ('nationalId', lambda text: bool(NATIONAL_ID.match(text)), {'strategy': 'key', 'charset': 'digits'},
     'sampled values look like national identifiers'),
    ('card', isCard, {'strategy': 'digits', 'keepTrailing': 4}, 'sampled values look like card numbers'),
    ('ip', lambda text: bool(IPV4.match(text)), {'strategy': 'hash'}, 'sampled values look like IP addresses'),
    ('uuid', lambda text: bool(UUID_TEXT.match(text)), {'strategy': 'keep'}, 'sampled values are UUIDs, usually surrogate keys -- review'),
    # Before phone numbers, which ISO dates would otherwise pass for.
    ('date', isIsoDate, {'strategy': 'keep'}, 'sampled values are dates -- review whether they identify anyone'),
    ('phone', isPhone, {'strategy': 'digits'}, 'sampled values look like phone numbers'),
    )

# Words in a table's name that say its rows are people, so a bare `name`
# column in it is a person's name.
PERSONAL_TABLE_WORDS = frozenset({'customer', 'customers', 'user', 'users', 'person', 'people', 'employee', 'employees', 'contact',
                                  'contacts', 'member', 'members', 'patient', 'patients', 'client', 'clients', 'account', 'accounts',
                                  'student', 'students', 'applicant', 'applicants', 'guest', 'guests'})

RULE_NAMES = frozenset({rule[0] for rule in NAME_RULES} | {rule[0] for rule in VALUE_RULES})
