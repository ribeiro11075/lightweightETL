"""Scrubbing data values out of driver error messages.

The messages here are verbatim from the drivers, captured against the servers
in docker-compose.yml; tests/test_integration_scrubbing.py checks the same
failures against the live servers.
"""
import logging

import pytest

from bauta.dependencyGraph import JobStatus
from bauta.log import LOGGER_NAME, JsonFormatter, ScrubbingFilter, portableRecord
from bauta.runner import _executeWithRetries
from bauta.scrubbing import describeError, scrubText

SECRET = 'SeCrEt7'

# (driver message, what it must become)
KNOWN_MESSAGES = [
    # PostgreSQL
    ('duplicate key value violates unique constraint "sc_email_key"\nDETAIL:  Key (email)=(SeCrEt7@x.com) already exists.\nCONTEXT:  COPY sc, line 1\n',
     'duplicate key value violates unique constraint "sc_email_key"\nDETAIL:  Key (email)=(<redacted>) already exists.\nCONTEXT:  COPY sc, line 1\n'),
    ('insert or update on table "sc" violates foreign key constraint "sc_p_fkey"\nDETAIL:  Key (p)=(SeCrEt7) is not present in table "sp".\n',
     'insert or update on table "sc" violates foreign key constraint "sc_p_fkey"\nDETAIL:  Key (p)=(<redacted>) is not present in table "sp".\n'),
    ('new row for relation "sc" violates check constraint "sc_n_check"\nDETAIL:  Failing row contains (5, cSeCrEt7, 4242, 1, null).\n'
     'CONTEXT:  COPY sc, line 1: "5\tcSeCrEt7\t4242\t1\t\\N"\n',
     'new row for relation "sc" violates check constraint "sc_n_check"\nDETAIL:  Failing row contains (<redacted>).\n'
     'CONTEXT:  COPY sc, line 1: <redacted>\n'),
    ('value too long for type character varying(40)\nCONTEXT:  COPY sc, line 1, column email: "SeCrEt7SeCrEt7SeCrEt7"\n',
     'value too long for type character varying(40)\nCONTEXT:  COPY sc, line 1, column email: <redacted>\n'),
    ('invalid input syntax for type integer: "SeCrEt7"\nCONTEXT:  COPY bauta_upsert_c50af8cb5441, line 1, column n: "SeCrEt7"\n',
     'invalid input syntax for type integer: "<redacted>"\nCONTEXT:  COPY bauta_upsert_c50af8cb5441, line 1, column n: <redacted>\n'),
    ('value "SeCrEt7" is out of range for type integer', 'value "<redacted>" is out of range for type integer'),
    ('invalid byte sequence for encoding "UTF8": 0xe9 0x20', 'invalid byte sequence for encoding "UTF8": <redacted>'),
    ('conflicting key value violates exclusion constraint "lk_name_excl"\nDETAIL:  Key (name)=(SeCrEt7) conflicts with existing key (name)=(SeCrEt7).\n',
     'conflicting key value violates exclusion constraint "lk_name_excl"\nDETAIL:  Key (name)=(<redacted>) conflicts with existing key (name)=(<redacted>).\n'),
    ('column "j" is of type json but expression is of type integer[]\nLINE 1: ...SERT INTO lk VALUES (1, \'SeCrEt7\', ARRAY[1])\n'
     '                                                   ^\nHINT:  You will need to rewrite or cast the expression.\n',
     'column "j" is of type json but expression is of type integer[]\nLINE 1: <redacted>\nHINT:  You will need to rewrite or cast the expression.\n'),
    ('invalid input syntax for type integer: "SeCrEt7"\nLINE 1: INSERT INTO lk (id, name) VALUES (\'SeCrEt7\'...\n                                          ^\n',
     'invalid input syntax for type integer: "<redacted>"\nLINE 1: <redacted>\n'),
    # A statement's own text is redacted too, since it can't be told apart from one carrying values.
    ('relation "customers" does not exist\nLINE 1: SELECT * FROM customers\n                      ^\n',
     'relation "customers" does not exist\nLINE 1: <redacted>\n'),
    ('update or delete on table "sp" violates foreign key constraint "f" on table "sc"\nDETAIL:  Key (id)=(SeCrEt7) is still referenced from table "sc".',
     'update or delete on table "sp" violates foreign key constraint "f" on table "sc"\nDETAIL:  Key (id)=(<redacted>) is still referenced from table "sc".'),
    ('error\nCONTEXT:  SQL statement "INSERT INTO t VALUES (\'SeCrEt7\')"\nPL/pgSQL function f() line 3 at SQL statement',
     'error\nCONTEXT:  SQL statement "<redacted>"\nPL/pgSQL function f() line 3 at SQL statement'),
    # MySQL and MariaDB
    ("1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax "
     "to use near 'rank, name) VALUES (1, 2, 'SeCrEt7')' at line 1",
     "1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax "
     "to use near '<redacted>' at line 1"),
    ("1062 (23000): Duplicate entry 'SeCrEt7@x.com' for key 'sc.email'", "1062 (23000): Duplicate entry '<redacted>' for key 'sc.email'"),
    ("1366 (HY000): Incorrect integer value: 'SeCrEt7' for column 'n' at row 1",
     "1366 (HY000): Incorrect integer value: '<redacted>' for column 'n' at row 1"),
    ("1292 (22007): Incorrect date value: 'SeCrEt7' for column `bauta_test`.`sc`.`d` at row 1",
     "1292 (22007): Incorrect date value: '<redacted>' for column `bauta_test`.`sc`.`d` at row 1"),
    ("1292 (22007): Truncated incorrect DOUBLE value: 'SeCrEt7'", "1292 (22007): Truncated incorrect DOUBLE value: '<redacted>'"),
    # Oracle 23ai
    ("ORA-00001: unique constraint (SYSTEM.SYS_C009324) violated on table SYSTEM.SC columns (EMAIL)\n"
     "ORA-03301: (ORA-00001 details) row with column values (EMAIL:'SeCrEt7@x.com') already exists\nHelp: https://docs.oracle.com/error-help/db/ora-00001/",
     "ORA-00001: unique constraint (SYSTEM.SYS_C009324) violated on table SYSTEM.SC columns (EMAIL)\n"
     "ORA-03301: (ORA-00001 details) <redacted>\nHelp: https://docs.oracle.com/error-help/db/ora-00001/"),
    ("ORA-01722: unable to convert string value containing 'S' to a number: N\nORA-03302: (ORA-01722 details) invalid string value: SeCrEt7\n"
     "Help: https://docs.oracle.com/error-help/db/ora-01722/",
     "ORA-01722: unable to convert string value containing <redacted>\nORA-03302: (ORA-01722 details) <redacted>\n"
     "Help: https://docs.oracle.com/error-help/db/ora-01722/"),
    # SQL Server, through pymssql
    ('(102, b"Incorrect syntax near \'SeCrEt7\'.DB-Lib error message 20018")', '(102, b"Incorrect syntax near \'<redacted>\'.DB-Lib error message 20018")'),
    ("(105, b'Unclosed quotation mark after the character string \\'SeCrEt7)\\'.DB-Lib error message 20018')",
     "(105, b'Unclosed quotation mark after the character string \\'<redacted>\\'.DB-Lib error message 20018')"),
    ('(2627, b"Violation of UNIQUE KEY constraint \'UQ__sc__AB6E6164EC060BB2\'. Cannot insert duplicate key in object \'dbo.sc\'. '
     'The duplicate key value is (SeCrEt7@x.com).DB-Lib error message 20018, severity 14:\\nGeneral SQL Server error: Check messages from the SQL Server\\n")',
     '(2627, b"Violation of UNIQUE KEY constraint \'UQ__sc__AB6E6164EC060BB2\'. Cannot insert duplicate key in object \'dbo.sc\'. '
     'The duplicate key value is (<redacted>).DB-Lib error message 20018, severity 14:\\nGeneral SQL Server error: Check messages from the SQL Server\\n")'),
    ('(2628, b"String or binary data would be truncated in table \'master.dbo.sc\', column \'email\'. Truncated value: \'SeCrEt7SeCrE\'.DB-Lib error message 20018")',
     '(2628, b"String or binary data would be truncated in table \'master.dbo.sc\', column \'email\'. Truncated value: \'<redacted>\'.DB-Lib error message 20018")'),
    ("(245, b'Conversion failed when converting the nvarchar value \\'a\"SeCrEt7\\' to data type int.DB-Lib error message 20018')",
     "(245, b'Conversion failed when converting the nvarchar value \\'<redacted>\\' to data type int.DB-Lib error message 20018')"),
    ("(248, b'The conversion of the varchar value \\'SeCrEt7\\' overflowed an int column.DB-Lib error message 20018')",
     "(248, b'The conversion of the varchar value \\'<redacted>\\' overflowed an int column.DB-Lib error message 20018')"),
    ]

# Values built to break a pattern: quotes, parentheses, newlines, and the
# text a pattern stops at. The messages are as the drivers wrote them.
AWKWARD_MESSAGES = [
    'duplicate key value violates unique constraint "lk2_pkey"\nDETAIL:  Key (a, b)=(x) already exists, y, SeCrEt7) already exists.\n',
    'duplicate key value\nDETAIL:  Key (a, b)=(x) is not present in table "t"., SeCrEt7) is not present in table "t".\n',
    'duplicate key value violates unique constraint "sq_pkey"\nDETAIL:  Key (v)=(p(q)r\nline2 )" already exists. SeCrEt7) already exists.\nCONTEXT:  COPY sq, line 1\n',
    'invalid input syntax for type integer: "p(q)r\nline2 )" already exists. SeCrEt7"\nCONTEXT:  COPY sq, line 1, column n: "p(q)r\nline2 )" already exists. SeCrEt7"\n',
    "1062 (23000): Duplicate entry 'x' for key 'y SeCrEt7' for key 'sq.PRIMARY'",
    "1366 (HY000): Incorrect integer value: 'p(q)r\nline2 )\" already exists. SeCrEt7' for column 'n' at row 1",
    "ORA-00001: unique constraint (SYSTEM.SYS_C009327) violated on table SYSTEM.SQ columns (V)\n"
    "ORA-03301: (ORA-00001 details) row with column values (V:'multi\nHelp: x\nSeCrEt7') already exists\nHelp: https://docs.oracle.com/error-help/db/ora-00001/",
    "ORA-01722: unable to convert string value containing 'm' to a number: \nORA-03302: (ORA-01722 details) invalid string value: multi\nHelp: x\nSeCrEt7\n"
    "Help: https://docs.oracle.com/error-help/db/ora-01722/",
    "(2627, b'Violation of PRIMARY KEY constraint \\'PK__sq__3BD0199B9E807AA0\\'. Cannot insert duplicate key in object \\'dbo.sq\\'. "
    "The duplicate key value is (a\"b\\'c).x@y SeCrEt7).DB-Lib error message 20018, severity 14:\\nGeneral SQL Server error\\n')",
    '(245, b"Conversion failed when converting the nvarchar value \'x\' for key \'y SeCrEt7\' to data type int.DB-Lib error message 20018")',
    ]

# Messages that quote nothing from the data, and must come through whole.
VALUE_FREE_MESSAGES = [
    'relation "customers" does not exist',
    "1048 (23000): Column 'email' cannot be null",
    'ORA-12899: value too large for column "SYSTEM"."SC"."EMAIL" (actual: 70, maximum: 40)\nHelp: https://docs.oracle.com/error-help/db/ora-12899/',
    "(547, b'The INSERT statement conflicted with the FOREIGN KEY constraint \"FK__sc__p__03FB8544\". The conflict occurred in database \"master\"')",
    'UNIQUE constraint failed: customers.email',
    ]


@pytest.mark.parametrize('message,expected', KNOWN_MESSAGES)
def test_known_driver_messages_keep_everything_but_the_value(message, expected):
    assert scrubText(message) == expected


@pytest.mark.parametrize('message', AWKWARD_MESSAGES)
def test_awkward_values_are_removed_whole(message):
    assert SECRET not in scrubText(message)


@pytest.mark.parametrize('message', [message for message, _ in KNOWN_MESSAGES] + AWKWARD_MESSAGES)
def test_scrubbing_twice_changes_nothing(message):
    assert scrubText(scrubText(message)) == scrubText(message)


@pytest.mark.parametrize('message', VALUE_FREE_MESSAGES)
def test_messages_without_values_are_unchanged(message):
    assert scrubText(message) == message


def test_describe_error_names_the_type_and_scrubs_the_message():
    error = RuntimeError("1062 (23000): Duplicate entry 'SeCrEt7' for key 'PRIMARY'")

    assert describeError(error) == "RuntimeError: 1062 (23000): Duplicate entry '<redacted>' for key 'PRIMARY'"


def _record(message, args=(), exc_info=None):
    return logging.LogRecord(LOGGER_NAME, logging.ERROR, __file__, 1, message, args, exc_info)


def _raised(message):
    try:
        raise RuntimeError(message)
    except RuntimeError as error:
        return (type(error), error, error.__traceback__)


def test_the_filter_scrubs_the_message_and_its_arguments():
    record = _record('Failed: %s', ("Duplicate entry 'SeCrEt7' for key 'PRIMARY'",))

    assert ScrubbingFilter().filter(record)
    assert record.getMessage() == "Failed: Duplicate entry '<redacted>' for key 'PRIMARY'"


def test_the_filter_scrubs_the_traceback():
    record = _record('Job failed', exc_info=_raised("Duplicate entry 'SeCrEt7' for key 'PRIMARY'"))

    ScrubbingFilter().filter(record)
    formatted = logging.Formatter().format(record)

    assert SECRET not in formatted
    assert 'Traceback' in formatted and "Duplicate entry '<redacted>'" in formatted
    assert SECRET not in JsonFormatter().format(record)


def test_the_filter_scrubs_a_record_forwarded_from_a_job_process():
    forwarded = portableRecord(_record('Job failed', exc_info=_raised("Duplicate entry 'SeCrEt7' for key 'PRIMARY'")))

    ScrubbingFilter().filter(forwarded)

    assert SECRET not in forwarded.exc_text


def test_the_filter_leaves_a_malformed_call_for_the_handler_to_report():
    record = _record('%s and %s', ('only one',))

    assert ScrubbingFilter().filter(record)
    assert record.args == ('only one',)


def test_the_package_logger_scrubs_what_any_handler_receives():
    logger = logging.getLogger(LOGGER_NAME)
    received = []

    class Collect(logging.Handler):
        def emit(self, record):
            received.append(self.format(record))

    handler = Collect()
    logger.addHandler(handler)
    try:
        logger.error('Failed: %s', "Duplicate entry 'SeCrEt7' for key 'PRIMARY'", exc_info=_raised('Key (id)=(SeCrEt7) already exists.'))
    finally:
        logger.removeHandler(handler)

    assert received and all(SECRET not in text for text in received)


def test_a_failed_job_outcome_carries_the_scrubbed_error():

    class Job:
        retries = 1
        retryDelaySeconds = 0

    def attempt():
        raise RuntimeError('duplicate key value\nDETAIL:  Key (email)=(SeCrEt7@x.com) already exists.\n')

    outcome = _executeWithRetries(Job(), 'maskCustomers', attempt)

    assert outcome.status == JobStatus.FAILED and outcome.attempts == 2
    assert outcome.error == 'RuntimeError: duplicate key value\nDETAIL:  Key (email)=(<redacted>) already exists.\n'
