"""Removes data values from database error messages before they are reported.

Drivers quote the values a statement choked on: the duplicate key, the text
that wasn't a number, the row that broke a constraint. Those messages reach
logs, run history and webhook notifications, and the values in them can be
anything the statement carried -- masked target values, or, when the failing
statement is a sourceQuery, production values that were never masked.

Each pattern below matches a message format a driver was seen to produce
against a real server, and replaces only the quoted value, so the rest of the
message -- which constraint, which column -- still says what went wrong. A
format not listed here passes through unchanged; see docs/security.md.

Where a server quotes the statement itself (PostgreSQL's `LINE 1:`, MySQL's
`near '...'`), the whole quote goes: drivers write values into the statement
text, and nothing tells a value from the SQL around it.

SQL Server's messages arrive as the repr of pymssql's (code, bytes) tuple, on
one line and with quotes that may be backslash-escaped, hence QUOTE -- and the
lookbehinds that keep a closing quote's backslash out of the value.
"""
from __future__ import annotations

import re
from typing import List, Tuple

REDACTED = '<redacted>'

QUOTE = r"\\?'"

# Where a field of a PostgreSQL message ends: the next `DETAIL:  `-style line,
# the `LINE 1: ` quoting the statement, or the end of the text. A value may
# hold newlines, so a field isn't a line.
POSTGRESQL_FIELD_END = r'(?=\n(?:[A-Z][A-Z ]*:  |LINE \d+: )|\n?\Z)'

# Where a line of an Oracle message ends, for the same reason: at the next
# ORA- line or the link to Oracle's help that closes the message.
ORACLE_LINE_END = r'(?=\nORA-\d{5}: |\nHelp: https://docs\.oracle\.com/|\n?\Z)'

_PATTERNS: List[Tuple['re.Pattern[str]', str]] = [(re.compile(pattern, flags), replacement) for pattern, flags, replacement in (

    # PostgreSQL: DETAIL:  Key (email)=(ann@example.com) already exists.
    # A value may itself contain `) already exists`, so the key ends only where
    # its sentence ends the field -- or, for an exclusion constraint, where the
    # conflicting key starts, which the next pattern handles.
    (r'(Key \((?:(?!\)=\().)*\)=\().*?(\) (?:(?:already exists\.|is duplicated\.|is (?:not present in|still referenced from) table "[^"\n]*"\.)'
     + POSTGRESQL_FIELD_END + r'|conflicts with (?:existing )?key \()|\Z)',
     re.DOTALL, r'\1' + REDACTED + r'\2'),
    # PostgreSQL: ... conflicts with existing key (name)=(ann@example.com).
    (r'(conflicts with (?:existing )?key \((?:(?!\)=\().)*\)=\().*?(\)\.' + POSTGRESQL_FIELD_END + r'|\Z)', re.DOTALL, r'\1' + REDACTED + r'\2'),
    # PostgreSQL: LINE 1: INSERT INTO t VALUES (1, 'ann@example.com', ...  and the
    # caret line under it. psycopg2 writes values into the statement, and the
    # server quotes the statement around the error.
    (r'(\nLINE \d+: ).*?' + POSTGRESQL_FIELD_END, re.DOTALL, r'\1' + REDACTED),
    # PostgreSQL: CONTEXT:  SQL statement "INSERT ... VALUES ('ann@example.com')"
    # and the function it ran in, on the line after.
    (r'(SQL statement ").*?("(?=\n(?:PL/pgSQL function |SQL function ))|"' + POSTGRESQL_FIELD_END + r'|\Z)', re.DOTALL, r'\1' + REDACTED + r'\2'),
    # PostgreSQL: DETAIL:  Failing row contains (4, null, 1).
    (r'(Failing row contains ).*?' + POSTGRESQL_FIELD_END, re.DOTALL, r'\1(' + REDACTED + ').'),
    # PostgreSQL: CONTEXT:  COPY customers, line 3, column email: "..."  (or the whole line, with no column)
    (r'(COPY [^\n,]+, line \d+(?:, column [^\n:]+)?: ).*?' + POSTGRESQL_FIELD_END, re.DOTALL, r'\1' + REDACTED),
    # PostgreSQL: CONTEXT:  JSON data, line 1: ...
    (r'(JSON data, line \d+: ).*?' + POSTGRESQL_FIELD_END, re.DOTALL, r'\1' + REDACTED),
    # PostgreSQL: DETAIL:  Token "..." is invalid.
    (r'(Token )".*?"( is invalid)', 0, r'\1"' + REDACTED + r'"\2'),
    # PostgreSQL: value "12345678901" is out of range for type integer
    (r'(value )".*?"( is out of range)', 0, r'\1"' + REDACTED + r'"\2'),
    # PostgreSQL: invalid byte sequence for encoding "UTF8": 0xe9 0x20
    (r'(byte sequence (?:for encoding "[^"\n]*": )?)0x[0-9a-fA-F]{2}(?: 0x[0-9a-fA-F]{2})*', 0, r'\1' + REDACTED),
    # PostgreSQL: invalid input syntax for type integer: "abc"
    (r'(invalid input (?:syntax|value) for [^\n:]*: ).*?' + POSTGRESQL_FIELD_END, re.DOTALL, r'\1"' + REDACTED + '"'),
    # Anything else that ends a line with a quoted value after a colon, as PostgreSQL's messages do
    (r'(: )"[^\n]*"(?=\n|\Z)', 0, r'\1"' + REDACTED + '"'),

    # MySQL and MariaDB: Duplicate entry 'ann@example.com' for key 'email'
    (r"(Duplicate entry ').*(' for key)", re.DOTALL, r'\1' + REDACTED + r'\2'),
    # MySQL and MariaDB: Incorrect integer value: 'abc' for column 'n' at row 1
    (r"(Incorrect [\w ]+ value: ').*(' for (?:column|function))", re.DOTALL, r'\1' + REDACTED + r'\2'),
    # MySQL and MariaDB: ... the right syntax to use near 'ann@example.com', 3)' at line 1
    (r"(to use near ').*(' at line \d+)", re.DOTALL, r'\1' + REDACTED + r'\2'),
    # MySQL and MariaDB: Truncated incorrect DOUBLE value: 'abc'
    (r"(Truncated incorrect [\w ]+ value: )[^\n]*", 0, r"\1'" + REDACTED + "'"),

    # Oracle 23ai: ORA-03301: (ORA-00001 details) row with column values (EMAIL:'...') already exists
    (r'(\(ORA-\d+ details\) ).*?' + ORACLE_LINE_END, re.DOTALL, r'\1' + REDACTED),
    # Oracle 23ai: ORA-01722: unable to convert string value containing 'S' to a number: N
    (r'(unable to convert string value containing ).*?' + ORACLE_LINE_END, re.DOTALL, r'\1' + REDACTED),

    # SQL Server: The duplicate key value is (ann@example.com).
    (r'(The duplicate key value is \().*(\)\.)', 0, r'\1' + REDACTED + r'\2'),
    # SQL Server: String or binary data would be truncated ... Truncated value: 'abc'.
    (r'(Truncated value: ' + QUOTE + r').*(?<!\\)(' + QUOTE + r'\.)', 0, r'\1' + REDACTED + r'\2'),
    # SQL Server: Conversion failed when converting the nvarchar value 'abc' to data type int.
    (r'(converting the \w+ value ' + QUOTE + r').*(?<!\\)(' + QUOTE + r' to data type)', 0, r'\1' + REDACTED + r'\2'),
    # SQL Server: Incorrect syntax near 'ann'.  pymssql writes values into the
    # statement, so the text near an error can be one.
    (r'(Incorrect syntax near ' + QUOTE + r').*(?<!\\)(' + QUOTE + r'\.)', 0, r'\1' + REDACTED + r'\2'),
    # SQL Server: Unclosed quotation mark after the character string 'ann'.
    (r'(Unclosed quotation mark after the character string ' + QUOTE + r').*(?<!\\)(' + QUOTE + r'\.)', 0, r'\1' + REDACTED + r'\2'),
    # SQL Server: The conversion of the varchar value '99999999999' overflowed an int column.
    (r'(conversion of the \w+ value ' + QUOTE + r').*(?<!\\)(' + QUOTE + r' overflowed)', 0, r'\1' + REDACTED + r'\2'),
    )]


def scrubText(text: str) -> str:
    """`text` with every value a known driver message quotes replaced by <redacted>.

    Idempotent, so text that passes through more than one reporting step is
    unchanged by the second.
    """

    for pattern, replacement in _PATTERNS:
        text = pattern.sub(replacement, text)

    return text


def describeError(error: BaseException) -> str:
    """`TypeName: message`, scrubbed -- how an error is reported anywhere it
    leaves the process.
    """

    return '{}: {}'.format(type(error).__name__, scrubText(str(error)))
