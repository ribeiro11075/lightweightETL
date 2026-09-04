import datetime

from library.databaseDialects import ColumnCategory
from library.databaseScrambleInterface import Scramble


def test_hash_string_does_not_raise_type_error():
    """Regression test: hashString used to pass a str to hashlib.update(), which
    requires bytes in Python 3 and raised TypeError on every call.
    """
    scramble = Scramble(job='t', data=[], columns=[], randomSalt='saltvalue')

    result = scramble.hashString(nonce=0)

    assert isinstance(result, bytes)


def test_hash_string_varies_by_nonce():
    scramble = Scramble(job='t', data=[], columns=[], randomSalt='saltvalue')

    assert scramble.hashString(nonce=0) != scramble.hashString(nonce=1)


def test_scramble_with_random_columns_end_to_end():
    data = [(1, 'alice', datetime.date(2020, 1, 1)), (2, 'bob', datetime.date(2021, 1, 1)), (3, 'carol', datetime.date(2022, 1, 1))]

    scramble = Scramble(job='t', data=data, columns=['id', 'name', 'joined'],
                         columnCategories={'id': ColumnCategory.NUMBER, 'name': ColumnCategory.TEXT, 'joined': ColumnCategory.DATE},
                         identifierColumns=['id'], randomColumns=['name', 'joined'], randomSalt='saltvalue')
    scramble.scramble()

    assert len(scramble.dataScrambled) == len(data)
    assert [row[0] for row in scramble.dataScrambled] == [1, 2, 3]


def test_default_column_values_repeats_the_scalar_per_row():
    """Regression test: (value) * numberRecords used to string-multiply a str
    default instead of repeating it once per row -- (value,) * n is the fix.
    """
    data = [(1, 'x'), (2, 'y'), (3, 'z')]

    scramble = Scramble(job='t', data=data, columns=['id', 'status'],
                         columnCategories={'id': ColumnCategory.NUMBER, 'status': ColumnCategory.TEXT},
                         identifierColumns=['id'], defaultColumnValues={'status': 'N/A'})
    scramble.scramble()

    assert [row[1] for row in scramble.dataScrambled] == ['N/A', 'N/A', 'N/A']


def test_scramble_columns_are_shuffled_not_dropped():
    data = [(1, 'a'), (2, 'b'), (3, 'c')]

    scramble = Scramble(job='t', data=data, columns=['id', 'letter'],
                         columnCategories={'id': ColumnCategory.NUMBER, 'letter': ColumnCategory.TEXT},
                         identifierColumns=['id'], scrambleColumns=['letter'])
    scramble.scramble()

    assert sorted(row[1] for row in scramble.dataScrambled) == ['a', 'b', 'c']


def test_scramble_with_no_data_does_not_raise():
    scramble = Scramble(job='t', data=[], columns=['id'], columnCategories={'id': ColumnCategory.NUMBER}, identifierColumns=['id'])
    scramble.scramble()

    assert scramble.dataScrambled == []


def test_scramble_treats_an_uncategorized_column_as_text_when_randomizing():
    """A column absent from columnCategories entirely (e.g. its database type
    wasn't recognized by that database's dialect) still gets random data when
    randomized -- falls back to the text generator rather than erroring.
    """
    data = [(1, 'a'), (2, 'bb'), (3, 'ccc')]

    scramble = Scramble(job='t', data=data, columns=['id', 'unknownType'], columnCategories={},
                         identifierColumns=['id'], randomColumns=['unknownType'], randomSalt='saltvalue')
    scramble.scramble()

    assert [row[0] for row in scramble.dataScrambled] == [1, 2, 3]
    assert all(isinstance(row[1], str) for row in scramble.dataScrambled)
