import datetime

from library.scrambleInterface import Scramble


def test_hash_string_does_not_raise_type_error():
    """Regression test: hashString used to pass a str to hashlib.update(), which
    requires bytes in Python 3 and raised TypeError on every call.
    """
    scramble = Scramble(job='t', data=[], columns=[], dataTypes=[], randomSalt='saltvalue')

    result = scramble.hashString(nonce=0)

    assert isinstance(result, bytes)


def test_hash_string_varies_by_nonce():
    scramble = Scramble(job='t', data=[], columns=[], dataTypes=[], randomSalt='saltvalue')

    assert scramble.hashString(nonce=0) != scramble.hashString(nonce=1)


def test_scramble_with_random_columns_end_to_end():
    data = [(1, 'alice', datetime.date(2020, 1, 1)), (2, 'bob', datetime.date(2021, 1, 1)), (3, 'carol', datetime.date(2022, 1, 1))]

    scramble = Scramble(job='t', data=data, columns=['id', 'name', 'joined'], dataTypes=['INT', 'VARCHAR', 'DATE'],
                         identifierColumns=['id'], randomColumns=['name', 'joined'], randomSalt='saltvalue')
    scramble.scramble()

    assert len(scramble.dataScrambled) == len(data)
    assert [row[0] for row in scramble.dataScrambled] == [1, 2, 3]


def test_default_column_values_repeats_the_scalar_per_row():
    """Regression test: (value) * numberRecords used to string-multiply a str
    default instead of repeating it once per row -- (value,) * n is the fix.
    """
    data = [(1, 'x'), (2, 'y'), (3, 'z')]

    scramble = Scramble(job='t', data=data, columns=['id', 'status'], dataTypes=['INT', 'VARCHAR'],
                         identifierColumns=['id'], defaultColumnValues={'status': 'N/A'})
    scramble.scramble()

    assert [row[1] for row in scramble.dataScrambled] == ['N/A', 'N/A', 'N/A']


def test_scramble_columns_are_shuffled_not_dropped():
    data = [(1, 'a'), (2, 'b'), (3, 'c')]

    scramble = Scramble(job='t', data=data, columns=['id', 'letter'], dataTypes=['INT', 'VARCHAR'],
                         identifierColumns=['id'], scrambleColumns=['letter'])
    scramble.scramble()

    assert sorted(row[1] for row in scramble.dataScrambled) == ['a', 'b', 'c']


def test_scramble_with_no_data_does_not_raise():
    scramble = Scramble(job='t', data=[], columns=['id'], dataTypes=['INT'], identifierColumns=['id'])
    scramble.scramble()

    assert scramble.dataScrambled == []
