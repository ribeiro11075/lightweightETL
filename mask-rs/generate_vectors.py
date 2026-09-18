"""Generates the vectors the Rust port is checked against.

The Python implementation is the reference: every vector here is what
understudy_data.masking produces today, and a Rust build that disagrees with
any of them is a silent key change for anyone who has already masked data.

Run from the repository root:  python3 mask-rs/generate_vectors.py
"""
from __future__ import annotations

import json
import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from understudy_data.masking import STRATEGIES, KeyedHash

KEY = 'a-test-key-that-is-long-enough'
DOMAIN = 'vectors'

# Domain sizes that exercise the awkward corners of permute(): the degenerate
# sizes, both parities of bit width, the byte edges where a Feistel half stops
# fitting its serialised width, and the sizes real shapes actually produce.
PERMUTE_SIZES = [
    1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 100, 255, 256, 257, 1000,
    10 ** 6, 10 ** 7 - 10 ** 6, 2 ** 16, 2 ** 17, 2 ** 32, 2 ** 33, 2 ** 48, 2 ** 49,
    16 ** 32,                  # a UUID: exactly 2**128, one bit past u128
    26 ** 4 * 10 ** 7,         # an 11-character alphanumeric shape, odd bit width
    62 ** 8, 62 ** 20,         # inside u128
    62 ** 22,                  # the first alphanumeric length past u128
    10 ** 40, 62 ** 60,        # well into bignum
    ]

# Values chosen for the boundaries rather than for volume: the u128 cliff at 21
# to 22 alphanumerics and 32 to 33 hex, MAXIMUM_KEY_LENGTH at 256, and the
# one-character and empty shapes that make `size` degenerate.
TEXTS = [
    '', 'a', '0', 'Z', 'ab', '00', 'user0000001', 'USER0000001',
    '0123456789', '01234567890123456789', '012345678901234567890',
    '0123456789012345678901', 'a' * 21, 'a' * 22, 'A1b2C3d4E5f6G7h8I9j0K',
    'deadbeef', 'deadbeefcafebabe0123456789abcdef', 'deadbeefcafebabe0123456789abcdef0',
    'DEADBEEF', 'DeadBeef', '00000000-0000-4000-a000-000000000000',
    'x' * 255, 'x' * 256, 'user@example.com', '+1 (555) 010-9999', 'héllo',
    ]

INTEGERS = [0, 1, -1, 9, 10, -10, 99, 100, 12345, -12345, 10 ** 6, 10 ** 18, -10 ** 18, 10 ** 38, 10 ** 40]


def hashVectors() -> dict:
    keyedHash = KeyedHash(KEY, DOMAIN)
    messages = [b'', b'\x00', b'a', b'hello', b'x' * 63, b'x' * 64, b'x' * 65, b'\xff' * 200]
    purposes = [b'', b'integer', b'negative', b'text|hex|8']

    return {
        'digest': [
            {'message': message.hex(), 'purpose': purpose.hex(), 'digest': keyedHash.digest(message, purpose).hex()}
            for message in messages for purpose in purposes
            ],
        'expand': [
            {'message': message.hex(), 'length': length, 'expand': keyedHash.expand(message, length).hex()}
            for message in messages[:4] for length in (1, 7, 31, 32, 33, 64, 65, 200)
            ],
        'below': [
            # `upper` is text, not a JSON number: 2**128 has no exact JSON
            # number, and every other integer here is written the same way.
            {'message': message.hex(), 'upper': str(upper), 'below': str(keyedHash.below(message, upper))}
            for message in messages[:4] for upper in (1, 2, 7, 256, 10 ** 6, 2 ** 64, 2 ** 128 + 1)
            ],
        'unit': [
            {'message': message.hex(), 'unit': repr(keyedHash.unit(message))}
            for message in messages
            ],
        'permute': [
            {'size': str(size), 'value': str(value), 'permute': str(keyedHash.permute(size, value))}
            for size in PERMUTE_SIZES for value in {0, 1, size // 2, size - 1, min(size - 1, 12345)}
            ],
        }


def strategyVectors() -> dict:
    """Every strategy the port will cover, over the boundary corpus.

    A refusal is a vector too: the Rust layer has to raise MaskingError with
    the same message, or a job that fails today would quietly succeed.
    """

    combinations = [
        ('key', {}), ('key', {'charset': 'hex'}), ('key', {'charset': 'digits'}),
        ('fpe', {}), ('fpe', {'charset': 'hex'}), ('fpe', {'charset': 'digits'}), ('fpe', {'strict': True}),
        ('hash', {}), ('hash', {'length': 20, 'prefix': 'c_'}),
        ('email', {}), ('email', {'keepDomain': True}),
        ('digits', {}), ('fakeName', {}), ('fakeFirstName', {}), ('fakeCity', {}),
        ]

    values = TEXTS + INTEGERS + [uuid.UUID('00000000-0000-4000-a000-000000000000'), None, True]
    out = {}

    for name, options in combinations:
        strategyClass = STRATEGIES[name]
        strategy = strategyClass(KeyedHash(KEY, DOMAIN), strategyClass.validateOptions(options))
        results = []

        for value in values:
            entry = {'type': type(value).__name__, 'value': None if value is None else str(value)}
            try:
                masked = strategy.maskColumn([value], 0)[0]
                entry['masked'] = None if masked is None else str(masked)
                entry['maskedType'] = type(masked).__name__
            except Exception as error:
                entry['error'] = type(error).__name__
                entry['message'] = str(error)
            results.append(entry)

        out['{} {}'.format(name, json.dumps(options, sort_keys=True))] = results

    return out


def main() -> None:
    here = os.path.dirname(os.path.abspath(__file__))
    vectors = {
        'key': KEY,
        'domain': DOMAIN,
        'keyedHash': hashVectors(),
        'strategies': strategyVectors(),
        }

    path = os.path.join(here, 'vectors', 'reference.json')
    with open(path, 'w') as handle:
        json.dump(vectors, handle, indent=1, sort_keys=True)
        handle.write('\n')

    counts = {name: len(entries) for name, entries in vectors['keyedHash'].items()}
    counts['strategies'] = sum(len(entries) for entries in vectors['strategies'].values())
    print('wrote {} ({:,} bytes)'.format(path, os.path.getsize(path)))
    for name, count in sorted(counts.items()):
        print('  {:<12} {:>6,} vectors'.format(name, count))


if __name__ == '__main__':
    main()
