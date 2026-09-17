"""FF1 format-preserving encryption, as specified in NIST SP 800-38G Rev. 1.

The `fpe` masking strategy is built on this, for policies that have to name a
published algorithm rather than this package's own keyed permutation. Only
encryption is implemented: masking never needs to reverse a value, and not
shipping the inverse keeps the key from becoming a way to unmask one.

AES comes from the `cryptography` package, imported only when an FF1 cipher is
created, so the rest of the package doesn't need it (`pip install
understudy-data[fpe]`).
"""
from __future__ import annotations

from typing import List, Sequence

# NIST SP 800-38G Rev. 1 requires radix ** minlen >= 1,000,000.
MINIMUM_DOMAIN_SIZE = 1_000_000

ROUNDS = 10


def minimumLength(radix: int) -> int:
    """The fewest numerals FF1 may encrypt in this radix."""

    length = 1
    while radix ** length < MINIMUM_DOMAIN_SIZE:
        length += 1

    return length


class FF1:
    """FF1 under one AES key, for numeral strings of one radix."""

    def __init__(self, key: bytes, radix: int) -> None:

        try:
            from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        except ImportError as error:
            raise ImportError('the fpe strategy needs the cryptography package: pip install "understudy-data[fpe]"') from error

        if len(key) not in (16, 24, 32):
            raise ValueError('FF1 needs an AES-128, AES-192 or AES-256 key')
        if not 2 <= radix <= 2 ** 16:
            raise ValueError('FF1 radix must be between 2 and 65536')

        self._encryptor = Cipher(algorithms.AES(key), modes.ECB()).encryptor()
        self.radix = radix
        self.minimumLength = minimumLength(radix)


    def _block(self, data: bytes) -> bytes:

        return self._encryptor.update(data)


    def _prf(self, data: bytes) -> bytes:
        """CBC-MAC with a zero IV: the last block of CBC encryption.

        The chaining XOR runs on integers rather than over zipped bytes: one
        machine-word operation per block instead of sixteen interpreted ones,
        which more than halves this function, and it is called ten times a value.
        """

        state = 0
        for offset in range(0, len(data), 16):
            block = state ^ int.from_bytes(data[offset:offset + 16], 'big')
            state = int.from_bytes(self._block(block.to_bytes(16, 'big')), 'big')

        return state.to_bytes(16, 'big')


    def _number(self, numerals: Sequence[int]) -> int:

        value = 0
        for numeral in numerals:
            value = value * self.radix + numeral

        return value


    def _numerals(self, value: int, length: int) -> List[int]:

        numerals = [0] * length
        for position in range(length - 1, -1, -1):
            value, numerals[position] = divmod(value, self.radix)

        return numerals


    def encrypt(self, numerals: Sequence[int], tweak: bytes = b'') -> List[int]:
        """FF1.Encrypt(K, T, X) -- the algorithm's steps, numbered as in the standard."""

        n = len(numerals)
        if n < self.minimumLength:
            raise ValueError('FF1 needs at least {} numerals in radix {}'.format(self.minimumLength, self.radix))
        if n > 2 ** 32 or any(not 0 <= numeral < self.radix for numeral in numerals):
            raise ValueError('not a numeral string in radix {}'.format(self.radix))

        radix = self.radix
        t = len(tweak)
        u = n // 2                                                               # 1
        v = n - u
        a, b = list(numerals[:u]), list(numerals[u:])                            # 2
        byteCount = ((radix ** v - 1).bit_length() + 7) // 8                     # 3
        d = 4 * ((byteCount + 3) // 4) + 4                                       # 4
        p = (bytes([1, 2, 1]) + radix.to_bytes(3, 'big') + bytes([10, u % 256])  # 5
             + n.to_bytes(4, 'big') + t.to_bytes(4, 'big'))

        for i in range(ROUNDS):                                                  # 6
            q = (tweak + bytes((-t - byteCount - 1) % 16) + bytes([i])           # 6.i
                 + self._number(b).to_bytes(byteCount, 'big'))
            r = self._prf(p + q)                                                 # 6.ii
            s = r                                                                # 6.iii
            j = 1
            block = int.from_bytes(r, 'big')
            while len(s) < d:
                s += self._block((block ^ j).to_bytes(16, 'big'))
                j += 1
            y = int.from_bytes(s[:d], 'big')                                     # 6.iv
            m = u if i % 2 == 0 else v                                           # 6.v
            c = (self._number(a) + y) % (radix ** m)                             # 6.vi
            a, b = b, self._numerals(c, m)                                       # 6.vii-ix

        return a + b                                                             # 7

