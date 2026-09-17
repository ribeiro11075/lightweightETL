"""FF1 against the sample vectors NIST published for SP 800-38G."""
import pytest

pytest.importorskip('cryptography')

from lightweight_etl.fpe import FF1, minimumLength

KEY_128 = bytes.fromhex('2B7E151628AED2A6ABF7158809CF4F3C')
KEY_192 = bytes.fromhex('2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F')
KEY_256 = bytes.fromhex('2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94')
TWEAK_1 = bytes.fromhex('39383736353433323130')
TWEAK_2 = bytes.fromhex('3737373770717273373737')
ALPHABET = '0123456789abcdefghijklmnopqrstuvwxyz'


@pytest.mark.parametrize('sample,key,radix,tweak,plaintext,ciphertext', [
    (1, KEY_128, 10, b'', '0123456789', '2433477484'),
    (2, KEY_128, 10, TWEAK_1, '0123456789', '6124200773'),
    (3, KEY_128, 36, TWEAK_2, '0123456789abcdefghi', 'a9tv40mll9kdu509eum'),
    (4, KEY_192, 10, b'', '0123456789', '2830668132'),
    (5, KEY_192, 10, TWEAK_1, '0123456789', '2496655549'),
    (6, KEY_192, 36, TWEAK_2, '0123456789abcdefghi', 'xbj3kv35jrawxv32ysr'),
    (7, KEY_256, 10, b'', '0123456789', '6657667009'),
    (8, KEY_256, 10, TWEAK_1, '0123456789', '1001623463'),
    (9, KEY_256, 36, TWEAK_2, '0123456789abcdefghi', 'xs8a0azh2avyalyzuwd'),
    ])
def test_nist_sample(sample, key, radix, tweak, plaintext, ciphertext):
    encrypted = FF1(key, radix).encrypt([ALPHABET.index(character) for character in plaintext], tweak)

    assert ''.join(ALPHABET[numeral] for numeral in encrypted) == ciphertext


def test_the_minimum_domain_is_a_million():
    assert [minimumLength(radix) for radix in (10, 16, 36, 62, 2)] == [6, 5, 4, 4, 20]


def test_strings_below_the_minimum_length_are_refused():
    with pytest.raises(ValueError, match='at least 6 numerals'):
        FF1(KEY_128, 10).encrypt([1, 2, 3, 4, 5])


def test_numerals_outside_the_radix_are_refused():
    with pytest.raises(ValueError, match='not a numeral string'):
        FF1(KEY_128, 10).encrypt([1, 2, 3, 4, 5, 10])


def test_ff1_is_a_permutation_of_its_domain():
    cipher = FF1(KEY_128, 2)
    width = cipher.minimumLength
    outputs = {tuple(cipher.encrypt([int(bit) for bit in format(value, '0{}b'.format(width))])) for value in range(0, 2 ** width, 997)}

    assert len(outputs) == len(range(0, 2 ** width, 997))
