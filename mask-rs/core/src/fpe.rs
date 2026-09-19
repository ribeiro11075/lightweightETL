//! The `fpe` strategy: NIST FF1, for policies that must name a published
//! algorithm. `builtinMasking.FPEStrategy`.
//!
//! FF1 is only defined for at least a million possible values, so shorter
//! values fall back to `key`'s permutation -- or fail, under `strict`. The two
//! never collide, because neither changes a value's length.

use num_bigint::{BigInt, Sign};

use crate::error::{MaskError, Masked, MAXIMUM_KEY_LENGTH};
use crate::ff1::Ff1;
use crate::key::{decimalDigits, Charset, KeyStrategy};
use crate::KeyedHash;

/// `builtinMasking._FPE_ALPHABETS`. Unlike `key`, which masks within a character's own
/// class, `fpe` has one alphabet per charset -- so under `alphanumeric` a
/// letter may become a digit.
fn alphabetFor(charset: Charset) -> &'static [u8] {
    match charset {
        Charset::Digits => b"0123456789",
        Charset::Hex => b"0123456789abcdef",
        Charset::Alphanumeric => b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
    }
}

pub struct FpeStrategy {
    pub charset: Charset,
    pub strict: bool,
    /// One cipher per radix, built on first use. The radix is fixed by the
    /// charset for text and is always 10 for integers, so there are at most two.
    textCipher: Ff1,
    integerCipher: Ff1,
    /// What masks a value too short for FF1, unless `strict` refuses it.
    short: KeyStrategy,
}

impl FpeStrategy {
    pub fn new(hash: &KeyedHash, charset: Charset, strict: bool) -> Self {
        let key = hash.digest(b"", b"ff1 key");

        Self {
            charset,
            strict,
            textCipher: Ff1::new(&key, alphabetFor(charset).len() as u32),
            integerCipher: Ff1::new(&key, 10),
            short: KeyStrategy::new(charset),
        }
    }

    pub fn maskInteger(&self, hash: &KeyedHash, value: &BigInt) -> Masked<BigInt> {
        let negative = value.sign() == Sign::Minus;
        let magnitude = value.magnitude();

        if decimalDigits(magnitude) > MAXIMUM_KEY_LENGTH {
            return Err(MaskError::tooLong("fpe"));
        }

        let digits: Vec<u32> = magnitude.to_str_radix(10).bytes().map(|byte| u32::from(byte - b'0')).collect();

        if digits.len() < self.integerCipher.minimumLength {
            if self.strict {
                return Err(MaskError::tooShortForFf1(self.integerCipher.minimumLength, "digits"));
            }
            return self.short.maskInteger(hash, value);
        }

        // Cycle-walk past results with a leading zero, which would shorten the
        // number. The input has none, so the walk comes back to such a value.
        let tweak: &[u8] = if negative { b"negative" } else { b"integer" };
        let mut masked = self.integerCipher.encrypt(&digits, tweak);
        while masked[0] == 0 {
            masked = self.integerCipher.encrypt(&masked, tweak);
        }

        let text: String = masked.iter().map(|digit| char::from(b'0' + *digit as u8)).collect();
        let number = text.parse::<num_bigint::BigUint>().expect("decimal digits");

        Ok(BigInt::from_biguint(if negative { Sign::Minus } else { Sign::Plus }, number))
    }

    pub fn maskText(&self, hash: &KeyedHash, text: &str) -> Masked<String> {
        // As in `key`: Python decides the non-ASCII refusal with `str.isalnum`,
        // whose categories Rust does not reproduce exactly, so those values go
        // back rather than risk a different answer.
        if !text.is_ascii() {
            return Err(MaskError::Unsupported);
        }
        let bytes = text.as_bytes();
        if bytes.len() > MAXIMUM_KEY_LENGTH {
            return Err(MaskError::tooLong("fpe"));
        }

        let alphabet = alphabetFor(self.charset);
        let lowered: Vec<u8> = if self.charset == Charset::Hex {
            bytes.iter().map(|byte| byte.to_ascii_lowercase()).collect()
        } else {
            bytes.to_vec()
        };

        let positions: Vec<usize> =
            (0..bytes.len()).filter(|index| alphabet.contains(&lowered[*index])).collect();

        if positions.len() < self.textCipher.minimumLength {
            if self.strict {
                let what = format!("{} characters", self.charset.name());
                return Err(MaskError::tooShortForFf1(self.textCipher.minimumLength, &what));
            }
            return self.short.maskText(hash, text);
        }

        // The tweak separates shapes: every masked position reads as NUL, so
        // two values differing only in their masked characters share a tweak.
        let mut tweak = Vec::with_capacity(bytes.len() + 24);
        tweak.extend_from_slice(b"text|");
        tweak.extend_from_slice(self.charset.name().as_bytes());
        tweak.push(b'|');
        let mut masked = vec![false; bytes.len()];
        for index in &positions {
            masked[*index] = true;
        }
        for (index, byte) in bytes.iter().enumerate() {
            tweak.push(if masked[index] { 0 } else { *byte });
        }

        let numerals: Vec<u32> = positions
            .iter()
            .map(|index| alphabet.iter().position(|entry| *entry == lowered[*index]).unwrap() as u32)
            .collect();
        let encrypted = self.textCipher.encrypt(&numerals, &tweak);

        let mut characters = bytes.to_vec();
        for (index, numeral) in positions.iter().zip(&encrypted) {
            characters[*index] = alphabet[*numeral as usize];
        }
        let result = String::from_utf8(characters).expect("ASCII in, ASCII out");

        if self.charset == Charset::Hex
            && text.bytes().any(|byte| (b'A'..=b'F').contains(&byte))
            && !text.bytes().any(|byte| (b'a'..=b'f').contains(&byte))
        {
            return Ok(result.to_ascii_uppercase());
        }

        Ok(result)
    }
}
