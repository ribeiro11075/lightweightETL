//! The `key` strategy: a one-to-one mapping, safe for primary and foreign keys.
//!
//! `builtinMasking.KeyStrategy`, and its shape rules are the reason the mapping is
//! one-to-one overall -- two inputs of different shapes can never collide,
//! and within a shape it is a permutation.
//!
//! Only ASCII text is handled here. Python refuses text carrying letters or
//! digits in other scripts, and decides that with `str.isalnum()`, whose
//! Unicode categories Rust's `char::is_alphanumeric` does not match exactly.
//! Rather than reimplement that and risk refusing a value Python accepts --
//! which is a behaviour change, not just a slow path -- anything non-ASCII
//! comes back as `Unsupported` and Python masks it.

use num_bigint::{BigInt, BigUint, Sign};
use num_traits::{One, Zero};

use crate::error::{MaskError, Masked, MAXIMUM_KEY_LENGTH};
use crate::KeyedHash;

pub const DIGITS: &[u8] = b"0123456789";
pub const LOWERCASE: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
pub const UPPERCASE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
pub const HEX_DIGITS: &[u8] = b"0123456789abcdef";

/// `builtinMasking._ALPHANUMERIC_CLASSES`, in order: a character is masked within the
/// first class it belongs to, which is what keeps a digit a digit and a case a
/// case.
const ALPHANUMERIC_CLASSES: [&[u8]; 3] = [DIGITS, LOWERCASE, UPPERCASE];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Charset {
    Alphanumeric,
    Digits,
    Hex,
}

impl Charset {
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "alphanumeric" => Some(Charset::Alphanumeric),
            "digits" => Some(Charset::Digits),
            "hex" => Some(Charset::Hex),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Charset::Alphanumeric => "alphanumeric",
            Charset::Digits => "digits",
            Charset::Hex => "hex",
        }
    }

    /// The alphabet this character is masked within, or None to keep it.
    /// `KeyStrategy._alphabets`.
    fn alphabetFor(&self, character: u8) -> Option<&'static [u8]> {
        match self {
            Charset::Hex => {
                let lowered = character.to_ascii_lowercase();
                HEX_DIGITS.contains(&lowered).then_some(HEX_DIGITS)
            }
            Charset::Digits => character.is_ascii_digit().then_some(DIGITS),
            Charset::Alphanumeric => ALPHANUMERIC_CLASSES.into_iter().find(|class| class.contains(&character)),
        }
    }
}

pub struct KeyStrategy {
    pub charset: Charset,
}

impl KeyStrategy {
    pub fn new(charset: Charset) -> Self {
        Self { charset }
    }

    /// An integer keeps its sign and digit count.
    pub fn maskInteger(&self, hash: &KeyedHash, value: &BigInt) -> Masked<BigInt> {
        let negative = value.sign() == Sign::Minus;
        let magnitude = value.magnitude().clone();
        let digitCount = decimalDigits(&magnitude);

        if digitCount > MAXIMUM_KEY_LENGTH {
            return Err(MaskError::tooLong("key"));
        }

        // Zero belongs to the non-negative one-digit range only; letting a
        // negative digit map to it would make -0 collide with 0's own image.
        let low = if digitCount > 1 || negative {
            BigUint::from(10u8).pow(digitCount as u32 - 1)
        } else {
            BigUint::zero()
        };
        let size = BigUint::from(10u8).pow(digitCount as u32) - &low;

        let purpose: &[u8] = if negative { b"negative" } else { b"integer" };
        let masked = &low + hash.permute(&size, &(&magnitude - &low), purpose);

        Ok(BigInt::from_biguint(if negative { Sign::Minus } else { Sign::Plus }, masked))
    }

    /// Text keeps its length and every character the charset doesn't cover.
    pub fn maskText(&self, hash: &KeyedHash, text: &str) -> Masked<String> {
        if !text.is_ascii() {
            return Err(MaskError::Unsupported);
        }
        let bytes = text.as_bytes();
        if bytes.len() > MAXIMUM_KEY_LENGTH {
            return Err(MaskError::tooLong("key"));
        }

        let alphabets: Vec<Option<&'static [u8]>> =
            bytes.iter().map(|character| self.charset.alphabetFor(*character)).collect();

        // `hex` masks case-insensitively, so the value is read lower-cased and
        // the original's case decides the result's, below.
        let lowered: Vec<u8> = if self.charset == Charset::Hex {
            bytes.iter().map(|character| character.to_ascii_lowercase()).collect()
        } else {
            bytes.to_vec()
        };

        let mut size = BigUint::one();
        let mut number = BigUint::zero();
        for (character, alphabet) in lowered.iter().zip(&alphabets) {
            if let Some(alphabet) = alphabet {
                size *= alphabet.len();
                number = number * alphabet.len() + alphabet.iter().position(|entry| entry == character).unwrap();
            }
        }

        if size.is_one() {
            return Ok(text.to_owned());
        }

        // The shape separates domains: a kept character stands for itself, a
        // masked one for the size of the alphabet it was masked within, written
        // in decimal exactly as Python's str(len(alphabet)) does.
        let mut purpose = Vec::with_capacity(bytes.len() + 24);
        purpose.extend_from_slice(b"text|");
        purpose.extend_from_slice(self.charset.name().as_bytes());
        purpose.push(b'|');
        for (character, alphabet) in bytes.iter().zip(&alphabets) {
            match alphabet {
                None => purpose.push(*character),
                Some(alphabet) => purpose.extend_from_slice(alphabet.len().to_string().as_bytes()),
            }
        }

        let mut masked = hash.permute(&size, &number, &purpose);

        let mut characters = vec![0u8; bytes.len()];
        for index in (0..bytes.len()).rev() {
            match alphabets[index] {
                None => characters[index] = bytes[index],
                Some(alphabet) => {
                    let divisor = BigUint::from(alphabet.len());
                    let position = (&masked % &divisor).try_into().unwrap_or(0usize);
                    masked /= &divisor;
                    characters[index] = alphabet[position];
                }
            }
        }

        let result = String::from_utf8(characters).expect("ASCII in, ASCII out");

        // An all-upper-case hex value stays upper case; a mixed or lower one
        // doesn't. Python decides this on the original text, not the result.
        if self.charset == Charset::Hex
            && text.bytes().any(|character| (b'A'..=b'F').contains(&character))
            && !text.bytes().any(|character| (b'a'..=b'f').contains(&character))
        {
            return Ok(result.to_ascii_uppercase());
        }

        Ok(result)
    }
}

/// How many decimal digits a magnitude has. `builtinMasking._digitCount`, which stops
/// short of writing out anything past MAXIMUM_KEY_LENGTH digits.
pub fn decimalDigits(magnitude: &BigUint) -> usize {
    if magnitude.is_zero() {
        return 1;
    }

    // log2(10) ~ 3.3219; the estimate is within one and the correction is exact.
    let estimate = ((magnitude.bits() as f64) / 3.321_928_094_887_362_f64).floor() as usize;
    let mut digits = estimate.max(1);

    while &BigUint::from(10u8).pow(digits as u32) <= magnitude {
        digits += 1;
    }
    while digits > 1 && &BigUint::from(10u8).pow(digits as u32 - 1) > magnitude {
        digits -= 1;
    }

    digits
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_digits_counts_what_python_counts() {
        for (value, expected) in [(0u64, 1), (1, 1), (9, 1), (10, 2), (99, 2), (100, 3), (12345, 5), (u64::MAX, 20)] {
            assert_eq!(decimalDigits(&BigUint::from(value)), expected, "digits of {value}");
        }
        assert_eq!(decimalDigits(&BigUint::from(10u8).pow(255)), 256);
        assert_eq!(decimalDigits(&(BigUint::from(10u8).pow(256) - 1u8)), 256);
        assert_eq!(decimalDigits(&BigUint::from(10u8).pow(256)), 257);
    }

    #[test]
    fn non_ascii_is_handed_back_rather_than_refused() {
        let hash = KeyedHash::new("k", "d");
        let strategy = KeyStrategy::new(Charset::Alphanumeric);

        assert_eq!(strategy.maskText(&hash, "héllo"), Err(MaskError::Unsupported));
    }
}
