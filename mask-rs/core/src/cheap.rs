//! `hash`, `email` and `digits` -- the strategies that cost a digest or two.
//!
//! Individually they are cheap. They matter because once `key` drops from 58
//! microseconds to 8, three `hash` columns are most of what a row costs.
//!
//! As elsewhere, anything non-ASCII goes back to Python: `digits` normalises
//! digits across scripts and `email` strips by `str.isspace()`, whose ASCII set
//! is not Rust's -- Python counts 0x1c to 0x1f as whitespace and Rust does not,
//! so `trim()` would mask a value with a trailing file separator differently.

use num_bigint::{BigInt, BigUint, Sign};

use crate::error::{MaskError, Masked};
use crate::KeyedHash;

/// Python's `str.isspace()` over ASCII, which `str.strip()` uses.
fn isPythonSpace(byte: u8) -> bool {
    matches!(byte, b'\t' | b'\n' | 0x0b | 0x0c | b'\r' | 0x1c | 0x1d | 0x1e | 0x1f | b' ')
}

fn pythonStrip(text: &str) -> &str {
    let bytes = text.as_bytes();
    let start = bytes.iter().position(|byte| !isPythonSpace(*byte)).unwrap_or(bytes.len());
    let end = bytes.iter().rposition(|byte| !isPythonSpace(*byte)).map_or(start, |index| index + 1);

    &text[start..end]
}

const HEX: &[u8; 16] = b"0123456789abcdef";

/// `hash`: an opaque hex token, `prefix` then `length` hex characters.
///
/// The hex is written through a table rather than `format!`, which allocates a
/// String per byte. That formatting was most of this function: `hash` costs one
/// HMAC, and a strategy that cheap is dominated by whatever surrounds it.
pub fn maskHash(hash: &KeyedHash, canonical: &[u8], length: usize, prefix: &str) -> String {
    let digest = hash.digest(canonical, b"");

    let mut out = Vec::with_capacity(prefix.len() + length);
    out.extend_from_slice(prefix.as_bytes());
    for byte in digest.iter().take(length.div_ceil(2)) {
        out.push(HEX[usize::from(byte >> 4)]);
        out.push(HEX[usize::from(byte & 0x0f)]);
    }
    out.truncate(prefix.len() + length);

    String::from_utf8(out).expect("hex and an ASCII-checked prefix")
}

/// `email`: still shaped like an address, keyed on the lower-cased whole of it.
pub fn maskEmail(hash: &KeyedHash, value: &str, length: usize, mailDomain: &str, keepDomain: bool) -> Masked<String> {
    if !value.is_ascii() {
        return Err(MaskError::Unsupported);
    }

    let address = pythonStrip(value);
    let lowered = address.to_ascii_lowercase();
    let local = maskHash(hash, lowered.as_bytes(), length, "u");

    if keepDomain {
        if let Some((_, domain)) = address.rsplit_once('@') {
            return Ok(format!("{local}@{domain}"));
        }
    }

    Ok(format!("{local}@{mailDomain}"))
}

/// The keyed replacement for a run of ASCII digits. `DigitsStrategy._maskDigits`.
fn maskDigitRun(hash: &KeyedHash, digits: &[u8], keepLeading: usize, keepTrailing: usize) -> Masked<Vec<u8>> {
    let stream = hash.expand(digits, 2 * digits.len() + 32, b"");
    // Bytes at or above 250 are dropped rather than folded, which would bias
    // the low digits. Python would divide by zero if none survived; that cannot
    // happen for any real length, and handing it back keeps the two identical.
    let generated: Vec<u8> = stream.iter().filter(|byte| **byte < 250).map(|byte| b'0' + byte % 10).collect();
    if generated.is_empty() {
        return Err(MaskError::Unsupported);
    }

    let masked = digits
        .iter()
        .enumerate()
        .map(|(position, digit)| {
            if position < keepLeading || position + keepTrailing >= digits.len() {
                *digit
            } else {
                generated[position % generated.len()]
            }
        })
        .collect();

    Ok(masked)
}

pub fn maskDigitsText(hash: &KeyedHash, value: &str, keepLeading: usize, keepTrailing: usize) -> Masked<String> {
    if !value.is_ascii() {
        return Err(MaskError::Unsupported);
    }

    let bytes = value.as_bytes();
    let digits: Vec<u8> = bytes.iter().copied().filter(u8::is_ascii_digit).collect();
    if digits.is_empty() {
        return Ok(value.to_owned());
    }

    let replacement = maskDigitRun(hash, &digits, keepLeading, keepTrailing)?;
    let mut next = replacement.iter();
    let masked: Vec<u8> = bytes
        .iter()
        .map(|byte| if byte.is_ascii_digit() { *next.next().expect("one per digit") } else { *byte })
        .collect();

    Ok(String::from_utf8(masked).expect("ASCII in, ASCII out"))
}

pub fn maskDigitsInteger(hash: &KeyedHash, value: &BigInt, keepLeading: usize, keepTrailing: usize) -> Masked<BigInt> {
    let negative = value.sign() == Sign::Minus;
    let text = value.magnitude().to_str_radix(10);
    let mut masked = maskDigitRun(hash, text.as_bytes(), keepLeading, keepTrailing)?;

    // A leading zero would shorten the integer; keep its digit count.
    if masked.len() > 1 && masked[0] == b'0' {
        let lead = hash.below(text.as_bytes(), &BigUint::from(9u8), b"lead");
        let digit = u8::try_from(&lead).unwrap_or(0) + 1;
        masked[0] = b'0' + digit;
    }

    let number = String::from_utf8(masked)
        .expect("digits")
        .parse::<BigUint>()
        .expect("decimal digits");

    Ok(BigInt::from_biguint(if negative { Sign::Minus } else { Sign::Plus }, number))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_follows_python_not_rust() {
        // 0x1c is whitespace to Python and not to Rust: trim() would leave it.
        assert_eq!(pythonStrip("  alice@corp.com \x1c"), "alice@corp.com");
        assert_eq!(pythonStrip("\x1f\tx\n"), "x");
        assert_eq!(pythonStrip("   "), "");
        assert_eq!(pythonStrip(""), "");
    }
}
