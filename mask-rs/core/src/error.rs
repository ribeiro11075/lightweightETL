//! What a strategy refuses, and why.
//!
//! Every variant carries the message Python raises for the same value, word for
//! word, because `tests/test_masking.py` asserts on those messages and a job
//! that fails today must fail identically here. The PyO3 layer turns these into
//! `bauta.masking.MaskingError`.
//!
//! `Unsupported` is the exception: it means "this value is one Rust does not
//! handle", not "this value cannot be masked". The Python layer masks those
//! itself rather than reporting anything.

use std::fmt;

/// The longest value `key` and `fpe` mask, in characters or digits.
/// `masking.MAXIMUM_KEY_LENGTH`.
pub const MAXIMUM_KEY_LENGTH: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaskError {
    /// Raise `MaskingError` with this message.
    Refused(String),
    /// Not Rust's to mask: fall back to Python for this one value. Non-ASCII
    /// text, a type the port does not cover, an option combination it does not
    /// implement. Never reported to anyone.
    Unsupported,
}

impl MaskError {
    pub fn tooLong(strategy: &str) -> Self {
        MaskError::Refused(format!(
            "the {strategy} strategy masks identifiers of up to {MAXIMUM_KEY_LENGTH} characters or digits, \
             and this value is longer; use hash, redact or null for long values"
        ))
    }

    pub fn notABool(strategy: &str) -> Self {
        MaskError::Refused(format!("the {strategy} strategy cannot mask a bool"))
    }

    pub fn wrongType(strategy: &str, typeName: &str) -> Self {
        MaskError::Refused(format!("the {strategy} strategy needs an integer or text, got {typeName}"))
    }

    pub fn fractionalDecimal(strategy: &str) -> Self {
        MaskError::Refused(format!("the {strategy} strategy needs a whole number, got a fractional Decimal"))
    }

    pub fn tooShortForFf1(minimumLength: usize, what: &str) -> Self {
        MaskError::Refused(format!(
            "the fpe strategy is strict, and FF1 needs at least {minimumLength} {what} in a value; this one has fewer"
        ))
    }
}

impl fmt::Display for MaskError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MaskError::Refused(message) => formatter.write_str(message),
            MaskError::Unsupported => formatter.write_str("not handled by the native masker"),
        }
    }
}

pub type Masked<T> = Result<T, MaskError>;
