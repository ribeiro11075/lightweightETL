//! Understudy's masking constructions, in Rust.
//!
//! The Python implementation in `understudy_data/masking.py` is the reference.
//! Everything here exists to produce exactly what it produces, faster; where
//! the two could differ, Python wins and this is the bug.
//!
//! Nothing in this crate knows about Python. The PyO3 layer lives in
//! `understudy-mask-py`, which holds every conversion and every unsafe boundary.

// The Python package names things in camelCase and this port is read side by
// side with it; matching its names is worth more here than Rust's convention.
#![allow(non_snake_case)]

pub mod cheap;
pub mod error;
pub mod ff1;
pub mod fpe;
pub mod key;
pub mod keyed_hash;

pub use ff1::Ff1;
pub use fpe::FpeStrategy;
pub use error::{MaskError, Masked};
pub use key::{Charset, KeyStrategy};
pub use keyed_hash::KeyedHash;
