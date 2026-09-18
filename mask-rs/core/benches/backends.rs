//! What the crypto backends actually cost, which is why they were chosen.
//!
//! Both of the obvious choices turned out to be software fallbacks on this
//! machine, and both were silent -- correct output, several times slower:
//!
//!     SHA-256   sha2 crate          0.32 GB/s
//!               sha2, "asm" feature 1.37 GB/s
//!               python hashlib      2.30 GB/s
//!               ring                2.48 GB/s   <- in use
//!
//!     AES-256   aes 0.8             0.06 GB/s
//!               aes 0.9             0.90 GB/s   <- in use, detects ARMv8 at runtime
//!               python cryptography 4.94 GB/s
//!
//! `tests/backend.rs` fails the build if either collapses again. This prints
//! the numbers behind that check.
//!
//!     cargo bench --bench backends

use aes::cipher::{BlockCipherEncrypt, KeyInit};
use std::time::Instant;

fn main() {
    let buffer = vec![0x78u8; 64 * 1024 * 1024];
    ring::digest::digest(&ring::digest::SHA256, &buffer);

    let start = Instant::now();
    std::hint::black_box(ring::digest::digest(&ring::digest::SHA256, &buffer));
    let seconds = start.elapsed().as_secs_f64();
    println!(
        "SHA-256 (ring)  {:>6.2} GB/s   {:.4} us per 64-byte block",
        buffer.len() as f64 / seconds / 1e9,
        seconds / (buffer.len() as f64 / 64.0) * 1e6
    );

    let cipher = aes::Aes256::new(&[0u8; 32].into());
    let mut block = [0u8; 16];
    let rounds = 5_000_000;

    let start = Instant::now();
    for _ in 0..rounds {
        cipher.encrypt_block((&mut block).into());
    }
    let seconds = start.elapsed().as_secs_f64();
    std::hint::black_box(block);
    println!(
        "AES-256 (aes)   {:>6.2} GB/s   {:.5} us per block",
        (rounds * 16) as f64 / seconds / 1e9,
        seconds / rounds as f64 * 1e6
    );
}
