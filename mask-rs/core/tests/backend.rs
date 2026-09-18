//! The SHA-256 backend is doing hardware work, not software.
//!
//! This is the check the port's value rests on, and both halves of it have
//! already been wrong once. On the machine this was written on, the pure-Rust
//! `sha2` crate manages 0.32 GB/s where `ring` manages 2.48; `aes` 0.8 manages
//! 0.06 GB/s where 0.9, which detects ARMv8 crypto at runtime, manages 0.91.
//! Both fallbacks are silent -- they show up as a slow job, months later, and
//! the `aes` one cost `fpe` a factor of three before this test existed.

use std::time::Instant;

/// These measure, so they mean nothing unoptimised: `ring` is precompiled
/// assembly and holds up either way, but `aes` is Rust whose speed depends on
/// the opt level, and a debug build looks exactly like a software fallback.
/// CI runs `cargo test --release`, which is where these have teeth.
fn skipUnoptimised(what: &str) -> bool {
    if cfg!(debug_assertions) {
        eprintln!("skipping the {what} backend check: debug build, timings are meaningless");
        return true;
    }

    false
}

/// Deliberately far below `ring`'s measured 2.48 GB/s: this fails on a software
/// fallback (~0.3 GB/s) and on a slow CI box, without failing on a merely
/// unremarkable one.
const FLOOR_GB_PER_SECOND: f64 = 1.0;

#[test]
fn aes_uses_the_hardware_path() {
    use aes::cipher::{BlockCipherEncrypt, KeyInit};

    if skipUnoptimised("AES") {
        return;
    }

    let cipher = aes::Aes256::new(&[0u8; 32].into());
    let mut block = [0u8; 16];
    let rounds = 1_000_000;

    for _ in 0..1000 {
        cipher.encrypt_block((&mut block).into());
    }

    let start = Instant::now();
    for _ in 0..rounds {
        cipher.encrypt_block((&mut block).into());
    }
    let elapsed = start.elapsed().as_secs_f64();
    std::hint::black_box(block);

    let rate = (rounds * 16) as f64 / elapsed / 1e9;

    // Well under the 0.91 GB/s measured, and well over the 0.06 a software
    // fallback gives.
    assert!(
        rate >= 0.3,
        "AES ran at {rate:.2} GB/s, which is the software fallback's range: \
         `fpe` will be several times slower than it should be"
    );
}

#[test]
fn sha256_uses_the_hardware_path() {
    if skipUnoptimised("SHA-256") {
        return;
    }

    let buffer = vec![0x78u8; 16 * 1024 * 1024];

    // Once to warm the caches, then the measured run.
    ring::digest::digest(&ring::digest::SHA256, &buffer);

    let start = Instant::now();
    let computed = ring::digest::digest(&ring::digest::SHA256, &buffer);
    let elapsed = start.elapsed().as_secs_f64();
    std::hint::black_box(computed);

    let rate = buffer.len() as f64 / elapsed / 1e9;

    assert!(
        rate >= FLOOR_GB_PER_SECOND,
        "SHA-256 ran at {rate:.2} GB/s, under the {FLOOR_GB_PER_SECOND} GB/s floor: \
         the build has most likely fallen back to a software implementation"
    );
}
