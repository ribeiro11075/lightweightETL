//! Not a criterion harness -- just the two numbers that decide the port.
#![allow(non_snake_case)]
use num_bigint::BigUint;
use std::time::Instant;
use understudy_mask_core::KeyedHash;

fn main() {
    let hash = KeyedHash::new("a-test-key-that-is-long-enough", "bench");

    let n = 200_000;
    let start = Instant::now();
    for i in 0..n {
        std::hint::black_box(hash.digest(b"some-message-here", &(i as u32).to_be_bytes()));
    }
    let per = start.elapsed().as_secs_f64() / n as f64 * 1e6;
    println!("digest (one HMAC)      {:.4} us", per);

    // 26**4 * 10**7: the 11-character alphanumeric shape, odd bit width, the
    // one that costs ~39 HMACs a value in Python.
    let size = BigUint::from(26u32).pow(4) * BigUint::from(10u32).pow(7);
    let n = 20_000;
    let start = Instant::now();
    for i in 0..n {
        std::hint::black_box(hash.permute(&size, &BigUint::from(i as u64), b"text|alphanumeric|"));
    }
    let per = start.elapsed().as_secs_f64() / n as f64 * 1e6;
    println!("permute (key/text(11)) {:.3} us", per);
}
