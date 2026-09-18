# bauta-rs

The optional native masker for [Bauta](https://github.com/ribeiro11075/bauta).

Bauta masks data on its way from production to a copy. Masking `key` and
`fpe` columns costs tens of microseconds a value in Python, most of it spent in
the interpreter rather than in cryptography. This computes the same masks in
Rust, four to five times faster on a whole job.

It is optional. Bauta works without it, and produces identical output
either way.

## Layout

| | |
| --- | --- |
| `core/` | The constructions. No Python dependency, so they are testable without an interpreter. |
| `py/` | The PyO3 layer: conversions in, results out, and every unsafe boundary. |
| `vectors/reference.json` | What the Python implementation produces, recorded. The contract between the two. |
| `generate_vectors.py` | Regenerates that file from the Python implementation. |

## Building

Needs Rust 1.83 or newer.

```
cargo test --release
cd py && maturin build --release
pip install ../target/wheels/bauta_rs-*.whl
```

`--release` matters for the tests: two of them measure SHA-256 and AES
throughput to catch a backend that has silently fallen back to software, and a
debug build is indistinguishable from one.

## The rule

**Python is the reference.** This crate exists to be faster, not to be
different. Where the two disagree, Python is right.

That is not a style preference. Bauta's masks are deterministic and keyed,
so a difference between the two implementations would not surface as a wrong
answer — it would surface as a changed key, months later, as joins between an
old copy and a new one quietly stopping matching. The key fingerprint would not
change, because the key did not.

So:

- Every covered strategy is checked against `vectors/reference.json`, over a
  corpus chosen for boundaries rather than volume: the lengths where a Feistel
  half stops fitting a machine word, domains of exactly 2\*\*128, the maximum
  identifier length, single-character alphabets, mixed-case hex, and every
  refusal with its exact message.
- Anything whose behaviour depends on Python's own Unicode rules is not
  reimplemented. Non-ASCII text, `str.isspace()` when an address is stripped,
  digits normalised across scripts — those values are handed back, and Python
  masks them.
- FF1 is checked against NIST SP 800-38G's sample vectors, and the keyed hash
  against RFC 4231.

## What it covers

`key`, `fpe`, `hash`, `email`, `digits`. Everything else stays in Python:
`redact` needs lookbehind that Rust's regex engine doesn't offer, `shuffle`,
`dateShift`, `number`, `keep`, `null` and `constant` are already cheap, the
`fake*` strategies would need a second copy of the name lists, and custom
strategies are Python by definition.
