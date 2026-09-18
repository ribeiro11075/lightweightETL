# bauta-rs

The optional native masker for [Bauta](https://github.com/ribeiro11075/bauta).

Bauta masks data on its way from production to a copy. Masking `key` and
`fpe` columns costs tens of microseconds a value in Python, most of it spent in
the interpreter rather than in cryptography. This computes the same masks in
Rust, seven to nine times faster on a whole job on one core, and can use
several.

It is optional. Bauta works without it, and produces identical output
either way.

## Installing

```
pip install "bauta[native]"
```

The extra installs the `bauta-rs` released with your version of `bauta`, which
is the only one Bauta uses; any other is ignored with a warning. Wheels cover
Linux (x86-64 and ARM) and macOS (Apple silicon and Intel) on every supported
Python. Elsewhere pip compiles it, which needs Rust 1.83 or newer.

## Layout

| Path | What it is |
| --- | --- |
| `core/` | The constructions. No Python dependency, so they are testable without an interpreter. |
| `py/` | The PyO3 layer: conversions in, results out, and every unsafe boundary. |
| `vectors/reference.json` | What the Python implementation produces, recorded. The contract between the two. |
| `generate_vectors.py` | Regenerates that file from the Python implementation. |

## Building

From a clone, with Rust 1.83 or newer.

```
cargo test --release
cd py && maturin build --release
pip install ../target/wheels/bauta_rs-*.whl
```

`--release` matters for the tests: two of them measure SHA-256 and AES
throughput to catch a backend that has silently fallen back to software, and a
debug build is indistinguishable from one. `cargo test` covers `core/`; `py/`
needs a Python interpreter to link, so it is tested from Python, by
`tests/test_nativeMasking.py`.

## Threads, and remembering masks

A chunk's distinct values are masked across a thread pool, whose size the
Python layer sets per process (`setThreads`) from `jobs.yaml`'s
`maskingThreads`; each mask depends on its value alone, so the count changes no
result. `availableCores()` reads a container's CPU quota, which Python's
`os.cpu_count()` doesn't. `key`, `fpe` and the `fake*` strategies also remember
masks across chunks. The extension allocates through mimalloc: the system
allocators serialise masking's many small allocations across threads.

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

`key`, `fpe`, `hash`, `email`, `digits`, and the `fake*` strategies, which
pick from the lists Python passes in when a masker is built -- so the lists
are defined once, in Python, and the vectors record them. Everything else
stays in Python: `redact` needs lookbehind that Rust's regex engine doesn't
offer, `shuffle`, `dateShift`, `number`, `keep`, `null` and `constant` are
already cheap, and custom strategies are Python by definition.
