# Security model

What Bauta protects, how, and what it does not. Written for the security or privacy reviewer deciding whether masked copies made with it are fit for a purpose. For how to configure masking, see [masking.md](masking.md).

- [Summary](#summary)
- [What is protected, and from whom](#what-is-protected-and-from-whom)
- [Where unmasked data goes](#where-unmasked-data-goes)
- [The masking constructions](#the-masking-constructions)
- [Two implementations](#two-implementations)
- [What masking does not hide](#what-masking-does-not-hide)
- [Keys](#keys)
- [The manifest](#the-manifest)
- [Credentials and transport](#credentials-and-transport)
- [Trusted inputs](#trusted-inputs)
- [Out of scope](#out-of-scope)
- [Checklist for a deployment](#checklist-for-a-deployment)


## Summary

- Masking is **deterministic and keyed**: the same value, in the same domain, under the same key, always gives the same mask. That is what keeps joins working, and it makes the output **pseudonymized, not anonymized**. Under GDPR and similar laws, a masked copy is still personal data.
- Protection rests on **the masking key staying secret** and being **long and random**. Anyone holding the key can confirm guesses; anyone with a weak key's fingerprint can search for it offline.
- Unmasked values exist only in the memory of the process running a job. They are never written to the target, the stage table, logs, errors, the manifest, history or notifications. Database drivers quote values in their error messages; those values are **removed before an error is reported**, for every message format the tests know (see [driver errors](#where-unmasked-data-goes)).
- The strategies differ in what they hide. `null`, `constant` and `redact` remove information; `key`, `fpe`, `hash` and `email` replace it one-to-one or nearly so; `number`, `dateShift` and `shuffle` deliberately keep some of it.


## What is protected, and from whom

**The asset** is the personal or confidential data in the source databases.

**The adversary** is anyone with access to a masked copy (developers, testers, analysts, a vendor, whoever compromises a non-production system) but not to production and not to the masking key. They may know some real values independently: their own customer record, public information about colleagues, a leaked list.

**The goal** is that such an adversary can't recover real values from the copy, beyond what the chosen strategies deliberately keep, and can't link masked rows to real people any better than those kept values allow.

**Not in the adversary model:** someone with the masking key, access to production, or control of the machine running the jobs. They already have what the masking protects.


## Where unmasked data goes

```
production --(TLS, if configured)--> job process memory --(masked)--> target stage/table
```

- **In memory only.** A job streams rows a chunk at a time (`chunkSize`), transforms them, masks them, and only then writes them. Unmasked rows are never written to the target, not even to its stage table.
- **Logs and errors.** Masking errors name the column and the value's type, never the value. Transform errors do the same and drop the transformer's own message, which often quotes the value. Run history, the manifest and notifications carry job names, counts and error text.
- **Driver errors are scrubbed.** Every server but SQLite quotes data in its error messages: the duplicate key, the text that wasn't a number, the row that broke a constraint, and PostgreSQL's `COPY` context lines. When the failing statement is a `sourceQuery`, those are production values that were never masked. Before an error reaches a log, a traceback, run history, a notification or an `audit` report, each quoted value is replaced with `<redacted>`, keeping the rest of the message (which constraint, which column). The patterns cover the messages PostgreSQL 16, MySQL 8.4, MariaDB 11, Oracle 23ai and SQL Server 2022 were seen to write for constraint, conversion and truncation failures, including values containing quotes and newlines, and the integration suite checks them against those servers. Where a server quotes the statement around an error (PostgreSQL's `LINE 1:`, MySQL's `near '...'`), the whole quote is removed, since drivers write values into statement text. **A message in a format not covered passes through unchanged**, and some drivers can also log through their own loggers, outside the package's.
- **Process boundaries.** Job processes send log records and outcomes to the main process over private pipes; neither carries row data. [Masking threads](masking.md#masking-threads) run inside the job's own process and share its memory, so masking on several cores moves no data anywhere new.
- **Transport.** Connections are encrypted only if configured to be (see [driver options and TLS](configuration.md#driver-options-and-tls)). `run --dry-run` and `audit --connect` report what each server says about its connection, and `audit` warns when a masked job reads over an unencrypted one.


## The masking constructions

Everything keyed starts from the masking key `K` and the column's domain `D` (by default, the column name):

```
subkey(D)       = HMAC-SHA256(K, "domain" || 0x00 || D)
digest(m, p)    = HMAC-SHA256(subkey(D), p || 0x00 || m)          purpose p separates uses
expand(m, n, p) = digest(m, p || "#" || counter), counter = 0, 1, ...   truncated to n bytes
```

`m` is the value's canonical form: integers, whole decimals and whole floats as the same decimal text, dates as ISO 8601, text as UTF-8.

| Strategy | Construction | Output space |
| --- | --- | --- |
| `hash` | first `length` hex characters of `digest(m)` | 48 to 256 bits |
| `email` | `u` + hex of `digest(lowercased address)` | 32 to 160 bits |
| `digits` | each digit replaced from `expand(digits)` | the value's digit count; not one-to-one |
| `key` | a Feistel network, 10 rounds, round function `expand` over the right half, on the smallest even bit width covering the value's shape, cycle-walked into range | a permutation of the value's shape: one-to-one |
| `fpe` | NIST SP 800-38G Rev. 1 **FF1** with AES-256; the AES key is `digest("", "ff1 key")`; the tweak separates integers by sign, and text by charset and shape; checked against NIST's nine sample vectors | a permutation of the value's shape: one-to-one |
| `fake*` | a list entry chosen by `below(digest)` | a few dozen to a few thousand values: many-to-one |
| `number` | the value moved by up to `variance`, or placed within `min`–`max`, by `unit(digest)` | keeps magnitude |
| `dateShift` | the date moved by a keyed number of days in ±`maxDays`, never zero | keeps the date to within `maxDays` |
| `shuffle` | the chunk's values permuted by a PRNG seeded from `digest(chunk number)` | keeps every value, on another row |
| `redact` | identifiers found by pattern (and checksum, for cards and IBANs), replaced by labels or by `email`/`digits`/`key` masks | the rest of the text is kept |

**`key` versus `fpe`.** Both are keyed permutations with the same shape rules. `key` uses HMAC-SHA256 as a Feistel round function. That's the same structure as FF1 and FF3-1, but it's this package's own construction, it hasn't been reviewed as a standard, and it adds no dependency. `fpe` is FF1 as NIST specifies it. **Choose `fpe` where a policy must name a published algorithm.**

### Two implementations

The same constructions exist twice: in Python, and in the optional `bauta-rs` extension, which computes them in Rust several times faster (see [the native masker](masking.md#the-native-masker)). Which one ran is recorded in the manifest as `maskedBy`.

**They are required to agree byte for byte.** A difference would not present as a wrong answer. It would present as a changed key: masks that no longer match the ones already in a target, joins that silently stop matching, an incremental job writing rows its earlier rows can't be linked to. The key fingerprint would not change, because the key did not.

How that is held:

- **Recorded vectors.** `mask-rs/vectors/reference.json` holds what the Python implementation produces for every covered strategy over a corpus chosen for boundaries rather than volume: the lengths where a Feistel half stops fitting a machine word, domains of exactly 2\*\*128, `MAXIMUM_KEY_LENGTH`, single-character alphabets, mixed-case hex, and every refusal with its exact message. The Rust tests check against it, and a Python test fails if Python itself drifts from it — so changing masks requires changing the file, deliberately.
- **Both implementations, same corpus.** `tests/test_nativeMasking.py` runs each strategy and option combination through both and compares masks, types and error messages.
- **Any number of threads.** The same tests mask columns large enough to be split across threads, on one thread and on eight, chunk after chunk with the cross-chunk cache warm, and require the same masks as pure Python.
- **The whole suite, twice.** CI runs it with the extension and with `BAUTA_NATIVE=0`.
- **A trace if they ever didn't.** Every masked job records the implementation beside its key fingerprint, and an upsert job run under a different one logs a warning naming both, rather than refusing. The fingerprint alone couldn't show it, since the key hasn't changed — and it is the evidence an operator would need if rows masked before and after stopped joining.
- **Published vectors.** FF1 is checked against NIST's sample vectors on both sides, and the keyed hash against RFC 4231.

The Rust implementation is not a second design. It is a port, and where the two could differ, Python is the reference and the port is the bug. Values whose handling depends on Python's own Unicode rules — the refusal of letters and digits outside ASCII, `str.isspace()` when an address is stripped, digits normalised across scripts — are not reimplemented at all: they are handed back to Python per value.

**Small domains.** Format-preserving encryption over small domains has known message-recovery attacks for anyone who holds enough pairs of real and masked values (Bellare, Hoang and Tessaro, 2016; Durak and Vaudenay, 2017). NIST responded by requiring at least a million possible values for FF1. `fpe` follows that rule, and by default masks shorter values with `key` instead; **`strict: true`** refuses them. `key` applies its permutation to domains of any size, down to single digits, so a two-digit value has only 90 possible masks, and an adversary who knows enough real/masked pairs in one domain learns the whole mapping. This is inherent to one-to-one masking of short values, not a flaw in either cipher: use `hash`, `null`, or a longer key space where short identifiers are sensitive.


## What masking does not hide

These follow from masking being deterministic and shape-preserving. They are why the output is pseudonymized:

- **Equality and frequency.** Equal values get equal masks in a domain, so counts survive. In a low-cardinality column (a status, a department) or a skewed one (surnames), frequency analysis against known distributions can recover values. Every deterministic strategy keeps these frequencies, `fake*` included; use `null` or `constant` where the distribution itself is sensitive.
- **Linkage across tables and runs.** Masks agree across tables in a domain and across runs under one key; that's the purpose. A copy made for one audience can be joined to another copy made under the same key.
- **Shape.** Lengths, formats, sign and digit counts, and characters outside the charset are kept. `key` and `fpe` refuse letters and digits outside ASCII rather than keep them.
- **Magnitude and dates.** `number` with `variance` and `dateShift` reveal approximate values by design: a salary within 10%, a birth date within `maxDays`.
- **Kept columns.** `keep` copies values as they are. Combinations of kept quasi-identifiers (postal code, birth year, gender) can identify people. `audit` flags kept columns whose names suggest personal data, but names can mislead.
- **`shuffle`.** Every real value remains in the table; a small chunk (the tail of a load, a small incremental run) barely moves them, and a one-row chunk not at all.
- **`redact`.** Only identifiers with a recognisable shape are found. Names, addresses written in words and other free-form details pass through.
- **Structure.** Row counts, NULL patterns, relationships and timing are all kept.


## Keys

- **Strength.** Keys must be at least 16 characters; that's a floor, not a recommendation. The key is used as HMAC key material directly, with no password-stretching, so **a guessable key can be found offline** by anyone holding one real/masked pair or a key fingerprint. Use a random key of 32 bytes or more: `python -c "import secrets; print(secrets.token_urlsafe(32))"`.
- **Fingerprints.** Logs, manifests and `audit` show a key fingerprint: the first 48 bits of `HMAC-SHA256(K, fixed text)`. It identifies which key was used without revealing it, provided the key is strong; it is exactly the kind of known pair that makes a weak key searchable.
- **Storage.** Read keys from the environment or a mounted file (`${MASKING_KEY}`, `${file:/run/secrets/masking-key}`); never commit them. Keys are held as secrets in the configuration model, so they don't appear in its `repr`, logs or tracebacks.
- **Rotation.** A new key changes every mask. Masked upsert jobs refuse to run when their key has changed until the change is acknowledged, since their targets would otherwise mix masks from two keys. See [the key](masking.md#the-key).
- **Implementation.** Each masked job also records which implementation masked it; a change is warned about, not refused. See [two implementations](#two-implementations).
- **Separation.** Use different keys for copies that must not be linkable to each other, and a separate key, never a masking key, to sign manifests.


## The manifest

- **Contents.** What was masked, how, under which key fingerprint, from which jobs file (with its SHA-256), with which tool version, and which masking implementation produced it (`maskedBy`: `python`, or the extension and its version). Never a value, never a key.
- **Integrity.** A SHA-256 digest of the manifest's content (canonical JSON) catches accidental changes. Anyone can recompute it, so it proves nothing about origin.
- **Authenticity.** With `BAUTA_MANIFEST_KEY` set, the manifest is also signed with HMAC-SHA256, and `verify-manifest` requires a signature, so removing one doesn't make an edited manifest pass. This is symmetric: anyone who can verify a manifest can also create one. It shows a manifest came from a holder of the signing key, not which holder. Where that distinction matters, keep the signing key with the auditors' process, not with the team running the jobs.


## Credentials and transport

- **Passwords** are held as secrets and never logged. `passwordCommand` output is never logged, and the command is never run by `validate`. Driver `options` are kept out of the configuration's `repr`, since some (a wallet password) are secrets.
- **Short-lived credentials.** `passwordCommand` runs at every connection, so cloud IAM tokens are always fresh; its failures are retried like connection errors.
- **TLS** is configured per driver through `options`. Settings express intent; the server's own report, shown by `run --dry-run` and `audit --connect`, is what to rely on. SQL Server encryption is configured through FreeTDS; pymssql's own `encryption` argument had no effect in testing.
- **Run state, history and manifests** (their files, or their tables) hold job names, times, watermarks and key fingerprints; manifests hold no values. Watermarks are values from the source: an `updated_at` timestamp, usually, but a watermark column could be anything. Protect them like the configuration.


## Trusted inputs

The configuration is trusted: whoever can change it controls what the tool does.

- **SQL.** `sourceQuery`, subset `--where` filters, adhoc queries, table and column names, and `currentSchema` are written into SQL as given (`currentSchema` must be a plain identifier). Data values are always bound as parameters.
- **Code.** Transformer references and custom masking strategies import and run Python modules named in the configuration. Transformer arguments are limited to literals.
- **Commands.** `passwordCommand` runs a program named in the configuration, without a shell.
- **Files.** `${file:...}` reads any file the process can read.
- **Discovery rules.** `discovery.yaml`'s regular expressions run against values sampled from production, in memory; like any regular expression, a badly written one can be slow.
- **YAML** is parsed with PyYAML's safe loader, and run memory is written with its safe dumper, so neither can construct Python objects.

Keep the configuration directory writable only by the people who may run jobs against production.


## Out of scope

- **Anonymization guarantees.** No k-anonymity, l-diversity or differential-privacy checks are made, and none of the strategies provides them.
- **Re-identification risk assessment** of a particular dataset.
- **Access control** on the copies, and their retention and deletion.
- **Protection from someone who holds the key**, production access, or the machine running the jobs.
- **Side channels** such as timing, and the security of the database servers and drivers themselves.
- **Hard deletes** in production reaching incremental copies (see [deletes](design.md#deletes)): a deleted person stays in the copy until it's refreshed in full.


## Checklist for a deployment

1. A random masking key of 32+ bytes, from a secret store, different per audience whose copies must not be linkable.
2. `fpe` with `strict: true` wherever policy requires a published algorithm.
3. No `keep` on quasi-identifiers without a documented reason; `audit --connect --strict` in CI.
4. `null` for free text that may hold names; `redact` only where the text is needed and names are acceptable.
5. Encrypted connections, confirmed by `run --dry-run`.
6. Manifests signed with a key held apart from the operators, and checked with `verify-manifest`.
7. Webhook notifications and run history treated as sensitive: driver error text is scrubbed of the values it quotes in known formats, not all formats.
8. The configuration directory, run state and history writable only by the operators.
9. Masked copies handled as personal data: pseudonymized, not anonymized.
