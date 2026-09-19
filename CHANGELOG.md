# Changelog

What changed in each release of `bauta` and `bauta-rs`, which are always released together at the same version. **Breaking** lists what can stop an existing setup from working after an upgrade; read it before upgrading.

Masks never change between releases unless an entry here says so: the same value, key and domain give the same mask in every version so far.


## 0.1.4 — 2026-09-19

### Added
- `bauta --version` prints the version, and which masker a run would use: `bauta-rs` and its version, or Python and why.

### Changed
- The built-in masking strategies moved from `bauta/masking.py` to `bauta/builtinMasking.py`, and the lists the `fake*` strategies pick from to `bauta/fakeData.py`. Old import paths such as `bauta.masking.STRATEGIES` and `bauta.masking.LOCALES` still work.
- `bauta audit` no longer offers `--memory`, `--memory-database` or `--memory-table`, which it never read.


## 0.1.3 — 2026-09-18

### Added
- **Masking on several cores.** `jobs.yaml`'s `maskingThreads` sets how many threads the native masker spreads each chunk over: `1` by default, a number up to the cores available, or `auto` to divide the cores between the jobs running as each starts. `BAUTA_MASKING_THREADS` overrides it. Results are identical for any count. See [masking threads](docs/masking.md#masking-threads).
- The `fake*` strategies are masked by the native masker too, from the lists Python hands it.
- The native masker remembers masks across chunks for `key`, `fpe` and `fake*`, and allocates through mimalloc.
- The native-masking demo compares Python and Rust on a narrow table, and one core against all of them on a 25-column one.

### Changed
- A 25-column masked table runs at about 73,000 rows a second on ten cores, from about 18,000 in 0.1.2. See [speed](docs/masking.md#speed) for the measurements and their conditions.
- The documentation was made consistent throughout, and every speed figure re-measured.


## 0.1.2 — 2026-09-18

### Breaking
- **`bauta init` is removed.** Copy the starter configuration from `example/starter/configuration/` instead.
- **Prometheus metrics are removed:** `--metrics`, `--metrics-push`, `writeMetricsFile` and `pushMetrics`. Take the flags out of cron entries and scripts. Run history in a table covers alerting on jobs that stop completing; see [run history](docs/operations.md#run-history).
- **PostgreSQL uses psycopg 3** (`psycopg[binary]` 3.2.10 or newer) in place of psycopg2. `bauta[postgresql]` now installs without a compiler. psycopg2 is no longer used, so a leftover install can be removed. Connection `options` are still libpq parameters.

### Added
- `memory`, `history` and `manifest` settings in `jobs.yaml`, each a file relative to it or a table in one of `database.yaml`'s aliases. Flags still override them, and `--history-database`, `--manifest-database` and the `--*-table` flags are new.
- Masking manifests can be kept in a table, and `bauta verify-manifest` reads the latest, or `--run RUN_ID` an earlier one.
- `discovery.yaml`: rules of your own for recognising personal data, used by `discover`, `subset --mask`, `audit` and `synthesize` ahead of the built-in rules, which it can leave out by name. `--rules FILE` names one elsewhere.

### Changed
- The built-in discovery rules moved to `bauta/builtinDiscovery.py`.


## 0.1.1 — 2026-09-18

### Added
- **`bauta-rs`, the native masker, on PyPI.** `pip install "bauta[native]"` installs the version that matches; wheels for Linux (x86-64 and ARM) and macOS (Apple silicon and Intel).
- `bauta` uses `bauta-rs` only at its own version, and masks in Python, with a warning, alongside any other.
- `bauta init`, which wrote a starter configuration. Removed again in 0.1.2.


## 0.1.0 — 2026-09-18

The first release on PyPI, as `bauta`: masking, discovery, subsets, synthetic data, `audit`, sealed manifests, incremental loads and a dependency graph, across Oracle, SQL Server, PostgreSQL, MySQL, MariaDB and SQLite.
