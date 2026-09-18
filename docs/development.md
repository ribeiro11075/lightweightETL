# Development

```
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[all,dev]"
pytest
mypy
```


## The default test run

`pytest` needs no servers. It covers everything that doesn't need a real network database, which is most of the package:

- Unit tests against mocked cursors, verifying the SQL each dialect builds and the control flow around it.
- **Real SQLite**, end to end — `tests/test_integration_sqlite.py` runs real streaming and incremental loads through real worker processes. It isn't marked `integration`, since SQLite ships with Python and needs no service.
- The CLI, driven through `main()` against a real SQLite database, including its exit codes, and `discover` and `subset` output that is then run.
- Masking end to end through real worker processes (`tests/test_masking_end_to_end.py`), and the masking strategies' properties (`tests/test_masking.py`): determinism, consistency within a domain, one-to-one keys, and preserved types.
- The shipped examples: `example/configuration/` is validated against the real models, and `example/incremental_demo.py` is run end to end. Neither can drift from what the code accepts.

`tests/conftest.py` stubs `oracledb` and `psycopg2` only when they aren't installed, so the default run needs no native client libraries.


## The native masker

`mask-rs/` holds `understudy-mask`, the optional Rust extension. It is a
separate distribution so this package keeps its setuptools build and installs
anywhere without a Rust toolchain. Nothing in `understudy_data` imports it
except through a guarded import, so the package is developed and tested without
it.

```
cd mask-rs
cargo test --release              # 918 recorded vectors, NIST FF1, RFC 4231
cd py && maturin build --release
pip install target/wheels/understudy_mask-*.whl
```

Needs Rust 1.83 or newer — PyO3 does, and older toolchains fail to resolve
rather than fail to build.

Two crates. `core` has no Python dependency, so the constructions are testable
without an interpreter and could later back something other than this package;
`py` is the PyO3 layer and holds every conversion.

**The Python implementation is the reference.** Where the two could differ,
Python is right and the port is the bug — so a change to masking is made in
Python first, then ported, then the vectors are regenerated:

```
python3 mask-rs/generate_vectors.py
```

`tests/test_maskVectors.py` fails if Python drifts from the recorded file, which
makes regenerating it a deliberate act: the file changing means every already
masked value has changed too.

Run the Python suite both ways. CI does:

```
pytest                              # with the extension, if installed
UNDERSTUDY_NATIVE=0 pytest          # without
```

`cargo test --release` rather than `cargo test`: two of the tests measure
throughput to catch a SHA-256 or AES backend that has silently fallen back to a
software implementation, and a debug build looks exactly like one. Both
fallbacks have happened — the pure-Rust `sha2` crate runs at a seventh of
`ring`'s speed, and `aes` 0.8 at a fifteenth of 0.9's — so the checks are not
hypothetical.


## Integration tests

Six files run the same operations against real servers: `tests/test_integration_{mysql,postgresql,oracle,mssql,mariadb}.py`, plus `test_integration_cross_database.py`, which extracts from MySQL, applies a transform, and loads into PostgreSQL in one job. `test_integration_masking.py` runs against all five servers: foreign-key discovery (composite keys included), subset queries, and masked jobs over each driver's own numeric and date types. `test_integration_schema.py` runs `schema` and a copy for every pair of the six databases, 36 in all, plus `clear` under live foreign keys. `test_integration_keys.py` checks primary-key lookups against a same-named table in another schema, upserts beside UNIQUE constraints and into key-only tables, and swaps of schema-qualified tables. `test_integration_scrubbing.py` makes each server fail on duplicates and bad values, and checks that no value reaches an error, an outcome or a log. `test_integration_connections.py` asks each server whether driver options arrived, whether the connection is encrypted, and where `currentSchema` sends unqualified names. `test_integration_postgresql.py` also round-trips every value type through `COPY`, and `test_integration_mssql.py` through SQL Server's multi-row statements. `tests/test_fpe.py` checks FF1 against NIST's published sample vectors.

They cover schema introspection, chunked inserts, both upsert paths, swap, truncate, streaming (including abandoning a stream part-way), the full `runDataJobs` path through real job processes, and `DatabaseMemory`.

They're marked `integration` and excluded from the default run:

```
docker compose up -d mysql postgresql oracle mssql mariadb
pip install -e ".[all,dev]"
pip install psycopg2-binary     # only without PostgreSQL's build toolchain (pg_config)
pytest -m integration
docker compose down
```

| Service | Port | Image |
| --- | --- | --- |
| mysql | 3307 | `mysql:8.4` |
| postgresql | 5433 | `postgres:16` |
| oracle | 1522 | [`gvenzl/oracle-free`](https://github.com/gvenzl/oci-oracle-free) — free, no Oracle registry login |
| mssql | 1434 | Microsoft's official image — amd64 only, runs under emulation on Apple Silicon |
| mariadb | 3308 | `mariadb:11` |

Each test creates its own uniquely named table and drops it afterwards, so the suite is safe to re-run against running containers. A missing driver or server skips the affected tests with a reason, rather than failing them.

These tests have found real bugs the mocked suite couldn't — MySQL leaving unread rows on a connection, and an Oracle identifier-case mismatch — so run them before trusting a change to anything database-facing.


## Notes

The `postgresql` extra requires the source-built `psycopg2`, the upstream recommendation for production. `psycopg2-binary` is fine for running the tests.

mypy targets Python 3.10, the oldest version the package supports.


## Dependency versions

`pyproject.toml` gives ranges, not pins, so the package installs beside other tools that have their own. Two files in `constraints/` pin them:

- **`lowest.txt`** is the bottom of every range. CI installs it on Python 3.10 and runs everything, the integration suite included, so a lower bound that stops working fails there first.
- **`image.txt`** is every package the container image installs. The Dockerfile builds with it, and CI's other integration run uses it, so the image ships what was tested.

`tests/test_packaging.py` checks that `lowest.txt` matches the lower bounds and that `image.txt` is within the ranges. To raise a lower bound, change both `pyproject.toml` and `lowest.txt`. To move the image to newer versions, edit the direct pins in `image.txt` and regenerate the rest with the command at its top.


## Continuous integration and releases

`.github/workflows/ci.yml` runs mypy and the default tests on every supported Python, with the newest dependency versions the ranges allow. It runs the integration suite against the `docker-compose.yml` servers twice: with the image's versions on Python 3.14, and with the lowest versions on Python 3.10.

`.github/workflows/release.yml` publishes a release when a tag matching the version in `pyproject.toml` is pushed:

```
git tag v0.1.0 && git push origin v0.1.0
```

It builds and checks the sdist and wheel, publishes them to PyPI, and pushes a multi-architecture image to `ghcr.io/<owner>/understudy-data` tagged with the version. PyPI publishing uses trusted publishing, so there is no token to store. Set it up once, before the first tag:

1. On PyPI, add a pending trusted publisher for the project `understudy-data`: this repository, workflow `release.yml`, environment `pypi`.
2. In the repository's settings, create an environment named `pypi`. Requiring a reviewer there makes each release wait for approval.

To check the distributions and the image locally:

```
python -m build && twine check dist/*
docker build -t understudy-data:local . && docker run --rm understudy-data:local --help
```
