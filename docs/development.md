# Development

```
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[all,dev]" ./mask-rs/py    # leave off ./mask-rs/py without Rust
pytest
mypy
```

`all` builds psycopg2 from source; see [the README](../README.md#install) for what that needs, or how to use `psycopg2-binary` instead.


## The default test run

`pytest` needs no servers. It covers everything that doesn't need a real network database, which is most of the package:

- Unit tests against mocked cursors, verifying the SQL each dialect builds and the control flow around it.
- **Real SQLite**, end to end — `tests/test_integration_sqlite.py` runs real streaming and incremental loads through real worker processes. It isn't marked `integration`, since SQLite ships with Python and needs no service.
- The CLI, driven through `main()` against a real SQLite database, including its exit codes, and `discover` and `subset` output that is then run.
- Masking end to end through real worker processes (`tests/test_masking_end_to_end.py`), and the masking strategies' properties (`tests/test_masking.py`): determinism, consistency within a domain, one-to-one keys, and preserved types.
- The shipped examples: every `configuration/` under `example/` is validated against the real models, and every demo is run end to end, so none can drift from what the code accepts.

`tests/conftest.py` stubs `oracledb` and `psycopg2` only when they aren't installed, so the default run needs no native client libraries.


## The native masker

`mask-rs/` holds `understudy-mask`, the optional Rust extension: a separate distribution, so this package installs anywhere without a Rust toolchain. See [its README](../mask-rs/README.md) for the layout. Rust 1.83 or newer:

```
cd mask-rs
cargo test --release              # 918 recorded vectors, NIST FF1, RFC 4231
cd py && maturin build --release
pip install ../target/wheels/understudy_mask-*.whl
```

`--release` matters: two tests measure SHA-256 and AES throughput to catch a backend that fell back to software, which a debug build is indistinguishable from.

**Python is the reference.** Change masking in Python first, port it, then regenerate the vectors with `python3 mask-rs/generate_vectors.py`. `tests/test_maskVectors.py` fails if Python drifts from the recorded file, so regenerating it is deliberate: it means every masked value has changed. Run the suite both ways, as CI does:

```
pytest                              # with the extension, if installed
UNDERSTUDY_NATIVE=0 pytest          # without
```


## Integration tests

Six files run the same operations against real servers: `tests/test_integration_{mysql,postgresql,oracle,mssql,mariadb}.py`, plus `test_integration_cross_database.py`, which extracts from MySQL, applies a transform, and loads into PostgreSQL in one job. `test_integration_masking.py` runs against all five servers: foreign-key discovery (composite keys included), subset queries, and masked jobs over each driver's own numeric and date types. `test_integration_schema.py` runs `schema` and a copy for every pair of the six databases, 36 in all, plus `clear` under live foreign keys. `test_integration_keys.py` checks primary-key lookups against a same-named table in another schema, upserts beside UNIQUE constraints and into key-only tables, and swaps of schema-qualified tables. `test_integration_scrubbing.py` makes each server fail on duplicates and bad values, and checks that no value reaches an error, an outcome or a log. `test_integration_connections.py` asks each server whether driver options arrived, whether the connection is encrypted, and where `currentSchema` sends unqualified names. `test_integration_postgresql.py` also round-trips every value type through `COPY`, and `test_integration_mssql.py` through SQL Server's multi-row statements. `tests/test_fpe.py` checks FF1 against NIST's published sample vectors.

They cover schema introspection, chunked inserts, both upsert paths, swap, truncate, streaming (including abandoning a stream part-way), the full `runDataJobs` path through real job processes, and `DatabaseMemory`.

They're marked `integration` and excluded from the default run:

```
docker compose up -d mysql postgresql oracle mssql mariadb
pip install -e ".[all,dev]"
pytest -m integration
docker compose down
```

Without PostgreSQL's build toolchain, install `".[mysql,oracle,mssql,fpe,dev]" psycopg2-binary` instead of `".[all,dev]"`.

| Service | Port | Image |
| --- | --- | --- |
| mysql | 3307 | `mysql:8.4` |
| postgresql | 5433 | `postgres:16` |
| oracle | 1522 | [`gvenzl/oracle-free`](https://github.com/gvenzl/oci-oracle-free) — free, no Oracle registry login |
| mssql | 1434 | Microsoft's official image — amd64 only, runs under emulation on Apple Silicon |
| mariadb | 3308 | `mariadb:11` |

Each test creates its own uniquely named table and drops it afterwards, so the suite is safe to re-run against running containers. A missing driver or server skips the affected tests with a reason, rather than failing them.

Run them before trusting a change to anything database-facing; they have found bugs the mocked suite couldn't.


## Notes

The `postgresql` extra requires the source-built `psycopg2`, the upstream recommendation for production. `psycopg2-binary` is fine for running the tests.

mypy targets Python 3.10, the oldest version the package supports.


## Dependency versions

`pyproject.toml` gives ranges, not pins, so the package installs beside other tools that have their own. Two files in `constraints/` pin them:

- **`lowest.txt`** is the bottom of every range. CI installs it on Python 3.10 and runs everything, the integration suite included, so a lower bound that stops working fails there first.
- **`image.txt`** pins every package `understudy-data[all]` installs, to one tested set of newer versions. CI's other integration run installs it.

`tests/test_packaging.py` checks that `lowest.txt` matches the lower bounds and that `image.txt` is within the ranges. To raise a lower bound, change both `pyproject.toml` and `lowest.txt`. To move the pinned set to newer versions, edit the direct pins in `image.txt` and regenerate the rest with the command at its top.


## Continuous integration and releases

`.github/workflows/ci.yml` runs mypy and the default tests on every supported Python, with the newest dependency versions the ranges allow. It runs the integration suite against the `docker-compose.yml` servers twice: with `image.txt`'s versions on Python 3.14, and with the lowest versions on Python 3.10.

`.github/workflows/release.yml` publishes a release when a tag matching the version in `pyproject.toml` is pushed:

```
git tag v0.1.0 && git push origin v0.1.0
```

It builds and checks the sdist and wheel, and publishes them to PyPI. PyPI publishing uses trusted publishing, so there is no token to store. Set it up once, before the first tag:

1. On PyPI, add a pending trusted publisher for the project `understudy-data`: this repository, workflow `release.yml`, environment `pypi`.
2. In the repository's settings, create an environment named `pypi`. Requiring a reviewer there makes each release wait for approval.

To check the distributions locally:

```
python -m build && twine check dist/*
```
