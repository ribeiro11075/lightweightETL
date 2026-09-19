# Bauta

**Put a mask on production:** a safe, realistic stand-in for your production data, in whichever database you need it. Bauta masks what it copies, copies only the slice you need with every relationship intact, generates what may not be copied at all, and moves data between databases on a schedule you already run, with nothing to host.

- **Masking:** consistent across tables and runs, one-to-one for keys (NIST FF1 where policy requires it), applied before anything reaches the target, and every column must be covered.
- **Discovery, subsets and synthetic data:** propose a masking policy from a live schema, copy a referentially complete slice of production, create the copy's tables in whichever database it goes to, and fill tables that can't be copied with generated rows.
- **Audit:** report what every job does with data and what a reviewer should question, and seal each run's masking manifest so it can be verified later.
- **Six databases:** Oracle, SQL Server, PostgreSQL, MySQL, MariaDB and SQLite, as source or target in any combination.
- **Streaming:** memory stays flat however large the table, and PostgreSQL and SQL Server targets load in bulk.
- **Fast:** a million rows of six masked columns in under 7 seconds on one core with the optional native masker, and a minute without; more cores for wide tables. Either way the masks are the same.
- **Incremental loads:** extract only what changed since the last successful run.
- **A dependency graph:** jobs run in order, concurrently where they can, each in its own process with an optional timeout.
- **Operable:** webhook alerts; run state, history and manifests each in a file or a table; and passwords from a command for cloud IAM tokens.
- **No infrastructure:** a `pip install`, some YAML, and a command you run from cron.


## Install

Python 3.10 or newer. Choose the drivers you need as extras; each is loaded only when a connection uses it.

```
pip install "bauta[postgresql,oracle]"
```

| Extra | Installs | Needs besides pip |
| --- | --- | --- |
| `mysql`, `mariadb` | mysql-connector-python | nothing |
| `postgresql` | psycopg 3, with its own libpq | nothing |
| `oracle` | oracledb, in thin mode | nothing — no Oracle client |
| `mssql` | pymssql | nothing |
| `sqlite` | Python's own `sqlite3` | nothing |
| `fpe` | cryptography, for the `fpe` masking strategy | nothing; `oracle` already brings it |
| `native` | `bauta-rs`, the native masker (below) | nothing on Linux (x86-64, ARM) or macOS; elsewhere, [Rust](https://rustup.rs) 1.83 or newer |
| `all` | every driver above | nothing |

**The native masker (optional).** `bauta-rs` masks in Rust: seven to nine times the throughput on one core, with identical masks. It can also mask on several cores: `jobs.yaml`'s `maskingThreads` is `1` by default, a number up to the cores available, or `auto` to divide the cores between the jobs running (see [masking threads](docs/masking.md#masking-threads)). `pip install "bauta[postgresql,native]"` installs the version that matches, which is the only one Bauta uses. Without it, everything works, only slower. See [the native masker](docs/masking.md#the-native-masker).


## Quickstart

Start from the configuration in [`example/starter/configuration/`](example/starter/configuration/), from a clone or downloaded from GitHub:

```
mkdir configuration
cp example/starter/configuration/*.yaml configuration/
```

Edit `configuration/database.yaml` and `configuration/jobs.yaml` for your databases, then supply the credentials they reference:

```
export SOURCE_DB_PASSWORD=...  TARGET_DB_PASSWORD=...  MASKING_KEY=...

bauta validate          # check the configuration, offline
bauta run --dry-run     # check connections and tables, moving nothing
bauta run               # run every job once
```

To see it work without any of that, the demos in a clone of this repository use throwaway SQLite databases:

```
git clone https://github.com/ribeiro11075/bauta.git && cd bauta
pip install -e ".[fpe]"

python example/walkthrough/demo.py       # the whole workflow: discover, subset, audit, mask, verify, synthesize
python example/incremental/demo.py       # streaming and incremental loads
python example/masking/demo.py           # masking, discovery and a subset, from Python
python example/native-masking/demo.py    # Python against Rust, and Rust on one core against all of them
```


## The command

```
bauta run              run every job that's due, once
bauta validate         check the configuration, without connecting
bauta jobs             show the job graph and which jobs are due
bauta history          show recent job outcomes

bauta discover         propose a masking policy for tables
bauta subset           generate jobs that copy a referentially complete subset
bauta schema           create target tables from source ones, in the target's dialect
bauta synthesize       fill tables with generated rows, for data that can't be copied
bauta clear            empty the jobs' target tables, children first

bauta audit            report what each job does with data, and what to question
bauta verify-manifest  check a masking manifest is unaltered, and who signed it

bauta --version        print the version, and which masker it would use
```

| Exit code | Meaning |
| --- | --- |
| `0` | Every job completed. |
| `1` | A job failed, or was skipped because a predecessor failed, or the command failed on a database error. |
| `2` | Invalid configuration or usage. |
| `130` | Interrupted by a signal: running jobs finished, the rest were skipped. |

`run` makes one pass and exits, so it fits under cron or a Kubernetes CronJob. A second `run` sharing the same run state refuses to start while the first is still going. `bauta <command> --help` lists every flag.

### Configuration and logging

Every command takes these.

| Flag | Default | Effect |
| --- | --- | --- |
| `--config DIR` | `$BAUTA_CONFIG`, else `./configuration` | Read `jobs.yaml` and `database.yaml` from this directory. |
| `--log FILE` | none | Also write logs to this file. |
| `--log-format json` | `text` | Write one JSON object per log line, for a collector. |
| `--quiet` | off | Don't log to stderr. |

### Running jobs

| Flag | Default | Effect |
| --- | --- | --- |
| `--job NAME` | every job | Run only this job, ignoring its `refresh` window. Its predecessors don't run; `run` warns about each. Repeatable. |
| `--force` | off | Ignore every job's `refresh` window. |
| `--forever` | off | Keep running cycles instead of exiting after one. For freshness under cron's one-minute floor. |
| `--dry-run` | off | Check connections, target tables, primary keys and masking coverage, moving no rows. |
| `--accept-key-change` | off | Run upsert jobs whose masking key changed since their last run. |
| `--notify-url URL` | `$BAUTA_NOTIFY_URL` | Post a JSON summary to this webhook when a cycle doesn't succeed. |

### Run state, history and the manifest

Each is kept in a file or in a database table, set in [`jobs.yaml`](docs/configuration.md#file-level) or overridden for one run by its flags. A file set in `jobs.yaml` is relative to `jobs.yaml`; a file flag is relative to the working directory.

| What | `jobs.yaml` setting | File | Table | Without either |
| --- | --- | --- | --- | --- |
| **Run state**: last runs, watermarks and key fingerprints | `memory` | `--memory FILE` | `--memory-database ALIAS`, `--memory-table NAME` | `memory.yaml` beside `jobs.yaml` |
| **History**: one record per job per cycle, for `bauta history` | `history` | `--history FILE` | `--history-database ALIAS`, `--history-table NAME` | Not recorded. |
| **Masking manifest**: what was masked and how, sealed, for `bauta verify-manifest` | `manifest` | `--manifest FILE` | `--manifest-database ALIAS`, `--manifest-table NAME` | Not written. |

Tables default to `bauta_memory`, `bauta_history` and `bauta_manifest`, and must exist first; [operations.md](docs/operations.md#tables) has their definitions. A manifest is signed when `$BAUTA_MANIFEST_KEY` is set.

### Proposing and reviewing policies

`discover`, `subset --mask`, `audit` and `synthesize` recognise personal data by built-in rules, and by rules of your own:

| Flag | Default | Effect |
| --- | --- | --- |
| `--rules FILE` | `discovery.yaml` in the configuration directory, if there is one | Check these rules before the built-in ones. See [your own rules](docs/masking.md#your-own-rules-discoveryyaml). |

### Environment variables

The ones you'd set in a deployment; [operations.md](docs/operations.md#environment-variables) also lists two for diagnosis.

| Variable | Effect |
| --- | --- |
| `BAUTA_CONFIG` | The configuration directory, when `--config` isn't given. |
| `BAUTA_NOTIFY_URL` | The webhook, when `--notify-url` isn't given. |
| `BAUTA_MANIFEST_KEY` | Sign manifests, and verify their signatures. |
| `BAUTA_MASKING_THREADS` | Threads the native masker uses per job: a number or `auto`. Overrides `jobs.yaml`'s `maskingThreads`; see [masking threads](docs/masking.md#masking-threads). |


## Documentation

| Document | Covers |
| --- | --- |
| [Configuration](docs/configuration.md) | every field, how credentials are read from the environment, and connection options such as TLS |
| [Masking](docs/masking.md) | strategies, consistent masks across tables, the key, the manifest, `audit`, `discover`, `subset`, `schema`, `synthesize` and `clear` |
| [How it works](docs/design.md) | streaming, incremental loads, retries, scheduling, and the masking design |
| [Operating it](docs/operations.md) | run state, history and notifications |
| [Security model](docs/security.md) | what masking protects and what it doesn't, the constructions, keys, and a deployment checklist |
| [Library](docs/library.md) | embedding it in Python, results, memory backends |
| [Development](docs/development.md) | running the tests, including against real databases |
| [Changelog](CHANGELOG.md) | what changed in each release, breaking changes first |


## Layout

| Path | What it is |
| --- | --- |
| `bauta/` | the package; `runner.py` runs jobs, `masking.py` masks with the strategies in `builtinMasking.py` and their lists in `fakeData.py`, `databaseDialects.py` holds per-database SQL |
| `mask-rs/` | the optional native masker, in Rust — see [its README](mask-rs/README.md) |
| `example/` | runnable demos, each with its `configuration/`, and a starter configuration — see [its README](example/README.md) |
| `docs/` | the documentation above |
| `tests/` | the test suite |


## License

[MIT](LICENSE)
