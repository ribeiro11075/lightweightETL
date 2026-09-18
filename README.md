# Understudy

**Give production an understudy:** a safe, realistic stand-in for your production data, in whichever database you need it. Understudy masks what it copies, copies only the slice you need with every relationship intact, generates what may not be copied at all, and moves data between databases on a schedule you already run, with nothing to host.

- **Masking:** consistent across tables and runs, one-to-one for keys (NIST FF1 where policy requires it), applied before anything reaches the target, and every column must be covered.
- **Discovery, subsets and synthetic data:** propose a masking policy from a live schema, copy a referentially complete slice of production, create the copy's tables in whichever database it goes to, and fill tables that can't be copied with generated rows.
- **Audit:** report what every job does with data and what a reviewer should question, and seal each run's masking manifest so it can be verified later.
- **Six databases:** Oracle, SQL Server, PostgreSQL, MySQL, MariaDB and SQLite, as source or target in any combination.
- **Streaming:** memory stays flat however large the table, and PostgreSQL and SQL Server targets load in bulk.
- **Fast:** ten million rows of six masked columns in under two minutes on one core with the optional native masker, and under eight without. Either way the masks are the same.
- **Incremental loads:** extract only what changed since the last successful run.
- **A dependency graph:** jobs run in order, concurrently where they can, each in its own process with an optional timeout.
- **Operable:** run history, Prometheus metrics, webhook alerts, run state in a file or a table, and passwords from a command for cloud IAM tokens.
- **No infrastructure:** a `pip install`, some YAML, and a command you run from cron.


## Install

Python 3.10 or newer. Choose the drivers you need as extras; each is loaded only when a connection uses it.

```
pip install "understudy-data[postgresql,oracle]"
```

It isn't on PyPI yet. Until the first release, install from a clone:

```
git clone https://github.com/ribeiro11075/understudy.git
cd understudy
pip install -e ".[postgresql,oracle]"
```

| Extra | Driver | Needs besides pip |
| --- | --- | --- |
| `mysql`, `mariadb` | mysql-connector-python | nothing |
| `postgresql` | psycopg2, built from source | a C compiler and PostgreSQL's client library (below) |
| `oracle` | oracledb, in thin mode | nothing — no Oracle client |
| `mssql` | pymssql | nothing |
| `sqlite` | Python's own `sqlite3` | nothing |
| `fpe` | cryptography, for the `fpe` masking strategy | nothing; `oracle` already brings it |
| `all` | every driver above | as for `postgresql` |

**Building psycopg2.** pip compiles it, so it needs a compiler and `pg_config`:

- macOS: `xcode-select --install`, then `brew install libpq` and `export PATH="$(brew --prefix libpq)/bin:$PATH"` (Homebrew doesn't put libpq on the path by itself).
- Debian or Ubuntu: `apt install build-essential libpq-dev`.

To skip the build, leave `postgresql` (and `all`) out and install the prebuilt driver beside the other extras: `pip install "understudy-data[mysql,oracle,mssql]" psycopg2-binary`. psycopg2's maintainers recommend the source build for production.

**The native masker (optional).** `understudy-mask`, in `mask-rs/`, masks in Rust: four to five times the throughput, identical masks, nothing to configure. With [Rust](https://rustup.rs) 1.83 or newer, pip builds it in the same command as the rest:

```
pip install -e ".[all]" ./mask-rs/py
```

Without Rust, leave `./mask-rs/py` off; everything works, only slower. See [the native masker](docs/masking.md#the-native-masker).


## Quickstart

```
mkdir configuration
cp example/starter/configuration/*.yaml configuration/
```

Edit `configuration/database.yaml` and `configuration/jobs.yaml` for your databases, then supply the credentials they reference:

```
export SOURCE_DB_PASSWORD=...  TARGET_DB_PASSWORD=...  MASKING_KEY=...

understudy validate          # check the configuration, offline
understudy run --dry-run     # check connections and tables, moving nothing
understudy run               # run every job once
```

To see it work without any of that, using throwaway SQLite databases:

```
python example/walkthrough/demo.py       # the whole workflow: discover, subset, audit, mask, verify, synthesize
python example/incremental/demo.py       # streaming and incremental loads
python example/masking/demo.py           # masking, discovery and a subset, from Python
python example/native-masking/demo.py    # the same job masked in Python and in Rust, compared
```


## The command

```
understudy run              run data jobs once, masking any with a `masking` section
understudy validate         check configuration without connecting
understudy jobs             show the job graph and what's due
understudy history          show recent job outcomes recorded with --history

understudy discover         propose a masking policy for tables
understudy subset           generate jobs that copy a referentially complete subset
understudy schema           create target tables from source ones, in the target's dialect
understudy synthesize       fill tables with generated rows, for data that can't be copied
understudy clear            empty the target tables of jobs, children first

understudy audit            report what each job does with data, and what to question
understudy verify-manifest  check a manifest is unaltered, and who signed it
```

| Exit code | Meaning |
| --- | --- |
| `0` | every job completed |
| `1` | a job failed, or was skipped because a predecessor failed, or the command failed on a database error |
| `2` | invalid configuration or usage |
| `130` | interrupted by a signal: running jobs finished, the rest were skipped |

`run` makes one pass and exits, so it fits under cron or a Kubernetes CronJob. A second `run` sharing the same run state refuses to start while the first is still going. The useful flags:

| Flag | |
| --- | --- |
| `--config DIR` | where the YAML lives; default `./configuration` |
| `--job NAME` | run only this job — without its predecessors, which it warns about |
| `--force` | ignore `refresh` windows |
| `--forever` | stay running; for freshness under a minute |
| `--log-format json` | structured logs for a collector |
| `--log FILE` | also log to a file, in addition to stderr (`--quiet` silences stderr) |
| `--memory FILE` | where run state (last runs, watermarks) is kept, overriding `jobs.yaml`'s [`memory`](docs/configuration.md#file-level) |
| `--memory-database ALIAS` | keep run state in a database table instead |
| `--history FILE` | append each job's outcome to a JSON-lines history |
| `--metrics FILE` | write Prometheus metrics for the textfile collector (`--metrics-push URL` for a Pushgateway) |
| `--notify-url URL` | post to a webhook when a run doesn't succeed; default `$UNDERSTUDY_NOTIFY_URL` |
| `--accept-key-change` | run upsert jobs whose masking key changed since their last run |
| `--manifest FILE` | write a sealed JSON record of what was masked, and how; signed if `$UNDERSTUDY_MANIFEST_KEY` is set |


## Documentation

| | |
| --- | --- |
| [Configuration](docs/configuration.md) | every field, how credentials are read from the environment, and connection options such as TLS |
| [Masking](docs/masking.md) | strategies, consistent masks across tables, the key, the manifest, `audit`, `discover`, `subset`, `schema`, `synthesize` and `clear` |
| [How it works](docs/design.md) | streaming, incremental loads, retries, scheduling, and the masking design |
| [Operating it](docs/operations.md) | run state, history, metrics and notifications |
| [Security model](docs/security.md) | what masking protects and what it doesn't, the constructions, keys, and a deployment checklist |
| [Library](docs/library.md) | embedding it in Python, results, memory backends |
| [Development](docs/development.md) | running the tests, including against real databases |


## Layout

| | |
| --- | --- |
| `understudy_data/` | the package; `runner.py` runs jobs, `masking.py` masks, `databaseDialects.py` holds per-database SQL |
| `mask-rs/` | the optional native masker, in Rust — see [its README](mask-rs/README.md) |
| `example/` | runnable demos, each with its `configuration/`, and a starter configuration — see [its README](example/README.md) |
| `docs/` | the documentation above |
| `tests/` | the test suite |


## License

[MIT](LICENSE)
