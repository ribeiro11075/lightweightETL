# lightweight-etl

Move data between databases, including across different engines, on a schedule you already run, with nothing to host. It can also mask that data on the way, so a copy of production is safe to use elsewhere.

- **Six databases:** Oracle, SQL Server, PostgreSQL, MySQL, MariaDB and SQLite, as source or target in any combination.
- **Streaming:** memory stays flat however large the table.
- **Incremental loads:** extract only what changed since the last successful run.
- **A dependency graph:** jobs run in order, concurrently where they can.
- **Masking:** consistent across tables and runs, one-to-one for keys, applied before anything reaches the target, and every column must be covered.
- **Discovery and subsets:** propose a masking policy from a live schema, copy a referentially complete slice of production, and create the copy's tables in whichever database it goes to.
- **No infrastructure:** a `pip install`, some YAML, and a command you run from cron.


## Install

Python 3.9+. Install with the drivers you need; each is optional and loaded only when used.

```
pip install -e ".[postgresql,oracle]"
```

Extras: `mysql`, `postgresql`, `oracle`, `mssql`, `mariadb`, `sqlite`, or `all`. Oracle runs in `oracledb`'s thin mode and SQL Server through `pymssql`, so neither needs a separate client install.


## Quickstart

```
mkdir configuration
cp example/configuration/*.yaml configuration/
```

Edit `configuration/database.yaml` and `configuration/jobs.yaml` for your databases, then supply the credentials they reference:

```
export SOURCE_DB_PASSWORD=...  TARGET_DB_PASSWORD=...  MASKING_KEY=...

lightweight-etl validate          # check the configuration, offline
lightweight-etl run --dry-run     # check connections and tables, moving nothing
lightweight-etl run               # run every job once
```

To see it work without any of that, using throwaway SQLite databases:

```
python example/incremental_demo.py    # streaming and incremental loads
python example/masking_demo.py        # masking, discovery and a subset
```


## The command

```
lightweight-etl run         run data jobs once, masking any with a `masking` section
lightweight-etl validate    check configuration without connecting
lightweight-etl jobs        show the job graph and what's due
lightweight-etl discover    propose a masking policy for tables
lightweight-etl subset      generate jobs that copy a referentially complete subset
lightweight-etl schema      create target tables from source ones, in the target's dialect
lightweight-etl clear       empty the target tables of jobs, children first
lightweight-etl scramble    deprecated: in-place scrambling, replaced by masking
```

| Exit code | Meaning |
| --- | --- |
| `0` | every job completed |
| `1` | a job failed, or was skipped because a predecessor failed |
| `2` | invalid configuration or usage |
| `130` | interrupted |

`run` makes one pass and exits, so it fits under cron or a Kubernetes CronJob. The useful flags:

| Flag | |
| --- | --- |
| `--config DIR` | where the YAML lives; default `./configuration` |
| `--job NAME` | run only this job — without its predecessors, which it warns about |
| `--force` | ignore `refresh` windows |
| `--forever` | stay running; for freshness under a minute |
| `--log-format json` | structured logs for a collector |
| `--log FILE` | also log to a file, in addition to stderr (`--quiet` silences stderr) |
| `--manifest FILE` | write a JSON record of what was masked, and how |


## Documentation

| | |
| --- | --- |
| [Configuration](docs/configuration.md) | every field, and how credentials are read from the environment |
| [Masking](docs/masking.md) | strategies, consistent masks across tables, the key, the manifest, `discover`, `subset`, `schema` and `clear` |
| [How it works](docs/design.md) | streaming, incremental loads, retries, scheduling, and the masking design |
| [Library](docs/library.md) | embedding it in Python, results, memory backends |
| [Development](docs/development.md) | running the tests, including against real databases |


## Layout

| | |
| --- | --- |
| `lightweight_etl/` | the package: `cli.py`, `configuration.py`, `database.py` with per-dialect SQL in `databaseDialects.py`, `dependencyGraph.py`, `runner.py`, `transform.py`, `builtinTransforms.py`, `masking.py`, `discovery.py`, `subset.py`, `schema.py`, `memory.py`, `log.py`, and the deprecated `scramble.py` |
| `example/` | runnable demos and a complete sample configuration — see [its README](example/README.md) |
| `docs/` | the documentation above |
| `tests/` | the test suite |


## License

[MIT](LICENSE)
