# lightweight-etl

Move data between databases — including across different engines — on a schedule you already run, with nothing to host.

- **Six databases:** Oracle, SQL Server, PostgreSQL, MySQL, MariaDB and SQLite, as source or target in any combination.
- **Streaming:** memory stays flat however large the table.
- **Incremental loads:** extract only what changed since the last successful run.
- **A dependency graph:** jobs run in order, concurrently where they can.
- **Table masking:** scramble a copy of production for use elsewhere.
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
export SOURCE_DB_PASSWORD=...  TARGET_DB_PASSWORD=...

lightweight-etl validate          # check the configuration, offline
lightweight-etl run --dry-run     # check connections and tables, moving nothing
lightweight-etl run               # run every job once
```

To see streaming and incremental loads work without any of that:

```
python example/incremental_demo.py
```


## The command

```
lightweight-etl run         run data jobs once
lightweight-etl scramble    run masking jobs — rewrites tables in place
lightweight-etl validate    check configuration without connecting
lightweight-etl jobs        show the job graph and what's due
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


## Documentation

| | |
| --- | --- |
| [Configuration](docs/configuration.md) | every field, and how credentials are read from the environment |
| [How it works](docs/design.md) | streaming, incremental loads, retries, scheduling, and masking's limits |
| [Library](docs/library.md) | embedding it in Python, results, memory backends |
| [Development](docs/development.md) | running the tests, including against real databases |


## Layout

| | |
| --- | --- |
| `lightweight_etl/` | the package: `cli.py`, `configuration.py`, `database.py` with per-dialect SQL in `databaseDialects.py`, `dependencyGraph.py`, `runner.py`, `transform.py`, `builtinTransforms.py`, `scramble.py`, `memory.py`, `log.py` |
| `example/` | a runnable demo and a complete sample configuration — see [its README](example/README.md) |
| `docs/` | the documentation above |
| `tests/` | the test suite |


## License

[MIT](LICENSE)
