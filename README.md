# lightweight-etl
A lightweight python library to perform ETL (Extract, Transform, Load) and table-level data masking, built around a dependency graph of jobs.

![ETL](https://www.blastanalytics.com/wp-content/uploads/extract-transform-load-icons-800x279.png)


## What this is
`lightweight_etl/` is a standalone, typed package you import directly -- it never reads a file, never knows a file path, and never requires every database driver to be installed. Your side of the contract is just configuration:

- **jobs** -- a validated `DataJobsFile` or `ScrambleJobsFile` (see the config field reference under Installation, below)
- **databases** -- a validated `Dict[str, DatabaseConnectionConfig]`
- **log location** -- a `Path` to write to
- **memory** -- a `MemoryBackend` to persist run history through (data jobs only, so `refresh` windows and incremental watermarks survive a restart). `FileMemory` (a YAML file, safe across concurrent worker processes) ships by default; write your own to persist it elsewhere, e.g. a database

Worker processes, the process pool, and the dependency graph between jobs are all managed internally; you don't write a worker function or touch `multiprocessing` yourself.

`example/` holds two pieces of executable documentation: `incremental_demo.py`, which runs against a throwaway SQLite database with nothing installed or configured, and `configuration/`, a complete worked set of the YAML files the CLI expects. Both are exercised by tests, so neither can drift from what the code actually accepts -- see `example/README.md`.

### Project layout
| Path | What it is |
| --- | --- |
| `lightweight_etl/` | The package. `cli.py` (the `lightweight-etl` command), `Configuration` (validation), `Database` + per-dialect SQL (`databaseDialects.py`), `DependencyGraph` (scheduling), `Transform`/`Scramble` (row-level work, in `transform.py`/`scramble.py`) with stock transformers in `builtinTransforms.py`, `MemoryBackend`/`FileMemory`/`Log` (`memory.py`/`log.py`), and `runner.py` (the two public entry points, `runDataJobs`/`runScrambleJobs`) |
| `example/` | `incremental_demo.py`, a self-contained runnable demo needing no config or server, and `configuration/`, a complete worked YAML config. Both are exercised by tests -- see `example/README.md` |
| `tests/` | pytest suite -- see "Running the tests" below |


## Installation

1. Setup prerequisites
    - [ ] **Required**: Install Python 3.9+, e.g. following [this guide](https://realpython.com/installing-python)
    - [ ] **Required**: Install pip, e.g. following [this guide](https://howchoo.com/g/mze4ntbknjk/install-pip-python)

1. Create a virtual environment and install the package
    - [ ] **Optional**: Create and activate a virtual environment, e.g. `python3 -m venv .venv && source .venv/bin/activate` (Windows: `.venv\Scripts\activate`)
    - [ ] **Required**: From the repository root, install with the database driver(s) you actually need -- `lightweight_etl`'s only hard dependencies are `pyyaml` and `pydantic`; each database driver is an optional extra, imported lazily so installing one doesn't require the others:
        - `pip install -e ".[mysql]"` -- mysql only
        - `pip install -e ".[postgresql]"` -- postgresql only
        - `pip install -e ".[oracle]"` -- oracle only, via [`oracledb`](https://python-oracledb.readthedocs.io/) in its default "thin" mode -- pure Python, no separate Oracle Client install needed
        - `pip install -e ".[mssql]"` -- SQL Server only, via [`pymssql`](https://github.com/pymssql/pymssql) -- bundles FreeTDS, no separate ODBC driver install needed
        - `pip install -e ".[mariadb]"` -- MariaDB, via the same `mysql-connector-python` driver as `mysql` (MariaDB is wire-compatible with MySQL for everything this library does)
        - `pip install -e ".[sqlite]"` -- SQLite; a no-op install, `sqlite3` ships in Python's standard library -- this extra exists only so `[all]` and the pattern above stay uniform
        - `pip install -e ".[all]"` -- every driver
        - append `,dev` to any of the above to also install `pytest`/`mypy`, e.g. `pip install -e ".[all,dev]"`

1. Point the example scripts at your own databases and jobs
    - The repo ships a small sample under `example/configuration/` (fake hosts, fake credentials, generic table/column names -- nothing here is a real deployment) that `lightweight-etl validate` accepts as-is. Replace the values with your own to run against real infrastructure.
    - [ ] **Required**: Edit `example/configuration/database.yaml` -- one entry per database alias:
        - `type` (**Required**): `oracle`, `mysql`, `postgresql`, `mssql`, `mariadb`, or `sqlite`
        - `database` (**Required**): the database name -- for `sqlite`, this is instead a filesystem path (or `:memory:`)
        - `user` / `password` / `host` (**Required for every type except `sqlite`**): connection credentials -- use `${VAR}` to read them from the environment rather than storing them here, see "Credentials, retries and structured logs" below -- `sqlite` is a local file with no server or authentication, so these are omitted entirely for it
        - `port` (**Optional**): defaults to the driver's standard port when omitted
        - `serviceName` / `sid` (oracle only): exactly one of these is **required** for `type: oracle`
    - [ ] **Required**: Edit `example/configuration/jobs.yaml`
        - `workers` (**Required**): number of processes to run jobs concurrently (number)
        - `cycleSleepSeconds` (**Optional**, default `0.5`): seconds to sleep between cycles when `runForever=True` -- once every active job in a cycle has completed or failed and the next cycle is about to start. Unrelated to `DependencyGraph`'s own fixed 1-second poll, which waits for jobs *within* a cycle to finish, not the gap between cycles
        - `jobs` (**Required**): a map of job name -> job definition. Each job supports:
            - `active` (**Required**): whether the job runs at all (boolean)
            - `refresh` (**Optional**): minimum minutes between runs (number). Evaluated against `memory`, so it applies across separate process invocations too -- running every 5 minutes from cron with `refresh: 60` correctly skips 11 runs out of 12. Note how it interacts with `predecessors`: **`refresh` decides whether a job is in a cycle at all, and `predecessors` only orders jobs within a cycle**, so a predecessor sitting inside its own refresh window is not waited for. A job with `refresh: 5` whose predecessor has `refresh: 60` therefore runs alone for 11 out of 12 cycles, and waits for its predecessor on the 12th. That's deliberate -- otherwise `refresh: 5` would silently behave as `refresh: 60` -- and it's what lets an hourly dimension load and a 5-minute fact load coexist. The consequence to weigh is freshness, not correctness: between windows the dependent reads output up to an hour old, which is fine for a durable table and wrong if the predecessor produces something transient the dependent consumes. Give both jobs the same `refresh` when that's the case
            - `predecessors` (**Optional**): jobs that must complete first (list of job names)
            - `sourceDatabase` (**Required**): database alias from `database.yaml`
            - `sourceQuery` (**Required**): query to extract data from the source database
            - `targetColumns` (**Optional**): the target column names `sourceQuery`'s SELECT list corresponds to, positionally, in that order. Left blank, `sourceQuery` is assumed to select every column of `targetTableFinal`, in that table's own column order -- set this explicitly whenever your query's column order (or subset) doesn't match the target table's own. This is a purely positional contract: valid-but-wrong-order column names insert data into the wrong columns *without an error* (both sides are real columns, so there's nothing to reject); a wrong name or count fails loudly instead, at the database
            - `sourceQueryColumnTransforms` (**Optional**): map of column name -> list of transformer references, each in the form `"module.path:function_name"` (e.g. `lightweight_etl.builtinTransforms:currency`). A set of common ones ships in `lightweight_etl/builtinTransforms.py` -- `currency`, `upper`, `lower`, `strip`, `truncate`, `nullIfBlank`, `digitsOnly`, `epochSecondsToDate`, `booleanToYN` -- but nothing there is privileged: the function can live in any importable module of your own, and is resolved at job-run time via `lightweight_etl.resolveTransformer`. Transforms run against `sourceQuery`'s own result columns -- whatever it actually selects, an explicit list or `select *` alike -- *not* `targetColumns`/`targetTableFinal`; a transform is applied to a value as extracted from the source, before it's ever mapped onto a target column name, so what the target calls that column doesn't matter. Naming a column here that `sourceQuery` doesn't actually return raises `lightweight_etl.TransformError` before anything is written, rather than silently never running. A transformer that raises on a particular row's value (e.g. it expects a string and gets an int) also raises `TransformError`, wrapping the original error with the column name and offending value attached
            - `targetDatabase` (**Required**): database alias from `database.yaml`
            - `targetTableStage` (**Optional**): required when `insertStrategy: swap`
            - `targetTableFinal` (**Required**): target table in the target database
            - `insertStrategy` (**Required**): `swap` or `upsert`
                - `swap`: loads into `targetTableStage`, then swaps it with `targetTableFinal`
                - `upsert`: upserts from `targetTableStage` if set, otherwise straight from the extracted data
            - `retries` (**Optional**, default `0`) / `retryDelaySeconds` (**Optional**, default `5.0`): additional attempts for a failing job, with exponential backoff -- see "Retries" below
            - `chunkSize` (**Required**): rows per batch (number). Extracts are streamed, so this is the **memory dial**, not just the insert batch size -- peak memory is roughly `chunkSize` x row width no matter how large the source table is. See "How a data job moves rows" below
            - `watermarkColumn` (**Optional**): makes the job incremental -- see "Incremental loads" below. Names which of `sourceQuery`'s own result columns to take a high-water mark from; the job then extracts only rows past where the last *successful* run got to. Requires a `{{ watermark }}` placeholder in `sourceQuery`, `watermarkInitial`, and `insertStrategy: upsert`
            - `watermarkInitial` (**Required when `watermarkColumn` is set**): the value bound on the very first run, before anything has been stored
            - `preTargetAdhocQueries` / `postTargetAdhocQueries` (**Optional**): queries run on the target database before/after load

1. Or edit `example/configuration/scramble.yaml` for scramble/masking jobs (run with `lightweight-etl scramble`)
    - `workers` (**Required**): number of processes to run jobs concurrently (number)
    - `cycleSleepSeconds` (**Optional**, default `0.5`): see above
    - `jobs` (**Required**): a map of job name -> job definition. Each job supports:
        - `active` / `refresh` / `predecessors` (see above)
        - `database` / `table` (**Required**): where to scramble data in place
        - `defaultColumnValues` (**Optional**): map of column name -> a fixed value to write into every row
        - `identifierColumns` (**Optional**): columns left untouched
        - `scrambleColumns` (**Optional**): columns whose existing values are shuffled across rows
        - `randomColumns` (**Optional**): columns replaced with freshly generated random values (used with `allDataRandom: false`; set `allDataRandom: true` to randomize every column not otherwise handled above)
        - `randomSalt` (**Required**): salt used to seed generated random text
        - `preTargetAdhocQueries` / `postTargetAdhocQueries` (**Optional**): see above

Invalid configuration (missing fields, an unknown `insertStrategy`, a `sourceDatabase` that isn't defined in `database.yaml`, a `predecessors` entry that isn't a real job, ...) raises `lightweight_etl.ConfigurationError` with a description of every problem found, rather than failing partway through a job run.


## Running it
Most deployments should use the `lightweight-etl` command (below). Embedding the library directly is for when you want a job run to be one step inside a larger Python program -- load and validate your configuration however you like, then call one of these:

```python
from lightweight_etl import Configuration, DataJobsFile, FileMemory, runDataJobs

databaseConfiguration = Configuration.validateDatabaseConfiguration(rawDatabaseConfig)
jobsFile = Configuration.validateJobConfiguration(rawJobConfig, DataJobsFile)
Configuration.validateJobGraph(jobsFile.jobs, databaseAliases=set(databaseConfiguration.keys()))

result = runDataJobs(
    jobsFile=jobsFile,
    databaseConfiguration=databaseConfiguration,
    logFile=logPath,
    memory=FileMemory(memoryFile=memoryPath),
    )

if not result.succeeded:
    raise SystemExit(1)
```

Both runners return a `RunResult`: `succeeded`, `completed`/`failed`/`skipped` (lists of `JobOutcome`, each carrying `rowCount`, `watermark`, `error` and `durationSeconds`), and a total `rowCount`. Results are *returned*, never written anywhere -- turning one into an exit code, an alert, or a log line is the caller's call, and it needs no backend and no configuration to be useful.

`runForever=False` (the default for both) makes a single pass over every active job and returns. Pass `True` to keep running, honoring each job's `refresh` window -- see "Single runs, not a daemon" below for why that isn't the default.

For a runnable demonstration of streaming and incremental loads that needs no configuration, credentials or server, run `python example/incremental_demo.py`. It builds a throwaway SQLite database, runs a real job through `runDataJobs` three times, and prints the watermark moving -- see "Incremental loads" below for what it's showing.

`runScrambleJobs(jobsFile, databaseConfiguration, logFile)` is the scramble-job equivalent -- no `memory` argument, since scramble jobs have no `refresh` window or watermark to track.

Two backends ship with the package:

| Backend | When |
| --- | --- |
| `FileMemory(memoryFile=...)` | A YAML file. Fine whenever a filesystem persists between runs and every worker shares it. |
| `DatabaseMemory(connectionSettings=..., table=...)` | A database table. Use it when that assumption doesn't hold -- a container with no volume, anything scaled across machines, or serverless, where `/tmp` is scoped to one execution environment and vanishes on a cold start. `FileMemory` there doesn't fail loudly: it silently forgets every watermark and re-extracts from `watermarkInitial`, the exact failure incremental loads exist to avoid. The table must already exist; `lightweight_etl.DATABASE_MEMORY_SCHEMA` is its shape, with types to adjust per database. |

`memory` accepts any `MemoryBackend`, not just these -- write your own if you want run state somewhere else again. The one constraint: since `runDataJobs` hands the same instance to every worker process, it needs to survive being pickled and reconstructed per process -- hold settings (a `Path`, connection settings, ...) rather than a live file handle or database connection, and open whatever resource you need inside `read()`/`recordRun()` itself. Note that `MemoryBackend` is scheduler *input* -- read before a job runs to decide whether it should, and from where -- not a place to record what happened; that's what `RunResult` is for. See `lightweight_etl/memory.py` for the interface, and `example/example_database_memory.py` for a database-backed reference implementation (built on `Database`, so it works across mysql/postgresql/oracle) -- it's deliberately not shipped in `lightweight_etl/` itself, since the table schema it expects is one opinion among many, not something the library should assume for you.

`MemoryBackend` has four methods, in two pairs. `read()`/`recordRun()` are abstract and track each job's last-run time, for `refresh`. `readWatermarks()`/`recordWatermark()` are **not** abstract -- they default to "no watermark support", so a backend written before incremental loads existed keeps working unchanged for every job that doesn't use one. If you configure a `watermarkColumn` against a backend that hasn't implemented them, `runDataJobs` raises `ConfigurationError` before starting any work, rather than letting a job discover it in a worker process after it has already loaded rows.


## The `lightweight-etl` command
Installing the package puts a `lightweight-etl` command on your PATH. It is the supported entry point -- `example/` is a reference for embedding the library in your own script, not the way to run it.

```
lightweight-etl run        # run data jobs once, then exit
lightweight-etl scramble   # run masking jobs (destructive: rewrites rows in place)
lightweight-etl validate   # check configuration offline, connecting to nothing
lightweight-etl jobs       # show the graph, and which jobs are due right now
```

Configuration is found by convention: `--config DIR` (holding `jobs.yaml`/`scramble.yaml` and `database.yaml`), else `$LIGHTWEIGHT_ETL_CONFIG`, else `./configuration`. `--jobs`/`--databases` override individually.

### Exit codes
For anything that schedules work, the exit code *is* the interface:

| Code | Meaning |
| --- | --- |
| `0` | every active job completed |
| `1` | at least one job failed, or was skipped because a predecessor didn't complete |
| `2` | invalid configuration, or a usage error |
| `130` | interrupted |

A skipped job counts as failure on purpose: it never ran, so the data it was meant to produce isn't there, and reporting success because nothing raised is a lie your scheduler would act on.

### Single runs, not a daemon
`run` executes one pass and exits. That's the default because it composes with whatever already schedules work in your deployment -- cron, a systemd timer, a Kubernetes CronJob, an Airflow task -- rather than competing with it. Those give you alerting, retries, backfill and calendar-aware schedules that `refresh` cannot express.

`refresh` still works across separate invocations, because it's evaluated against the durable memory file: running every 5 minutes from cron with `refresh: 60` correctly skips 11 runs out of 12.

```cron
*/5 * * * *  cd /srv/etl && lightweight-etl run --config ./configuration --memory ./memory.yaml
```

`--forever` keeps the process resident. Use it when you need freshness below cron's one-minute floor, or where there's no scheduler to hook into. It handles `SIGINT`/`SIGTERM` by finishing the current cycle and shutting its worker pool down cleanly -- without that, a container's ordinary shutdown would orphan workers mid-job.

### Useful flags
- `--job NAME` (repeatable) runs only that job, and implies `--force`. It does **not** run predecessors, and warns naming every one it skipped -- the use case is a fast iteration loop, but a silently ignored dependency is how a `--job` in a cron becomes a stale-upstream incident months later.
- `--force` ignores refresh windows.
- `--dry-run` is the online counterpart to `validate`: it connects to every alias, checks each driver is installed, checks target tables are readable, and checks that `insertStrategy: upsert` targets actually have a primary key -- without that key the upsert degrades silently rather than failing loudly. No rows move.
- `--log PATH` additionally writes a log file. Logs go to stderr by default, since in a container they have to reach stdout/stderr to be collected at all.
- `--log-format json` emits structured records carrying `job`/`status`/`rowCount` as fields a collector can filter on.


## Credentials, retries and structured logs

### Keep credentials out of the config file
Any string in either YAML file may reference the environment:

```yaml
prod:
  type: postgresql
  database: app
  host: db.internal
  user: etl
  password: ${PROD_DB_PASSWORD}
  port: ${PROD_DB_PORT:-5432}
```

An unset variable with **no default raises `ConfigurationError` before anything runs**, rather than expanding to an empty string -- a blank password fails later with the driver's own unhelpful message, and a blank host silently connects somewhere unintended. Every unset name in the document is reported at once.

Defaults are for the values that are awkward without them (ports, hosts, schema names). Don't give a secret a default; that just moves the credential back into the file.

A literal `${...}` is escaped as `$${...}`, since `sourceQuery` is arbitrary SQL. PostgreSQL's dollar-quoting (`$$body$$`) is never followed by a brace and passes through untouched.

The CLI expands automatically. A library caller loading their own YAML calls `expandEnvironmentVariables` on the loaded structure before validating it.

### Retries
A data job may set `retries` (default `0`) and `retryDelaySeconds` (default `5.0`); the delay doubles between attempts, so a database that is down isn't hammered at a fixed interval while it recovers.

```yaml
retries: 3
retryDelaySeconds: 5
```

Retrying a whole data job is safe because both insert strategies converge on a re-run: `swap` restages and re-swaps, and `upsert` re-applies rows already present as a no-op.

`ConfigurationError`, `TransformError` and `TransformResolutionError` are **never** retried. All three are raised by this package and are deterministic -- an unresolvable transformer reference or a column the query doesn't return cannot succeed on a second attempt, and retrying only delays the failure and buries the real message under identical repeats. Everything a driver raises *is* retried, because transient and permanent database errors cannot be told apart reliably across six drivers, and a needless retry costs far less than a nightly load lost to one dropped connection.

Scramble jobs deliberately have no retries: a failure there can leave the table truncated, and a second pass would find it empty and report success having masked nothing.

`JobOutcome.attempts` records how many it took.

### Structured logs
`--log-format json` emits one object per record, for a collector rather than a person:

```json
{"timestamp": "2026-09-16 01:00:12.514", "level": "INFO", "logger": "lightweight_etl",
 "message": "loadOrders: completed in 12.5s, 4200 row(s)", "file": "cli.py", "line": 167,
 "job": "loadOrders", "status": "completed", "rowCount": 4200, "durationSeconds": 12.5, "attempts": 1}
```

The point isn't the encoding, it's the fields. Job completions, failures, skips and the cycle summary all carry `job`, `status`, `rowCount` and `durationSeconds`, so a collector can alert on `status="failed"` or chart rows moved per job without anyone parsing a message string. Text remains the default, since a human reading a terminal is the more common case.


## How a data job moves rows
Extracts are streamed. `_executeDataJob` pulls `chunkSize` rows from the source, transforms them, writes them to the target, and only then pulls the next chunk -- it never holds the whole result set. Peak memory is roughly `chunkSize` x row width whether the source table has ten thousand rows or ten billion, so **`chunkSize` is the memory dial**, not just an insert batch size.

That is not free of consequences, and there is one worth knowing before you size a job:

- **Extract and load interleave**, so a source that fails part-way through leaves the rows it already yielded written. Buffering the whole extract first meant a mid-extract failure wrote nothing.
- This is **invisible for `swap`, and for `upsert` with a `targetTableStage`** -- both land in the stage table, and `targetTableFinal` isn't touched until the final swap/upsert step.
- It is **visible for a stage-less `upsert`**, which writes partial results straight into the live target. Prefer a `targetTableStage` for anything large.

Streaming is per-driver, because `fetchmany()` alone bounds nothing if the driver already pulled the whole result set off the socket: `DatabaseDialect.streamingCursor()` gets mysql/mariadb a `buffered=False` cursor, postgresql a server-side named cursor, oracle a tuned `arraysize`, and sqlite/mssql a plain cursor (both already stream). `Database.stream(query, chunkSize)` returns `(columns, chunkIterator)` if you want it directly.

Scramble jobs do **not** stream, and can't: shuffling a column's values across rows means holding that column's every value at once. A scramble job's memory is bounded by its table, which is the honest reason masking a very large table needs a different approach than this.


## Incremental loads
By default a data job re-extracts its whole source every run, and `refresh` just throttles how often that happens. Set `watermarkColumn` and it instead extracts only the rows past where the last **successful** run got to:

```yaml
loadOrders:
  active: true
  refresh: 5
  sourceDatabase: sourceDb
  sourceQuery: >
    select id, customerId, amount, updatedAt from orders
    where updatedAt > {{ watermark }} - interval 5 minute
  watermarkColumn: updatedAt
  watermarkInitial: '1970-01-01 00:00:00'
  targetDatabase: targetDb
  targetTableFinal: orders
  insertStrategy: upsert
  chunkSize: 5000
```

Three parts have to agree, and configuration validation enforces it:

| Field | What it does |
| --- | --- |
| `{{ watermark }}` in `sourceQuery` | Where the stored value is bound. It is a **bound parameter**, not text substitution, so it can go anywhere a value can -- including inside a join or subquery -- and is typed by the driver rather than pasted into your SQL. The token is rewritten to whatever placeholder the dialect uses (`%s`, `:1`, `?`), so one query is portable across all six |
| `watermarkColumn` | Which of `sourceQuery`'s **own result columns** to take the high-water mark from. Read from the raw source rows, before any transform runs -- a transform may reformat the column, and what goes back into the next predicate has to be something the source can still compare against its own column |
| `watermarkInitial` | What the first run binds, before anything is stored |

`insertStrategy: upsert` is **required**. `swap` is rejected outright: it replaces the target with the stage table's contents, so staging only the changed rows would delete every row that hadn't changed.

### Why the lookback window
The `- interval 5 minute` above is not decoration. It closes the one hole that silently loses data in every incremental pipeline:

> A transaction that **starts before** your run and **commits after** it, carrying a timestamp from before your run, was never visible to your query -- but a bare `>` watermark has already moved past it. That row is skipped permanently, because the next run's predicate starts beyond where it sits.

Re-reading a small overlap every run closes it, and costs nothing precisely because `insertStrategy` must be `upsert`: re-loading a row that's already there is a no-op. That's why the two requirements go together.

Write the arithmetic in your own dialect -- the library never parses or does maths on a watermark, it only stores what came back and binds it again:

| Dialect | Lookback expression |
| --- | --- |
| mysql / mariadb | `{{ watermark }} - interval 5 minute` |
| postgresql | `{{ watermark }} - interval '5 minutes'` |
| oracle | `{{ watermark }} - numtodsinterval(5, 'minute')` |
| mssql | `dateadd(minute, -5, {{ watermark }})` |
| sqlite | `datetime({{ watermark }}, '-5 minutes')` |
| a numeric id rather than a timestamp | `{{ watermark }} - 5` |

Also take the watermark from the **database's** clock rather than the ETL host's, or you inherit clock skew as data loss.

### What is and isn't safe
The watermark is written only on success, and before the run is recorded, so every point a job can die at falls backwards into re-reading rows that were already loaded -- harmless under `upsert`:

```
load committed  ->  recordWatermark  ->  recordRun  ->  cycle sees completion
```

Dying before `recordWatermark` leaves the old watermark and re-extracts rows already loaded. Dying between the two advances the watermark (correct -- the data landed) and re-runs sooner than `refresh` asked. A watermark advanced on a *failed* job would be the one unrecoverable direction, and is why it sits inside the success branch. A run that extracts no rows leaves the stored watermark alone rather than overwriting it with a null.

**The limitation to know: watermarks cannot see deletes.** A hard-deleted source row has no `updatedAt` to exceed the watermark -- it simply stops appearing, and the target keeps it forever. Soft deletes (a flag whose change bumps `updatedAt`) work fine. For hard deletes you need a periodic full refresh, or real change-data-capture, which this library does not do. Pick per table: `swap` full-refresh for small tables and anywhere deletes matter, watermarked `upsert` for the large append/update-heavy tables where full refresh is what's hurting.


## Running the tests
```
pip install -e ".[dev]"
pytest
```
The suite stubs out `oracledb`/`psycopg2` (see `tests/conftest.py`) so it runs without native database client libraries installed, and every database-touching test uses a mocked cursor/connection rather than a live server -- it verifies the SQL and control flow this library builds, not connectivity to a real MySQL/PostgreSQL/Oracle instance.

### Integration tests
`tests/test_integration_mysql.py`, `tests/test_integration_postgresql.py`, `tests/test_integration_oracle.py`, `tests/test_integration_mssql.py`, and `tests/test_integration_mariadb.py` run the same operations against a real server instead of a mocked cursor -- schema introspection, insert/chunking, upsert (both the direct and from-stage paths), swap, truncate, the context manager, the full `runDataJobs` path (a real `multiprocessing.Pool`, a worker running in its own process, `FileMemory` surviving being pickled into it), and the reference `DatabaseMemory` from `example/example_database_memory.py`. `tests/test_integration_cross_database.py` covers the case those don't: `sourceDatabase` and `targetDatabase` pointing at two *different* database systems in the same job, with a real `sourceQueryColumnTransforms` entry applied in between (extract from MySQL, format with `lightweight_etl.builtinTransforms:currency`, load into PostgreSQL). All six files are marked `integration` and excluded from the default `pytest` run (see `addopts` in `pyproject.toml`), so they never block anyone without Docker:
```
docker compose up -d mysql postgresql oracle mssql mariadb   # starts disposable servers on
                                                               # localhost:3307 / :5433 / :1522 / :1434 / :3308
pip install -e ".[mysql,oracle,mssql,mariadb,dev]"
pip install psycopg2-binary                          # only if you don't have PostgreSQL's build toolchain (pg_config) --
                                                       # pyproject.toml's `postgresql` extra pins source-build psycopg2,
                                                       # the upstream-recommended choice for production
pytest -m integration
docker compose down                                   # when you're done
```
Each test creates its own uniquely-named table and drops it afterward, so the suite is safe to re-run against the same running containers. Missing a driver or a server just skips the affected tests with a clear reason, rather than failing. The Oracle container is [`gvenzl/oracle-free`](https://github.com/gvenzl/oci-oracle-free) (free, Apache-2.0 licensed, no Oracle Container Registry login required, unlike Oracle's own images); the SQL Server container is Microsoft's own official image, amd64-only (no native arm64 Linux build) but runs fine under emulation on Apple Silicon.

`tests/test_integration_sqlite.py` runs the same kind of real-server checks for SQLite, but needs no docker service and isn't marked `integration` -- `sqlite3` is Python's standard library, and each test gets its own throwaway file, so it's part of the plain `pytest` default run above.


## Type checking
```
pip install -e ".[dev]"
mypy
```


## License
[MIT](LICENSE)
