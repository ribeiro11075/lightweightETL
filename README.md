# lightweight-etl
A lightweight python library to perform ETL (Extract, Transform, Load) and table-level data masking, built around a dependency graph of jobs.

![ETL](https://www.blastanalytics.com/wp-content/uploads/extract-transform-load-icons-800x279.png)


## What this is
`library/` is a standalone, typed package you import directly -- it never reads a file, never knows a file path, and never requires every database driver to be installed. Your side of the contract is just configuration:

- **jobs** -- a validated `DataJobsFile` or `ScrambleJobsFile` (see the config field reference under Installation, below)
- **databases** -- a validated `Dict[str, DatabaseConnectionConfig]`
- **log location** -- a `Path` to write to
- **memory location** -- a `Path` to persist run history to (data jobs only, so `refresh` windows survive a restart)

Worker processes, the process pool, and the dependency graph between jobs are all managed internally; you don't write a worker function or touch `multiprocessing` yourself.

`example/` shows one way to wire this up end to end -- it's a fully self-contained reference implementation, with its own `example/configuration/` (YAML job/database definitions), `example/log/`, and `example/memory/` (gitignored, written at runtime). Nothing outside `example/` is deployment-specific.

### Project layout
| Path | What it is |
| --- | --- |
| `library/` | The package. `Configuration` (validation), `Database` + per-dialect SQL (`databaseDialects.py`), `DependencyGraph` (scheduling), `Transform`/`Scramble` (row-level work), `Memory`/`Log`, and `runner.py` (the two public entry points, `runDataJobs`/`runScrambleJobs`) |
| `example/` | A working reference deployment: two scripts, their YAML config, and gitignored runtime output |
| `tests/` | pytest suite -- see "Running the tests" below |


## Installation

1. Setup prerequisites
    - [ ] **Required**: Install Python 3.9+, e.g. following [this guide](https://realpython.com/installing-python)
    - [ ] **Required**: Install pip, e.g. following [this guide](https://howchoo.com/g/mze4ntbknjk/install-pip-python)
    - [ ] **Optional**: Install the [Oracle Client](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html) -- only needed if you're installing the `oracle` extra below

1. Create a virtual environment and install the package
    - [ ] **Optional**: Create and activate a virtual environment, e.g. `python3 -m venv .venv && source .venv/bin/activate` (Windows: `.venv\Scripts\activate`)
    - [ ] **Required**: From the repository root, install with the database driver(s) you actually need -- `library`'s only hard dependencies are `pyyaml` and `pydantic`; each database driver is an optional extra, imported lazily so installing one doesn't require the others:
        - `pip install -e ".[mysql]"` -- mysql only
        - `pip install -e ".[postgresql]"` -- postgresql only
        - `pip install -e ".[oracle]"` -- oracle only (also needs the Oracle Client above)
        - `pip install -e ".[all]"` -- every driver
        - append `,dev` to any of the above to also install `pytest`/`mypy`, e.g. `pip install -e ".[all,dev]"`

1. Point the example scripts at your own databases and jobs
    - The repo ships a small sample under `example/configuration/` (fake hosts, fake credentials, generic table/column names -- nothing here is a real deployment) so `example/example_jobs.py` and `example/example_scramble.py` run out of the box up through configuration validation. Replace the values with your own to run against real infrastructure.
    - [ ] **Required**: Edit `example/configuration/database.yaml` -- one entry per database alias:
        - `type` (**Required**): `oracle`, `mysql`, or `postgresql`
        - `user` / `password` / `database` / `host` (**Required**): connection credentials
        - `port` (**Optional**): defaults to the driver's standard port when omitted
        - `threaded` (**Optional**, oracle only)
        - `serviceName` / `sid` (oracle only): exactly one of these is **required** for `type: oracle`
    - [ ] **Required**: Edit `example/configuration/jobs.yaml` (loaded by `example/example_jobs.py`)
        - `workers` (**Required**): number of processes to run jobs concurrently (number)
        - `jobs` (**Required**): a map of job name -> job definition. Each job supports:
            - `active` (**Required**): whether the job runs at all (boolean)
            - `predecessors` (**Optional**): jobs that must complete first (list of job names)
            - `refresh` (**Optional**): minimum minutes between runs (number)
            - `sourceDatabase` / `targetDatabase` (**Required**): database aliases from `database.yaml`
            - `insertStrategy` (**Required**): `swap` or `upsert`
                - `swap`: loads into `targetTableStage`, then swaps it with `targetTableFinal`
                - `upsert`: upserts from `targetTableStage` if set, otherwise straight from the extracted data
            - `chunkSize` (**Required**): rows per insert batch (number)
            - `targetTableStage` (**Optional**): required when `insertStrategy: swap`
            - `targetTableFinal` (**Required**): target table in the target database
            - `columnTransforms` (**Optional**): map of column name -> list of transformer references, each in the form `"module.path:function_name"` (e.g. `example.example_transforms:currency`). The function can live anywhere importable -- `example/example_transforms.py` is just a reference implementation -- and is resolved at job-run time via `library.resolveTransformer`
            - `preTargetAdhocQueries` / `postTargetAdhocQueries` (**Optional**): queries run on the target database before/after load
            - `sourceQuery` (**Required**): query to extract data from the source database

1. Or edit `example/configuration/scramble.yaml` for scramble/masking jobs (loaded by `example/example_scramble.py`)
    - `workers` (**Required**): number of processes to run jobs concurrently (number)
    - `jobs` (**Required**): a map of job name -> job definition. Each job supports:
        - `active` / `predecessors` (see above)
        - `database` / `table` (**Required**): where to scramble data in place
        - `defaultColumnValues` (**Optional**): map of column name -> a fixed value to write into every row
        - `identifierColumns` (**Optional**): columns left untouched
        - `scrambleColumns` (**Optional**): columns whose existing values are shuffled across rows
        - `randomColumns` (**Optional**): columns replaced with freshly generated random values (used with `allDataRandom: false`; set `allDataRandom: true` to randomize every column not otherwise handled above)
        - `randomSalt` (**Required**): salt used to seed generated random text
        - `preTargetAdhocQueries` / `postTargetAdhocQueries` (**Optional**): see above

Invalid configuration (missing fields, an unknown `insertStrategy`, a `sourceDatabase` that isn't defined in `database.yaml`, a `predecessors` entry that isn't a real job, ...) raises `library.ConfigurationError` with a description of every problem found, rather than failing partway through a job run.


## Running it
Once you have validated configuration, running jobs is one call -- see `example/example_jobs.py` and `example/example_scramble.py` for the full picture (loading YAML, validating it, then calling one of these):

```python
from library import Configuration, DataJobsFile, runDataJobs

databaseConfiguration = Configuration.validateDatabaseConfiguration(rawDatabaseConfig)
jobsFile = Configuration.validateJobConfiguration(rawJobConfig, DataJobsFile)
Configuration.validateJobGraph(jobsFile.jobs, databaseAliases=set(databaseConfiguration.keys()))

runDataJobs(
    jobsFile=jobsFile,
    databaseConfiguration=databaseConfiguration,
    logDirectory=logPath,
    memoryDirectory=memoryPath,
    runForever=True,
    )
```

`runForever=True` (the default) keeps running, honoring each job's `refresh` window; pass `False` for a single pass over every active job, then return.

`runScrambleJobs(jobsFile, databaseConfiguration, logDirectory, runForever=False)` is the scramble-job equivalent -- no `memoryDirectory`, since scramble jobs don't have a `refresh` window to track, and it defaults to a single pass (`runForever=False`) since masking a table is normally one-shot rather than a recurring job. Both accept `runForever` either way -- it's your call, not something the library assumes based on job type.


## Running the tests
```
pip install -e ".[dev]"
pytest
```
The suite stubs out `cx_Oracle`/`psycopg2` (see `tests/conftest.py`) so it runs without native database client libraries installed, and every database-touching test uses a mocked cursor/connection rather than a live server -- it verifies the SQL and control flow this library builds, not connectivity to a real MySQL/PostgreSQL/Oracle instance.

### Integration tests
`tests/test_integration_mysql.py` runs the same operations against a real MySQL server instead of a mocked cursor -- schema introspection, insert/chunking, upsert (both the direct and from-stage paths), swap, truncate, and the context manager, all round-tripped through an actual database. It's marked `integration` and excluded from the default `pytest` run (see `addopts` in `pyproject.toml`), so it never blocks anyone without Docker:
```
docker compose up -d mysql          # starts a disposable mysql:8.4 on localhost:3307
pip install -e ".[mysql,dev]"
pytest -m integration
docker compose down                 # when you're done
```
Each test creates its own uniquely-named table and drops it afterward, so the suite is safe to re-run against the same running container.


## Type checking
```
pip install -e ".[dev]"
mypy
```


## License
[MIT](LICENSE)
