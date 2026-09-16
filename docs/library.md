# Using it as a library

Most deployments should use the `lightweight-etl` command. Embed the library when a load needs to be one step inside a larger Python program.

The package does no file I/O of its own: you load configuration however you like and hand it over as plain data. It also never requires every driver — each is imported only when a connection of that type is opened.

- [Running jobs](#running-jobs)
- [Results](#results)
- [Memory backends](#memory-backends)
- [Streaming directly](#streaming-directly)


## Running jobs

```python
import yaml
from lightweight_etl import (Configuration, DataJobsFile, FileMemory,
                             expandEnvironmentVariables, runDataJobs)

def load(path):
    with open(path) as file:
        return expandEnvironmentVariables(yaml.safe_load(file))

databases = Configuration.validateDatabaseConfiguration(load('database.yaml'))
jobsFile = Configuration.validateJobConfiguration(load('jobs.yaml'), DataJobsFile)
Configuration.validateJobGraph(jobsFile.jobs, databaseAliases=set(databases))

result = runDataJobs(
    jobsFile=jobsFile,
    databaseConfiguration=databases,
    memory=FileMemory(memoryFile='memory.yaml'),
    )

if not result.succeeded:
    raise SystemExit(1)
```

`expandEnvironmentVariables` is what resolves `${NAME}` references — the CLI calls it for you, but a library caller must, before validating. It raises `ConfigurationError` naming every unset variable.

```python
runDataJobs(jobsFile, databaseConfiguration, memory,
            logFile=None, runForever=False, logLevel=logging.INFO, logFormat='text')

runScrambleJobs(jobsFile, databaseConfiguration,
                logFile=None, runForever=False, logLevel=logging.INFO, logFormat='text')
```

- **`runForever=False`** makes one pass and returns. `True` keeps running, honouring `refresh`, until `SIGINT` or `SIGTERM`.
- **`logFile`** is optional. Without one, attach a stream yourself: `Log(level=...).addStreamHandler(sys.stderr)`.
- **`logFormat='json'`** writes structured records — see [design.md](design.md#structured-logs).
- `runScrambleJobs` takes no `memory`, since masking jobs have no refresh window or watermark to track.

Worker processes, the pool and the dependency graph are managed for you.

Validation raises `ConfigurationError`. `runDataJobs` also raises it before starting any work if a job sets `watermarkColumn` against a backend that can't store watermarks.


## Results

Both runners return a `RunResult`. It's returned, never written anywhere — turning it into an exit code, an alert or a log line is up to you, and needs no configuration.

| `RunResult` | |
| --- | --- |
| `succeeded` | `True` only if every active job completed. A skipped job counts against it. |
| `completed`, `failed`, `skipped` | Lists of `JobOutcome`. |
| `rowCount` | Rows moved across all jobs. |
| `outcomes` | Every `JobOutcome`, in completion order. |

| `JobOutcome` | |
| --- | --- |
| `job` | The job name. |
| `status` | A `JobStatus`: `COMPLETED`, `FAILED` or `SKIPPED`. |
| `rowCount` | Rows loaded. |
| `watermark` | The high-water mark reached, for an incremental job that found rows. |
| `error` | The failure, as `ExceptionType: message`; for a skipped job, what it was waiting on. |
| `attempts` | How many tries it took. |
| `durationSeconds` | Wall-clock time. |

`error` is a string rather than the exception because outcomes cross a process boundary, and database drivers raise exceptions that don't reliably survive pickling.

A run with `runForever=True` returns only once it has been stopped, with the last cycle's outcomes.


## Memory backends

A `MemoryBackend` holds what the scheduler needs *before* a job runs: when it last ran, for `refresh`, and how far it got, for watermarks. It isn't a record of what happened — that's `RunResult`.

| Backend | Use when |
| --- | --- |
| `FileMemory(memoryFile=...)` | A filesystem persists between runs and every worker shares it. |
| `DatabaseMemory(connectionSettings=..., table=...)` | It doesn't: a container without a volume, anything across several machines, or serverless. |

`FileMemory` in the wrong environment doesn't fail loudly. It silently forgets every watermark and re-extracts from `watermarkInitial`, which is exactly what incremental loads exist to avoid.

`DatabaseMemory` needs its table to exist first — the library never creates tables you didn't ask for. `DATABASE_MEMORY_SCHEMA` is the shape; adjust the column types for your database:

```sql
CREATE TABLE lightweight_etl_memory (
    job VARCHAR(255) PRIMARY KEY,
    last_run DOUBLE,
    watermark_value VARCHAR(255),
    watermark_type VARCHAR(32)
    )
```

Watermarks are stored with a type tag, so a timestamp comes back as a timestamp and an id as an integer.

### Writing your own

Subclass `MemoryBackend`:

| Method | |
| --- | --- |
| `read()` | **abstract** — every job's last run time |
| `recordRun(job)` | **abstract** — record that `job` ran now |
| `readWatermarks()` | every job's stored watermark; defaults to none |
| `recordWatermark(job, value)` | store a watermark; defaults to raising |

The watermark pair isn't abstract, so a backend that predates incremental loads still works for every job that doesn't use them.

**One constraint:** the same instance is pickled into every worker process. Hold settings — a path, connection details — rather than an open file or connection, and open what you need inside each method.


## Streaming directly

```python
from lightweight_etl import Database

with Database(connectionSettings=databases['app']) as database:
    columns, chunks = database.stream('select * from orders', chunkSize=5000)
    for chunk in chunks:
        ...
```

`stream` returns the column names and an iterator of row lists, using each dialect's non-buffering cursor. The cursor is closed when the iterator is exhausted or abandoned.

To bind parameters, pass `parameters=` and use the dialect's own placeholder. On the `%s` dialects — mysql, postgresql and mssql — a literal `%` in a query that binds parameters must then be written `%%`.

On MySQL and MariaDB, an open stream holds its connection: run nothing else on that `Database` until the iterator is finished.
