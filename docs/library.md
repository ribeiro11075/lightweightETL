# Using it as a library

Most deployments should use the `bauta` command. Embed the library when a load needs to be one step inside a larger Python program.

You load configuration however you like and hand it over as plain data. Each driver is imported only when a connection of its type opens.

- [Running jobs](#running-jobs)
- [Results](#results)
- [Memory backends](#memory-backends)
- [History, metrics and notifications](#history-metrics-and-notifications)
- [Masking, discovery and subsets](#masking-discovery-and-subsets)
- [Streaming directly](#streaming-directly)


## Running jobs

```python
import yaml
from bauta import (Configuration, DataJobsFile, FileMemory,
                             expandEnvironmentVariables, runDataJobs)

def load(path):
    with open(path) as file:
        return expandEnvironmentVariables(yaml.safe_load(file))

def main():
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

if __name__ == '__main__':
    main()
```

**Keep the `if __name__ == '__main__':` guard.** Each job runs in a process of its own, started with Python's `spawn` method on every platform, and each one begins by importing your script. Without the guard, that import starts the run again.

`expandEnvironmentVariables` is what resolves `${NAME}` references — the CLI calls it for you, but a library caller must, before validating. It raises `ConfigurationError` naming every unset variable.

```python
runDataJobs(jobsFile, databaseConfiguration, memory,
            logFile=None, runForever=False, logLevel=logging.INFO, logFormat='text',
            acceptKeyChange=False, onCycle=None)
```

- **`onCycle`** is called with each cycle's `RunResult` as the cycle ends, including under `runForever`. See [below](#history-metrics-and-notifications). An exception it raises is logged, not raised.
- **`acceptKeyChange=True`** runs upsert jobs whose masking key changed since their last run; see [the key](masking.md#the-key).
- **`runForever=False`** makes one pass and returns. `True` keeps running, honouring `refresh`, until `SIGINT` or `SIGTERM`. Either signal stops new jobs from starting and lets running ones finish; see [stopping](design.md#single-runs-not-a-daemon).
- **`logFile`** is optional. Without one, attach a stream yourself: `Log(level=...).addStreamHandler(sys.stderr)`. Workers' records are written by the calling process's handlers, whichever those are.
- **`logFormat='json'`** writes structured records — see [design.md](design.md#structured-logs).

To keep two runs that share run state from overlapping, as the CLI does, hold `exclusiveRun(path)` around the call. It raises `RunInProgressError` if another process holds the same lock file.

Validation raises `ConfigurationError`. `runDataJobs` also raises it before starting any work if a job sets `watermarkColumn` against a backend that can't store watermarks, and `DependencyGraph` raises it for predecessors that form a cycle.


## Results

`runDataJobs` returns a `RunResult`, for you to turn into an exit code, an alert or a log line.

| `RunResult` | |
| --- | --- |
| `succeeded` | `True` only if every active job completed. A skipped job counts against it. |
| `completed`, `failed`, `skipped` | Lists of `JobOutcome`. |
| `rowCount` | Rows moved across all jobs. |
| `outcomes` | Every `JobOutcome`, in completion order. |
| `interrupted` | `True` if a signal stopped the run. The CLI exits with 130. |

| `JobOutcome` | |
| --- | --- |
| `job` | The job name. |
| `status` | A `JobStatus`: `COMPLETED`, `FAILED` or `SKIPPED`. |
| `rowCount` | Rows loaded. |
| `watermark` | The high-water mark reached, for an incremental job that found rows. |
| `error` | The failure, as `ExceptionType: message`; for a skipped job, what it was waiting on. |
| `attempts` | How many tries it took. |
| `durationSeconds` | Wall-clock time. |
| `masking` | For a masked job that completed, the policy applied to each column, as plain dicts. |

`RunResult.maskingManifest(jobsFile.jobs)` builds the [masking manifest](masking.md#the-manifest) for the run.

A run with `runForever=True` returns only once it has been stopped, with the last cycle's outcomes.


## Memory backends

A `MemoryBackend` holds what the scheduler needs *before* a job runs: when it last ran, for `refresh`, and how far it got, for watermarks. It isn't a record of what happened — that's `RunResult`.

| Backend | Use when |
| --- | --- |
| `FileMemory(memoryFile=...)` | A filesystem persists between runs and every worker shares it. |
| `DatabaseMemory(connectionSettings=..., table=...)` | It doesn't: a container without a volume, anything across several machines, or serverless. |

`FileMemory` in the wrong environment doesn't fail loudly: it forgets every watermark and re-extracts from `watermarkInitial`. `DatabaseMemory`'s table must exist first; its shape is `DATABASE_MEMORY_SCHEMA`, shown in [operations.md](operations.md#run-state).

### Writing your own

Subclass `MemoryBackend`:

| Method | |
| --- | --- |
| `read()` | **abstract** — every job's last run time |
| `recordRun(job)` | **abstract** — record that `job` ran now |
| `readWatermarks()` | every job's stored watermark; defaults to none |
| `recordWatermark(job, value)` | store a watermark; defaults to raising |
| `readKeyFingerprints()` | each masked job's last key fingerprint; defaults to none, which turns the key-change check off |
| `recordKeyFingerprint(job, fingerprint)` | store one, or forget it with `None`; defaults to doing nothing |

**One constraint:** the same instance is pickled into every worker process. Hold settings — a path, connection details — rather than an open file or connection, and open what you need inside each method.


## History, metrics and notifications

What the CLI's `--history`, `--metrics` and `--notify-url` do, as functions to call from `onCycle`:

```python
import os
from bauta import FileHistory, notify, writeMetricsFile
from bauta.reporting import newRunId

history = FileHistory('history.jsonl')

def report(result):
    history.append(result, newRunId())
    writeMetricsFile('/var/lib/node_exporter/etl.prom', result)
    notify(os.environ['ALERT_WEBHOOK'], result)

runDataJobs(jobsFile, databases, memory, onCycle=report)
```

| | |
| --- | --- |
| `FileHistory(path)`, `DatabaseHistory(connectionSettings, table=...)` | `RunHistory` backends: `append(result, runId)`, and `read(limit=20, job=None)` newest first. `DATABASE_HISTORY_SCHEMA` is the table. |
| `writeMetricsFile(path, result)` | Prometheus text for the textfile collector, keeping jobs that weren't in this cycle. |
| `pushMetrics(gatewayUrl, result)` | The same, to a Pushgateway. |
| `notify(url, result, always=False)` | Posts `reporting.notificationPayload(result)` if the cycle didn't succeed, or always; returns whether it posted. |

See [operations.md](operations.md) for the metrics and payload.


## Masking, discovery and subsets

The pieces behind `masking:`, `discover` and `subset` are all importable, and none of them does any I/O except through a `Database` you pass in.

```python
import os
from bauta import Database, MaskingPlan, planSubset, proposeTable

plan = MaskingPlan(key=os.environ['MASKING_KEY'], columns={'id': 'keep', 'email': 'email'})
masking = plan.bind(['id', 'email'])      # raises MaskingError for an uncovered column
masked = masking.apply([(1, 'ann@corp.com')])

with Database(connectionSettings=databases['prod']) as database:
    proposal = proposeTable(database, 'customers', sampleSize=500)
    subset = planSubset(database.getForeignKeys(), root='customers', where="region = 'eu'")
```

| | |
| --- | --- |
| `MaskingPlan(key, columns, defaultStrategy=None)` | A validated policy. `bind(columns)` checks coverage and returns an object whose `apply(rows)` masks one chunk, and whose `manifest` lists what each column gets. `fingerprint` is the key's safe identifier. |
| `STRATEGIES` | Strategy name → class. Each `Strategy` validates its own options in `validateOptions`. |
| `resolveStrategy(name)` | A built-in strategy, or your own named `module.path:ClassName`; see [your own strategies](masking.md#your-own-strategies). |
| `LOCALES` | The fake-data locales, as name → `Locale`. |
| `keyFingerprint(key)` | The same fingerprint, for a key on its own. |
| `buildMaskingManifest(outcomes, declared)` | The manifest from outcomes and each masked job's declared target and fingerprint. `RunResult.maskingManifest` wraps it. |
| `sealManifest(manifest, signingKey=None)`, `verifyManifest(manifest, signingKey=None)` | Add a manifest's digest (and signature), and check them. `verifyManifest` returns `digestValid`, `signed`, `signatureValid` and the signing key's fingerprint, and raises `ValueError` for a manifest with no integrity section. See [sealing and verifying](masking.md#sealing-and-verifying). |
| `auditJobs(jobs, returnedColumns=None, encryption=None, unreachable=None, targetColumns=None, foreignKeys=None)`, `renderAudit(report)` | The `audit` report as a dict, and as text. The optional arguments carry what `audit --connect` learns from the databases. |
| `Database.getForeignKeys()` | Every foreign key in the connection's current schema, as `ForeignKey(table, columns, referencedTable, referencedColumns, name)`. |
| `Database.sample(query, rows)` | Column names and at most `rows` rows, without reading the rest. |
| `Database.isEncrypted()` | Whether the server reports the connection as encrypted; `None` if it can't say. |
| `proposeTable(database, table, sampleSize=1000)` | A `TableProposal` with a suggested policy and the reason for it, per column. |
| `Database.getColumnDefinitions(table)`, `getPrimaryColumnNames(table)`, `tableExists(table)` | The catalog facts `schema` and upserts use. `table` may be `schema.table`; otherwise the connection's current schema is searched, and no other. |
| `subset.relatedTables(foreignKeys, roots, followChildren=True)` | Every table a subset from `roots` would copy. |
| `schema.readTable`, `schema.createStatements`, `schema.renderScript` | A table's shape, CREATE TABLE statements for a target dialect, and the script form. |
| `schema.clearTables(database, tables)` | Empties tables children-first, in one transaction. |
| `planSubset(foreignKeys, root, where, followChildren=True, ignore=(), materialize=False, quote=None)` | A `SubsetPlan`: tables in load order, a query for each, each table's parents, and the foreign keys ignored. Raises `SubsetError` on a cycle, or on a chain deeper than 16 tables. Pass `materialize=database.dialect.supportsMaterializedSelections()`, and `quote=lambda name: quoteIdentifier(database.type, name)` (from `bauta.databaseDialects`) so reserved-word columns work. |
| `synthesizeTable(database, table, rows, seed=0)`, `planTable(...)` | Fill a table with generated rows, returning how many; or just describe how, with a row generator. Raise `SynthesisError`. |


## Streaming directly

```python
from bauta import Database

with Database(connectionSettings=databases['app']) as database:
    columns, chunks = database.stream('select * from orders', chunkSize=5000)
    for chunk in chunks:
        ...
```

`stream` returns the column names and a `RowStream`: an iterator of row lists, using each dialect's non-buffering cursor. It closes itself when exhausted; one you stop reading early, close with `chunks.close()` or a `with chunks:` block. Closing the `Database` closes any stream still open.

To bind parameters, pass `parameters=` and use the dialect's own placeholder. On the `%s` dialects — mysql, postgresql and mssql — a literal `%` in a query that binds parameters must then be written `%%`.

On MySQL and MariaDB, an open stream holds its connection: run nothing else on that `Database` until the iterator is finished.
