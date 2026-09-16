# How it works, and why

The behaviour behind the fields in [configuration.md](configuration.md), and the consequences worth knowing before you rely on it.

- [How a data job moves rows](#how-a-data-job-moves-rows)
- [Incremental loads](#incremental-loads)
- [refresh and predecessors](#refresh-and-predecessors)
- [Single runs, not a daemon](#single-runs-not-a-daemon)
- [Retries](#retries)
- [Structured logs](#structured-logs)
- [Masking](#masking)
- [Moving values between drivers](#moving-values-between-drivers)


## How a data job moves rows

Extracts are **streamed**. A data job pulls `chunkSize` rows, transforms them, writes them, and only then pulls the next chunk — it never holds the whole result set. Peak memory is about `chunkSize` × row width whether the source has ten thousand rows or ten billion.

Streaming is per-driver, because `fetchmany()` bounds nothing if the driver has already pulled every row off the socket:

| Dialect | Cursor |
| --- | --- |
| mysql, mariadb | unbuffered |
| postgresql | server-side (named) |
| oracle | `arraysize` tuned to the chunk |
| mssql, sqlite | plain — both already stream |

**One consequence to know before sizing a job:** extract and load interleave, so a source that fails part-way leaves the rows it already yielded written.

- **Invisible** for `swap`, and for `upsert` with a `targetTableStage` — both write to the stage table, and `targetTableFinal` is only touched in the last step.
- **Visible** for a stage-less `upsert`, which writes partial results straight into the live target. Use a stage table for anything large.

Masking is a stage of this same pipeline (transform, then mask, then load), so a masked job streams like any other. See [masking](#masking).


## Incremental loads

By default a job re-extracts its whole source every run. Set `watermarkColumn` and it extracts only rows past where the last **successful** run got to:

```yaml
loadOrders:
  sourceQuery: >
    select id, customerId, amount, updatedAt from orders
    where updatedAt > {{ watermark }} - interval 5 minute
  watermarkColumn: updatedAt
  watermarkInitial: '1970-01-01 00:00:00'
  insertStrategy: upsert
  # ...
```

- **`{{ watermark }}`** is a bound parameter, not text substitution. It can go anywhere a value can, including a join or subquery, and is rewritten to each dialect's own placeholder, so one query works on all six.
- **`watermarkColumn`** is read from the *raw* rows, before transforms run. A transform may reformat the column, and the next run's predicate needs a value the source can still compare against.
- **`insertStrategy: upsert`** is required. `swap` would replace the target with only the rows that changed, deleting everything else.

### Why the lookback window

The `- interval 5 minute` above closes the hole that silently loses data in incremental pipelines:

> A transaction that **starts before** your run and **commits after** it, carrying a timestamp from before your run, was never visible to your query — but the watermark has already moved past it. The row is skipped for good.

Re-reading a small overlap every run closes it, and costs nothing *because* `upsert` makes reloading an existing row a no-op. That's why the two go together.

The library never does arithmetic on a watermark, so write the lookback in your own dialect:

| Dialect | Lookback |
| --- | --- |
| mysql, mariadb | `{{ watermark }} - interval 5 minute` |
| postgresql | `{{ watermark }} - interval '5 minutes'` |
| oracle | `{{ watermark }} - numtodsinterval(5, 'minute')` |
| mssql | `dateadd(minute, -5, {{ watermark }})` |
| sqlite | `datetime({{ watermark }}, '-5 minutes')` |
| numeric id | `{{ watermark }} - 5` |

Take watermarks from the **database's** clock, not the ETL host's, or clock skew becomes data loss.

### Crash safety

On success, each step commits before the next:

```
load committed  →  record watermark  →  record run  →  report completion
```

So every point a job can die at falls *backwards*, into re-reading rows already loaded — harmless under `upsert`. Nothing is recorded for a failed job: an advanced watermark there would skip rows permanently, the one unrecoverable direction. A run that finds no new rows leaves the stored watermark where it is.

### Deletes

**Watermarks cannot see hard deletes.** A deleted row has no `updatedAt` to pass the watermark; it just stops appearing, and the target keeps it. Soft deletes — a flag whose change bumps `updatedAt` — work fine. For hard deletes you need a periodic full refresh or change-data-capture, which this library doesn't do.

A reasonable split: `swap` for small tables and anywhere deletes matter; watermarked `upsert` for large append-and-update tables where full refreshes are what hurt.

### Where watermarks are kept

In the `MemoryBackend`. `FileMemory` assumes a filesystem that persists between runs and is shared by every worker. Where that's false — a container without a volume, anything scaled across machines, serverless — use `DatabaseMemory`. `FileMemory` there doesn't fail loudly: it silently forgets every watermark and re-extracts from `watermarkInitial`. See [library.md](library.md#memory-backends).


## refresh and predecessors

**`refresh` decides whether a job is in a cycle at all. `predecessors` only orders jobs within a cycle.** So a predecessor sitting inside its own refresh window is not waited for.

A job with `refresh: 5` whose predecessor has `refresh: 60` runs alone for 11 cycles in 12, and waits for its predecessor on the 12th. That's deliberate — otherwise `refresh: 5` would silently behave as `refresh: 60` — and it's what lets an hourly dimension load and a 5-minute fact load coexist.

The trade-off is freshness, not correctness: between windows the dependent reads output up to an hour old. That's fine for a durable table, and wrong if the predecessor produces something transient the dependent consumes. Give both the same `refresh` in that case.

`lightweight-etl jobs` shows which jobs are due and which are throttled.


## Single runs, not a daemon

`lightweight-etl run` makes one pass and exits, because it should compose with whatever already schedules work — cron, a systemd timer, a Kubernetes CronJob, an Airflow task — rather than compete with it. Those give you alerting, backfill and calendar-aware schedules that `refresh` can't express; `refresh` is a throttle, not a schedule.

`refresh` still works across separate invocations, since it's checked against the durable memory backend. Running every 5 minutes with `refresh: 60` correctly skips 11 runs in 12:

```cron
*/5 * * * *  cd /srv/etl && lightweight-etl run --memory ./memory.yaml
```

`--forever` keeps the process resident, for freshness below cron's one-minute floor or where there's no scheduler. It handles `SIGINT` and `SIGTERM` by finishing the current cycle and stopping its workers cleanly; without that, a container's ordinary shutdown would orphan them mid-job.

A skipped job exits non-zero just as a failed one does: it didn't run, so its data isn't there.


## Retries

A data job with `retries: 3` gets up to four attempts. The delay starts at `retryDelaySeconds` and doubles, so a database that's down isn't hit at a fixed interval while it recovers.

Retrying a whole job is safe because both strategies converge on a re-run: `swap` restages and re-swaps, and `upsert` reapplies existing rows as a no-op.

**What isn't retried:** configuration errors, transform errors, unresolvable transformer references and masking errors. All of them come from this package and fail the same way every time; retrying would only delay the failure and bury the message under repeats. Everything a database driver raises *is* retried — transient and permanent database errors can't be told apart reliably across six drivers, and a needless retry costs far less than losing a load to one dropped connection.

**Deprecated scramble jobs have no retries.** A failure can leave the table truncated, and a second attempt would find it empty and report success having masked nothing. Masked data jobs retry like any other data job.


## Structured logs

`--log-format json` writes one object per line, for a log collector:

```json
{"timestamp": "2026-09-16 01:00:12.514", "level": "INFO", "message": "loadOrders: completed in 12.5s, 4200 row(s)",
 "job": "loadOrders", "status": "completed", "rowCount": 4200, "durationSeconds": 12.5, "attempts": 1}
```

The fields are the point. Completions, failures, skips and each cycle's summary carry `job`, `status`, `rowCount` and `durationSeconds`, so a collector can alert on `status="failed"` or chart rows per job without parsing messages. Logs go to stderr by default, since a container only collects stdout and stderr; `--log FILE` adds a file.


## Masking

Masking is a stage of a data job, between transform and load, rather than a separate kind of job. That one decision does most of the work:

- **Unmasked rows never reach the target**, not even its stage table. Masking happens in the ETL process's memory, one chunk at a time.
- **It streams.** Memory stays bounded by `chunkSize`, however large the table.
- **It gets retries, watermarks, `--dry-run` and structured logs**, because data jobs already have them.
- **Masking in place is a `swap`.** Rows load into a stage table, which is then swapped with the original, so a failed run leaves the original untouched.

Every mask is derived from `HMAC(key, domain, value)`, keyed on the value itself rather than on the row's position. The same value therefore masks the same way in every table and on every run, which keeps joins working and makes runs reproducible. Keys are masked with a keyed permutation (a Feistel network), which can't produce collisions.

A policy must list **every column the query returns**, or the job fails before writing anything. A new production column should stop the job, not flow into a non-production copy unmasked.

[masking.md](masking.md) has the strategies, the key, the manifest, `discover`, `subset`, `schema`, `clear`, and how to migrate from the deprecated `scramble` command.


## Moving values between drivers

Copying between different databases means one driver's values have to be accepted by another. Two connection settings make that work, and both apply to every job:

- **Oracle.** CLOB and BLOB columns are fetched as plain text and bytes rather than as LOB handles, which no other driver can load. The session's date formats are set to ISO 8601, so text such as `'2026-01-02 03:04:05'` loads into a `DATE` or `TIMESTAMP` column; that includes SQLite's dates and a `watermarkInitial` compared against a date column. This changes Oracle's implicit conversions between dates and text in both directions, so a `sourceQuery` that relied on the default `DD-MON-RR` format, or that calls `TO_CHAR` on a date without a format, now sees ISO text. Dates that arrive as datetime objects are unaffected.
- **SQLite.** `Decimal` values, which other drivers return for `NUMERIC` columns, are stored as their exact text. Dates, timestamps and UUIDs are stored as ISO text, replacing Python's built-in converters, which are deprecated since 3.12.

`tests/test_integration_schema.py` copies the same rows between every pair of the six databases to keep this true.
