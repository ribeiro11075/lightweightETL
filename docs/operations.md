# Operating it

Running `bauta` unattended: where its state lives, and how to know what it did. For the reasoning behind single runs, retries and stopping, see [design.md](design.md).

- [Run state](#run-state)
- [Run history](#run-history)
- [Tables](#tables)
- [Throughput](#throughput)
- [Environment variables](#environment-variables)
- [Notifications](#notifications)
- [A deployment, put together](#a-deployment-put-together)


## Run state

`run` keeps each job's last run time, watermark and masking-key fingerprint between invocations. Losing it doesn't break anything, but it costs: every incremental job re-extracts from `watermarkInitial`, and `refresh` windows start over.

| Where | Set with | Use when |
| --- | --- | --- |
| A file named by `jobs.yaml`'s `memory`, relative to it; `memory.yaml` beside it without one | (default) | The filesystem persists between runs. |
| A table | `memory: {database: ALIAS}` in `jobs.yaml` | Nothing persists: containers without a volume, several machines. |
| Another file, or a table, for one run | `--memory FILE`, `--memory-database ALIAS` | It should live somewhere else this time, such as a mounted volume. |

Watermarks in a table are stored as text with a type beside them, and read back as the same type: dates, timestamps, times, integers, floats, decimals, and bytes such as SQL Server's `rowversion`. See [tables](#tables) for its definition.

**Overlapping runs.** `run` holds a lock beside the memory file (`memory.yaml.run.lock`) for as long as it runs, and a second run that finds it held exits with status 1. With run state in a table, the lock is `memory.run.lock` where the memory file would have been, so it only separates runs on one machine. Across machines, let the scheduler do it: a Kubernetes CronJob with `concurrencyPolicy: Forbid`, or Airflow's `max_active_runs=1`.


## Run history

With `history` set in `jobs.yaml`, `run` records one entry per job after each cycle: a JSON line in a file, or a row in a table. Nothing reads it to decide what to run; it's for answering "what happened last night".

```yaml
history: ../transaction/history.jsonl    # a file, relative to jobs.yaml
history:
  database: warehouse                    # or a table
```

```
bauta history
bauta history --job loadOrders --limit 5
bauta history --format json
```

```
FINISHED             JOB                          STATUS           ROWS   SECONDS  ERROR
2026-09-16 02:00:14  loadInvoices                 skipped             0       0.0  predecessor(s) did not complete: loadCustomers
2026-09-16 02:00:13  loadCustomers                failed              0       1.0  OperationalError: timeout
2026-09-16 02:00:12  loadOrders                   completed        4200      12.5
```

`--history FILE` or `--history-database ALIAS` overrides the setting, on `run` and on `history`.

Each record has `runId` (shared by the jobs of one cycle), `job`, `status`, `rowCount`, `attempts`, `startedAt`, `finishedAt`, `durationSeconds` and `error`, cut to 2000 characters. A file grows by one line per job per run, so rotate it with `logrotate` or similar.

**Alerting on staleness.** In a table, history answers the question worth alerting on, whether a job has stopped completing, from any dashboard or monitor that runs SQL. Alert on this rather than on one failure, which the next run's retry may already have fixed:

```sql
SELECT job, max(finished_at) AS last_success
FROM bauta_history
WHERE status = 'completed'
GROUP BY job
HAVING max(finished_at) < <now, in seconds since 1970> - 3 * 3600
```


## Tables

Run state, history and manifests each need their table to exist before a run uses it. These definitions are `DATABASE_MEMORY_SCHEMA`, `DATABASE_HISTORY_SCHEMA` and `DATABASE_MANIFEST_SCHEMA` in the package, and their types work on all six databases. Times are seconds since 1970.

```sql
CREATE TABLE bauta_memory (
    job VARCHAR(255) PRIMARY KEY,
    last_run DOUBLE PRECISION,
    watermark_value VARCHAR(255),
    watermark_type VARCHAR(32)
    )

CREATE TABLE bauta_history (
    run_id VARCHAR(36) NOT NULL,
    job VARCHAR(255) NOT NULL,
    status VARCHAR(16) NOT NULL,
    row_count NUMERIC(19),
    attempts INT,
    started_at DOUBLE PRECISION,
    finished_at DOUBLE PRECISION,
    error VARCHAR(2000),
    PRIMARY KEY (run_id, job)
    )

CREATE TABLE bauta_manifest (
    run_id VARCHAR(36) NOT NULL,
    part INT NOT NULL,
    written_at DOUBLE PRECISION NOT NULL,
    content VARCHAR(2000) NOT NULL,
    PRIMARY KEY (run_id, part)
    )
```

A manifest is stored in pieces of `content`, in `part` order, because its JSON can be longer than any one text type every database shares. See [manifests in a table](masking.md#in-a-table).


## Throughput

Three things decide how fast a job moves rows, in this order.

**The masking policy**, by about sevenfold. `key` is expensive because it must be a permutation; `hash` hides as much for a thirtieth of the work wherever a column needn't stay one-to-one. See [speed](masking.md#speed).

**The [native masker](masking.md#the-native-masker)**, four to five times faster on the same policy, with identical results. Ten million rows of six masked columns take 99 seconds with it and about eight minutes without. On a wide table, where masking rather than the database sets the pace, it also spreads each chunk over several cores (`maskingThreads`): 25 masked columns went from 25,000 rows a second on one core to 73,000 on ten.

**`chunkSize` — for latency, not throughput.** On a local database, chunks from 500 rows to 200,000 finish the same job in 8.6 to 9.2 seconds. What a chunk costs is a round trip: against a database 25 ms away, a million rows take 123 seconds at `chunkSize: 500` and 8.8 at `10000`. Latency stops mattering once a chunk's masking outlasts its round trips:

```
chunkSize  >  2 x latency / per-row masking cost
```

— a few thousand rows at 5 ms, about six thousand at 25 ms. Cheap policies need *larger* chunks, having less work to hide the wait behind. With the native masker, reading, masking and writing also overlap; a job then holds three chunks (ten million rows held 80 MB).

If a job is still slow, look at the database: the target's indexes and constraints during a bulk load, and a stage table (`targetTableStage`) so the final table is written once.


## Environment variables

| Variable | Effect |
| --- | --- |
| `BAUTA_CONFIG` | where to look for configuration |
| `BAUTA_NOTIFY_URL` | webhook for failed cycles |
| `BAUTA_MANIFEST_KEY` | signs and verifies masking manifests |
| `BAUTA_MASKING_THREADS` | a number, or `auto`: overrides `jobs.yaml`'s [`maskingThreads`](configuration.md#file-level) |
| `BAUTA_NATIVE=0` | mask in Python even where the extension is installed |
| `BAUTA_PIPELINE=0` / `=1` | force reading, masking and writing to take turns, or to overlap |

The last two are for diagnosis. The implementations are tested to agree, so a difference `BAUTA_NATIVE=0` reveals is a bug worth reporting. By default the stages overlap only with the native masker; overlapping pure-Python masking costs about 2%.


## Notifications

`--notify-url URL`, or `$BAUTA_NOTIFY_URL`, posts JSON to a webhook when a cycle doesn't succeed: a job failed or was skipped, or a signal stopped the run. `--notify-on always` posts after every cycle.

```json
{
  "text": "bauta on etl-7d9f: failed -- 1 completed, 1 failed, 1 skipped, 4200 row(s)\n- loadCustomers failed: OperationalError: timeout\n- loadInvoices skipped: predecessor(s) did not complete: loadCustomers",
  "status": "failed",
  "host": "etl-7d9f",
  "summary": {"completed": 1, "failed": 1, "skipped": 1, "rows": 4200},
  "jobs": [{"job": "loadOrders", "status": "completed", "rowCount": 4200, "durationSeconds": 12.5, "error": null}, "..."]
}
```

Slack, Mattermost and Microsoft Teams incoming webhooks show `text` as it is; anything else can read the rest. The URL usually carries a token, so prefer the environment variable to the flag. Error messages come from the database drivers. Values they quote are replaced with `<redacted>` for every message format the tests know, but not every format a driver can write (see [the security model](security.md#where-unmasked-data-goes)); keep that in mind when choosing the channel.

History and notifications never affect a run's outcome. If one fails, the failure is logged and the run carries on.


## A deployment, put together

In `/etc/etl/jobs.yaml`, beside the jobs:

```yaml
memory: /var/lib/etl/memory.yaml
history:
  database: warehouse
manifest:
  database: warehouse
```

```cron
*/15 * * * *  bauta run --config /etc/etl --log-format json --log /var/log/etl/etl.log --quiet
```

With `BAUTA_NOTIFY_URL` set in the environment, a failed run also posts to the team's channel. `bauta history --config /etc/etl` answers what happened overnight, and `bauta verify-manifest --config /etc/etl` checks the latest manifest.
