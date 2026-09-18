# Operating it

Running `understudy` unattended: where its state lives, and how to know what it did. For the reasoning behind single runs, retries and stopping, see [design.md](design.md).

- [Run state](#run-state)
- [Run history](#run-history)
- [Metrics](#metrics)
- [Throughput](#throughput)
- [Environment variables](#environment-variables)
- [Notifications](#notifications)
- [A deployment, put together](#a-deployment-put-together)


## Run state

`run` keeps each job's last run time, watermark and masking-key fingerprint between invocations. Losing it doesn't break anything, but it costs: every incremental job re-extracts from `watermarkInitial`, and `refresh` windows start over.

| Where | Flag | Use when |
| --- | --- | --- |
| `memory.yaml` beside the configuration | (default) | The filesystem persists between runs. |
| Another file | `--memory FILE` | It should live somewhere else, such as a mounted volume. |
| A database table | `--memory-database ALIAS` | Nothing persists: containers without a volume, several machines. |

The table for `--memory-database` must exist first. Watermarks are stored as text with a type beside them, and read back as the same type: dates, timestamps, times, integers, floats, decimals, and bytes such as SQL Server's `rowversion`. Its shape is `DATABASE_MEMORY_SCHEMA`; adjust the types for your database if needed:

```sql
CREATE TABLE understudy_memory (
    job VARCHAR(255) PRIMARY KEY,
    last_run DOUBLE PRECISION,
    watermark_value VARCHAR(255),
    watermark_type VARCHAR(32)
    )
```

**Overlapping runs.** `run` holds a lock beside the memory file (`memory.yaml.run.lock`) for as long as it runs, and a second run that finds it held exits with status 1. With `--memory-database` the lock is `memory.run.lock` in the configuration directory, so it only separates runs on one machine. Across machines, let the scheduler do it: a Kubernetes CronJob with `concurrencyPolicy: Forbid`, or Airflow's `max_active_runs=1`.


## Run history

`--history FILE` appends one JSON line per job to a file after each cycle. `--history-database ALIAS` writes the same to a table instead. Nothing reads it to decide what to run; it's for answering "what happened last night".

```
understudy run --history /var/lib/etl/history.jsonl

understudy history --history /var/lib/etl/history.jsonl
understudy history --history /var/lib/etl/history.jsonl --job loadOrders --limit 5
understudy history --history-database warehouse --format json
```

```
FINISHED             JOB                          STATUS           ROWS   SECONDS  ERROR
2026-09-16 02:00:14  loadInvoices                 skipped             0       0.0  predecessor(s) did not complete: loadCustomers
2026-09-16 02:00:13  loadCustomers                failed              0       1.0  OperationalError: timeout
2026-09-16 02:00:12  loadOrders                   completed        4200      12.5
```

Each record has `runId` (shared by the jobs of one cycle), `job`, `status`, `rowCount`, `attempts`, `startedAt`, `finishedAt`, `durationSeconds` and `error`, cut to 2000 characters. The file grows by one line per job per run, so rotate it with `logrotate` or similar.

The table for `--history-database` must exist first (`DATABASE_HISTORY_SCHEMA`). Its types work on all six databases:

```sql
CREATE TABLE understudy_history (
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
```

Times are seconds since 1970, as in the memory table.


## Metrics

Prometheus metrics, written after each cycle:

| Flag | |
| --- | --- |
| `--metrics FILE` | A text file for node_exporter's textfile collector. Name it `*.prom` inside the collector's directory. |
| `--metrics-push URL` | Pushed to a Pushgateway: each job to its own group, `etl_job=<name>`, and the cycle's totals to `job=understudy`. |

| Metric | Labels | |
| --- | --- | --- |
| `understudy_job_last_run_success` | `job` | 1 if the job completed in its latest run, 0 if it failed or was skipped |
| `understudy_job_last_run_skipped` | `job` | 1 if it was skipped |
| `understudy_job_last_run_rows` | `job` | rows loaded |
| `understudy_job_last_run_duration_seconds` | `job` | |
| `understudy_job_last_run_timestamp_seconds` | `job` | when it last ran |
| `understudy_job_last_success_timestamp_seconds` | `job` | when it last completed |
| `understudy_cycle_jobs` | `status` | jobs in the latest cycle |
| `understudy_cycle_rows` | | rows in the latest cycle |
| `understudy_cycle_timestamp_seconds` | | when the latest cycle finished |

A job that isn't part of a cycle, because it's inside its `refresh` window, keeps its last values rather than disappearing. Alert on staleness rather than on a single failure, which a retry on the next run may already have fixed:

```yaml
- alert: EtlJobStale
  expr: time() - understudy_job_last_success_timestamp_seconds > 3 * 3600
```


## Throughput

Three things decide how fast a job moves rows, in this order.

**The masking policy**, by about sevenfold. `key` is the expensive strategy
because it must be a permutation; `hash` hides just as much for a thirtieth of
the work, wherever a column doesn't have to stay one-to-one. See
[speed](masking.md#speed).

**The native masker.** `pip install "understudy-data[fast]"` masks `key`, `fpe`,
`hash`, `email` and `digits` in Rust, for four to five times the throughput on
the same policy. Optional, identical results, nothing to configure. Ten million
rows of six masked columns take 99 seconds with it and about eight minutes
without.

**`chunkSize`, but not the way it looks.** Larger chunks are not faster: from
500 rows to 200,000, a local database finishes the same job in 8.6 to 9.2
seconds. What `chunkSize` controls is how much *round-trip latency* a job pays,
because each chunk costs one trip out and one back. Against a database 25 ms
away, the same million rows take 123 seconds at `chunkSize: 500` and 8.8 at
`chunkSize: 10000`.

The threshold is where a chunk's masking outlasts its round trips:

```
chunkSize  >  2 x latency / per-row masking cost
```

At 5 ms that is a few thousand rows; at 25 ms, six thousand or so. Above it,
latency disappears and further increases only cost memory. A policy of cheap
strategies needs *larger* chunks than an expensive one, having less work to hide
the waiting behind.

Where the masker is native, reading, masking and writing overlap rather than
taking turns, which absorbs what is left (`UNDERSTUDY_PIPELINE`, under
[environment variables](#environment-variables)). A job then holds about three
chunks rather than one — still bounded by `chunkSize`, not by table size. Ten
million rows held 80 MB.

If a job is slower than this suggests, the database is usually the reason rather
than the masking: check the target's indexes and constraints during a bulk load,
and prefer a stage table (`targetTableStage`) so the final table is written once.


## Environment variables

| Variable | Effect |
| --- | --- |
| `UNDERSTUDY_CONFIG` | where to look for configuration |
| `UNDERSTUDY_NOTIFY_URL` | webhook for failed cycles |
| `UNDERSTUDY_MANIFEST_KEY` | signs and verifies masking manifests |
| `UNDERSTUDY_NATIVE=0` | mask in Python even where the extension is installed |
| `UNDERSTUDY_PIPELINE=0` / `=1` | force reading, masking and writing to take turns, or to overlap |

The last two are diagnostic. `UNDERSTUDY_NATIVE=0` rules the extension out when
a result looks wrong — the two are tested to agree, so a difference would be a
bug worth reporting. `UNDERSTUDY_PIPELINE` overrides the default, which is to
overlap only where the native masker is installed: pure-Python masking is slow
enough to swamp any wait worth hiding, so overlapping it costs about 2%.


## Notifications

`--notify-url URL`, or `$UNDERSTUDY_NOTIFY_URL`, posts JSON to a webhook when a cycle doesn't succeed: a job failed or was skipped, or a signal stopped the run. `--notify-on always` posts after every cycle.

```json
{
  "text": "understudy on etl-7d9f: failed -- 1 completed, 1 failed, 1 skipped, 4200 row(s)\n- loadCustomers failed: OperationalError: timeout\n- loadInvoices skipped: predecessor(s) did not complete: loadCustomers",
  "status": "failed",
  "host": "etl-7d9f",
  "summary": {"completed": 1, "failed": 1, "skipped": 1, "rows": 4200},
  "jobs": [{"job": "loadOrders", "status": "completed", "rowCount": 4200, "durationSeconds": 12.5, "error": null}, "..."]
}
```

Slack, Mattermost and Microsoft Teams incoming webhooks show `text` as it is; anything else can read the rest. The URL usually carries a token, so prefer the environment variable to the flag. Error messages come from the database drivers. Values they quote are replaced with `<redacted>` for every message format the tests know, but not every format a driver can write (see [the security model](security.md#where-unmasked-data-goes)); keep that in mind when choosing the channel.

History, metrics and notifications never affect a run's outcome. If one fails, the failure is logged and the run carries on.


## A deployment, put together

```cron
*/15 * * * *  understudy run --config /etc/etl --memory /var/lib/etl/memory.yaml \
                --history /var/lib/etl/history.jsonl --metrics /var/lib/node_exporter/etl.prom \
                --log-format json --log /var/log/etl/etl.log --quiet
```

With `UNDERSTUDY_NOTIFY_URL` set in the environment, a failed run also posts to the team's channel, and `understudy history` answers what happened overnight.
