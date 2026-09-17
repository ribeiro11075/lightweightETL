# Operating it

Running `lightweight-etl` unattended: where its state lives, and how to know what it did. For the reasoning behind single runs, retries and stopping, see [design.md](design.md).

- [Run state](#run-state)
- [Run history](#run-history)
- [Metrics](#metrics)
- [Notifications](#notifications)
- [A deployment, put together](#a-deployment-put-together)


## Run state

`run` keeps each job's last run time, watermark and masking-key fingerprint between invocations. Losing it doesn't break anything, but it costs: every incremental job re-extracts from `watermarkInitial`, and `refresh` windows start over.

| Where | Flag | Use when |
| --- | --- | --- |
| `memory.yaml` beside the configuration | (default) | The filesystem persists between runs. |
| Another file | `--memory FILE` | It should live somewhere else, such as a mounted volume. |
| A database table | `--memory-database ALIAS` | Nothing persists: containers without a volume, several machines. |

The table for `--memory-database` must exist first. Its shape is `DATABASE_MEMORY_SCHEMA`; adjust the types for your database if needed:

```sql
CREATE TABLE lightweight_etl_memory (
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
lightweight-etl run --history /var/lib/etl/history.jsonl

lightweight-etl history --history /var/lib/etl/history.jsonl
lightweight-etl history --history /var/lib/etl/history.jsonl --job loadOrders --limit 5
lightweight-etl history --history-database warehouse --format json
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
CREATE TABLE lightweight_etl_history (
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
| `--metrics-push URL` | Pushed to a Pushgateway: each job to its own group, `etl_job=<name>`, and the cycle's totals to `job=lightweight_etl`. |

| Metric | Labels | |
| --- | --- | --- |
| `lightweight_etl_job_last_run_success` | `job` | 1 if the job completed in its latest run, 0 if it failed or was skipped |
| `lightweight_etl_job_last_run_skipped` | `job` | 1 if it was skipped |
| `lightweight_etl_job_last_run_rows` | `job` | rows loaded |
| `lightweight_etl_job_last_run_duration_seconds` | `job` | |
| `lightweight_etl_job_last_run_timestamp_seconds` | `job` | when it last ran |
| `lightweight_etl_job_last_success_timestamp_seconds` | `job` | when it last completed |
| `lightweight_etl_cycle_jobs` | `status` | jobs in the latest cycle |
| `lightweight_etl_cycle_rows` | | rows in the latest cycle |
| `lightweight_etl_cycle_timestamp_seconds` | | when the latest cycle finished |

A job that isn't part of a cycle, because it's inside its `refresh` window, keeps its last values rather than disappearing. Alert on staleness rather than on a single failure, which a retry on the next run may already have fixed:

```yaml
- alert: EtlJobStale
  expr: time() - lightweight_etl_job_last_success_timestamp_seconds > 3 * 3600
```


## Notifications

`--notify-url URL`, or `$LIGHTWEIGHT_ETL_NOTIFY_URL`, posts JSON to a webhook when a cycle doesn't succeed: a job failed or was skipped, or a signal stopped the run. `--notify-on always` posts after every cycle.

```json
{
  "text": "lightweight-etl on etl-7d9f: failed -- 1 completed, 1 failed, 1 skipped, 4200 row(s)\n- loadCustomers failed: OperationalError: timeout\n- loadInvoices skipped: predecessor(s) did not complete: loadCustomers",
  "status": "failed",
  "host": "etl-7d9f",
  "summary": {"completed": 1, "failed": 1, "skipped": 1, "rows": 4200},
  "jobs": [{"job": "loadOrders", "status": "completed", "rowCount": 4200, "durationSeconds": 12.5, "error": null}, "..."]
}
```

Slack, Mattermost and Microsoft Teams incoming webhooks show `text` as it is; anything else can read the rest. The URL usually carries a token, so prefer the environment variable to the flag. Error messages come from the database drivers and can quote values from the target; keep that in mind when choosing the channel.

History, metrics and notifications never affect a run's outcome. If one fails, the failure is logged and the run carries on.


## A deployment, put together

```cron
*/15 * * * *  lightweight-etl run --config /etc/etl --memory /var/lib/etl/memory.yaml \
                --history /var/lib/etl/history.jsonl --metrics /var/lib/node_exporter/etl.prom \
                --log-format json --log /var/log/etl/etl.log --quiet
```

With `LIGHTWEIGHT_ETL_NOTIFY_URL` set in the environment, a failed run also posts to the team's channel, and `lightweight-etl history` answers what happened overnight.
