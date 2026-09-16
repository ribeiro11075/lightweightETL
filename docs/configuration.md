# Configuration

Everything `lightweight-etl` does is described by YAML. This is the field reference; for *why* things behave as they do, see [design.md](design.md).

`example/configuration/` holds a complete, valid set of these files. It's validated on every test run, so it can't drift from what the code accepts — copying it is the fastest start.

- [Where configuration is found](#where-configuration-is-found)
- [Credentials](#credentials)
- [`database.yaml`](#databaseyaml)
- [`jobs.yaml` — data jobs](#jobsyaml--data-jobs)
- [Validation](#validation)


## Where configuration is found

The CLI looks for a directory holding `database.yaml` and `jobs.yaml`, in this order. `discover`, `subset` and `schema` read only `database.yaml`.

1. `--config DIR`
2. `$LIGHTWEIGHT_ETL_CONFIG`
3. `./configuration`

`--jobs FILE` and `--databases FILE` override either file individually.

`run` and `jobs` keep run state (last-run times and watermarks) in `memory.yaml` in the same directory, or wherever `--memory FILE` says. It sits beside the configuration rather than in the working directory, so a cron entry and a shell that start in different places still share it. A `memory.yaml` left in the working directory by an earlier version is still used, with a warning, until you move it.


## Credentials

Any string in any of these files may read from the environment:

```yaml
password: ${PROD_DB_PASSWORD}
port: ${PROD_DB_PORT:-5432}
```

| Form | Behaviour |
| --- | --- |
| `${NAME}` | The variable's value. If it's unset, the run **stops before connecting to anything**, naming every missing variable at once. |
| `${NAME:-default}` | The variable, or `default` if unset. Use for ports, hosts and schema names — **never for a secret**, which would just put the credential back in the file. |
| `$${NAME}` | A literal `${NAME}`. Needed because `sourceQuery` is arbitrary SQL. PostgreSQL dollar-quoting (`$$body$$`) is never followed by a brace, so it needs no escaping. |

An unset variable raises rather than expanding to an empty string: a blank password fails later with the driver's own unhelpful message, and a blank host connects somewhere unintended.

Kept this way, `database.yaml` holds *references* to secrets rather than secrets, and is safe to commit alongside your jobs.


## `database.yaml`

One entry per database alias. Jobs refer to databases by these aliases.

```yaml
warehouse:
  type: postgresql
  database: analytics
  host: ${WAREHOUSE_HOST}
  user: etl
  password: ${WAREHOUSE_PASSWORD}
```

| Field | | |
| --- | --- | --- |
| `type` | required | `oracle`, `mysql`, `postgresql`, `mssql`, `mariadb` or `sqlite` |
| `database` | required | The database name. For `sqlite`, a file path or `:memory:`. |
| `host`, `user`, `password` | required except for `sqlite` | SQLite is a local file with no server or authentication, so these are omitted for it. The password is held as a secret, so it never appears in a log line or a traceback. |
| `port` | optional | The driver's standard port when omitted. |
| `serviceName` / `sid` | oracle only | Exactly one is required for `type: oracle`. |


## `jobs.yaml` — data jobs

```yaml
workers: 2
jobs:
  loadOrders:
    active: true
    sourceDatabase: app
    sourceQuery: select id, customerId, amount from orders
    targetDatabase: warehouse
    targetTableFinal: orders
    insertStrategy: upsert
    chunkSize: 5000
```

### File level

| Field | | |
| --- | --- | --- |
| `workers` | required | Worker processes to run jobs concurrently, at least 1. |
| `cycleSleepSeconds` | optional, `0.5` | Pause between cycles under `--forever`. |
| `jobs` | required | A map of job name to job definition. |

### Scheduling

| Field | | |
| --- | --- | --- |
| `active` | required | Whether the job runs at all. |
| `refresh` | optional | Minimum minutes between runs. Applies across separate invocations too. A predecessor inside its own refresh window is **not** waited for — see [refresh and predecessors](design.md#refresh-and-predecessors). |
| `predecessors` | optional | Jobs that must complete first. A job whose predecessor fails is **skipped**. Predecessors that form a cycle are a validation error. |
| `retries` | optional, `0` | Extra attempts after a failure, with exponential backoff. See [retries](design.md#retries). |
| `retryDelaySeconds` | optional, `5.0` | The first backoff delay; each subsequent one doubles. |

### Extract

| Field | | |
| --- | --- | --- |
| `sourceDatabase` | required | An alias from `database.yaml`. |
| `sourceQuery` | required | The query to extract with. |
| `chunkSize` | required | Rows per batch. Extracts stream, so this is the **memory dial**: peak memory is about `chunkSize` × row width however large the source is. |
| `watermarkColumn` | optional | Makes the job incremental. See [incremental loads](design.md#incremental-loads). |
| `watermarkInitial` | required with `watermarkColumn` | The value bound on the first run, before anything is stored. |

A job with `watermarkColumn` must also put a `{{ watermark }}` placeholder in `sourceQuery` and use `insertStrategy: upsert`. Validation enforces all three.

### Transform

| Field | | |
| --- | --- | --- |
| `sourceQueryColumnTransforms` | optional | A map of column name to a list of transformer references, applied in order. |

A reference is `module.path:function_name` — any importable function taking one value and returning one. A set ships with the package:

```yaml
sourceQueryColumnTransforms:
  amount:
  - lightweight_etl.builtinTransforms:currency
  email:
  - lightweight_etl.builtinTransforms:strip
  - lightweight_etl.builtinTransforms:lower
```

`currency`, `upper`, `lower`, `strip`, `truncate`, `nullIfBlank`, `digitsOnly`, `epochSecondsToDate` (UTC), `booleanToYN`. None of them is privileged; your own module works the same way.

Transforms apply to **`sourceQuery`'s own result columns**, not the target's — they act on a value as extracted, before it's mapped to a target column. Naming a column the query doesn't return fails before anything is written. A transformer that raises on a value fails the job; the error names the column and the value's type, but never the value, since transforms see raw rows before any masking.

### Load

| Field | | |
| --- | --- | --- |
| `targetDatabase` | required | An alias from `database.yaml`. |
| `targetTableFinal` | required | The table to load: `table`, or `schema.table` for one outside the connection's current schema. |
| `insertStrategy` | required | `swap` or `upsert` — below. |
| `targetTableStage` | required for `swap` | A staging table with the same shape. For `swap`, it must be in the same schema as `targetTableFinal`. |
| `targetColumns` | optional | Target column names matching `sourceQuery`'s SELECT list **by position**. |
| `preTargetAdhocQueries` | optional | SQL run on the target before any write, the stage load included. |
| `postTargetAdhocQueries` | optional | SQL run on the target after the load. |

- **`swap`** loads `targetTableStage`, then swaps it with `targetTableFinal` by renaming the two. The target is replaced wholesale. See [how the swap works](design.md#how-a-swap-works) for what renaming means for views and on Oracle.
- **`upsert`** inserts or updates by the target's declared primary key — from `targetTableStage` if set, otherwise straight from the extract. UNIQUE constraints aren't part of the match. A target without a primary key fails the job before anything is written; `lightweight-etl run --dry-run` checks for one too.

### Mask

| Field | | |
| --- | --- | --- |
| `masking` | optional | Masks rows between the extract and the load. Its fields are `key`, `columns` and `defaultStrategy`, all documented in [masking.md](masking.md#a-masked-job). |

```yaml
masking:
  key: ${MASKING_KEY}
  columns:
    id: keep
    email: email
    customerId: { strategy: key, domain: customer }
```

Masking runs after transforms, on `sourceQuery`'s result columns. **Every column the query returns must be listed**, or the job fails before writing anything. See [masking.md](masking.md) for the strategies, domains and the key.

### Load details

**`targetColumns` is purely positional.** Left unset, `sourceQuery` must select every column of `targetTableFinal` in that table's own order. Real column names in the wrong order load data into the wrong columns *without any error*, since both sides are valid; a wrong name or count fails at the database.


## Validation

`lightweight-etl validate` checks everything above without connecting to anything: every field, every alias, every predecessor and that they form no cycle, every transformer reference, and every masking strategy, option and key length. Problems are reported all at once, as `ConfigurationError`, rather than one per run.

`lightweight-etl run --dry-run` adds the checks that need a connection: that each database is reachable, that target tables exist, that upsert targets have a primary key, and that each masking policy covers every column its query returns.
