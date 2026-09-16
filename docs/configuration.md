# Configuration

Everything `lightweight-etl` does is described by YAML. This is the field reference; for *why* things behave as they do, see [design.md](design.md).

`example/configuration/` holds a complete, valid set of these files. It's validated on every test run, so it can't drift from what the code accepts — copying it is the fastest start.

- [Where configuration is found](#where-configuration-is-found)
- [Credentials](#credentials)
- [`database.yaml`](#databaseyaml)
- [`jobs.yaml` — data jobs](#jobsyaml--data-jobs)
- [`scramble.yaml` — deprecated](#scrambleyaml--deprecated)
- [Validation](#validation)


## Where configuration is found

The CLI looks for a directory holding `database.yaml` plus either `jobs.yaml` (for `run`) or `scramble.yaml` (for `scramble`), in this order. `discover` and `subset` read only `database.yaml`.

1. `--config DIR`
2. `$LIGHTWEIGHT_ETL_CONFIG`
3. `./configuration`

`--jobs FILE` and `--databases FILE` override either file individually.


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
| `host`, `user`, `password` | required except for `sqlite` | SQLite is a local file with no server or authentication, so these are omitted for it. |
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
| `workers` | required | Worker processes to run jobs concurrently. |
| `cycleSleepSeconds` | optional, `0.5` | Pause between cycles under `--forever`. |
| `jobs` | required | A map of job name to job definition. |

### Scheduling

| Field | | |
| --- | --- | --- |
| `active` | required | Whether the job runs at all. |
| `refresh` | optional | Minimum minutes between runs. Applies across separate invocations too. A predecessor inside its own refresh window is **not** waited for — see [refresh and predecessors](design.md#refresh-and-predecessors). |
| `predecessors` | optional | Jobs that must complete first. A job whose predecessor fails is **skipped**. |
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

Transforms apply to **`sourceQuery`'s own result columns**, not the target's — they act on a value as extracted, before it's mapped to a target column. Naming a column the query doesn't return fails before anything is written. A transformer that raises on a value fails the job, with the column and offending value in the error.

### Load

| Field | | |
| --- | --- | --- |
| `targetDatabase` | required | An alias from `database.yaml`. |
| `targetTableFinal` | required | The table to load. |
| `insertStrategy` | required | `swap` or `upsert` — below. |
| `targetTableStage` | required for `swap` | A staging table with the same shape. |
| `targetColumns` | optional | Target column names matching `sourceQuery`'s SELECT list **by position**. |
| `preTargetAdhocQueries` | optional | SQL run on the target before any write, the stage load included. |
| `postTargetAdhocQueries` | optional | SQL run on the target after the load. |

- **`swap`** loads `targetTableStage`, then swaps it with `targetTableFinal`. The target is replaced wholesale.
- **`upsert`** inserts or updates by primary key — from `targetTableStage` if set, otherwise straight from the extract. The target must have a primary key; `lightweight-etl run --dry-run` checks.

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


## `scramble.yaml` — deprecated

**Deprecated, and removed in the next release.** Use a data job with a [`masking`](#mask) section instead. [masking.md](masking.md#migrating-from-scrambleyaml) maps each field below to its replacement, and `lightweight-etl discover` writes the replacement job for you.

`lightweight-etl scramble` rewrites a table **in place** and logs a deprecation warning on every run.

```yaml
workers: 1
jobs:
  maskCustomers:
    active: true
    database: staging
    table: customers
    identifierColumns: [id]
    scrambleColumns: [email, phone]
    randomColumns: [birthDate]
    randomSalt: ${MASKING_SALT}
```

`workers`, `cycleSleepSeconds`, `active`, `refresh`, `predecessors`, `preTargetAdhocQueries` and `postTargetAdhocQueries` mean what they do for data jobs. Scramble jobs have no `retries`, because a failure can leave the table truncated.

| Field | | |
| --- | --- | --- |
| `database`, `table` | required | Where to mask. |
| `randomSalt` | required | Seeds regenerated text. Treat it like a secret. |
| `defaultColumnValues` | optional | Column → a constant written to every row. |
| `identifierColumns` | optional | Columns left untouched. |
| `scrambleColumns` | optional | Columns whose values are shuffled across rows. |
| `randomColumns` | optional | Columns regenerated by type: numbers and dates within their range, text as salted hashes. |
| `allDataRandom` | optional, `false` | Regenerate every column not listed above. |

Each column gets the first treatment that applies, in the order listed. **A column mentioned nowhere is shuffled, not left alone.**


## Validation

`lightweight-etl validate` checks everything above without connecting to anything: every field, every alias, every predecessor, every transformer reference, and every masking strategy, option and key length. Problems are reported all at once, as `ConfigurationError`, rather than one per run.

`lightweight-etl run --dry-run` adds the checks that need a connection: that each database is reachable, that target tables exist, that upsert targets have a primary key, and that each masking policy covers every column its query returns.
