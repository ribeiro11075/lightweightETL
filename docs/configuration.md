# Configuration

The field reference. For *why* things behave as they do, see [design.md](design.md). `example/starter/configuration/` is a complete set of these files, validated on every test run — copying it is the fastest start.

- [Where configuration is found](#where-configuration-is-found)
- [Credentials](#credentials)
- [`database.yaml`](#databaseyaml)
  - [Driver options and TLS](#driver-options-and-tls)
- [`jobs.yaml` — data jobs](#jobsyaml--data-jobs)
- [Validation](#validation)


## Where configuration is found

The CLI looks for a directory holding `database.yaml` and `jobs.yaml`, in this order. `discover`, `subset`, `schema` and `synthesize` read only `database.yaml`.

1. `--config DIR`
2. `$UNDERSTUDY_CONFIG`
3. `./configuration`

`--jobs FILE` and `--databases FILE` override either file individually.

Run state (last-run times and watermarks) goes where `jobs.yaml`'s [`memory`](#file-level) says, or `memory.yaml` beside `jobs.yaml` without it; `--memory FILE` overrides both. Either way it's found relative to the configuration, not the working directory, so a cron entry and a shell started elsewhere share it. See [operations.md](operations.md#run-state).

A layout that keeps what you write apart from what runs write:

```
configuration/    database.yaml, jobs.yaml (with memory: ../transaction/memory.yaml)
transaction/      memory.yaml and its locks; point --log, --manifest and --history here too
```

`example/starter/configuration/` is set up this way. Logs, manifests and history are only written where you name them, relative to the working directory like any other command-line path.


## Credentials

Any string in any of these files may read from the environment:

```yaml
password: ${PROD_DB_PASSWORD}
port: ${PROD_DB_PORT:-5432}
key: ${file:/run/secrets/masking-key}
```

| Form | Behaviour |
| --- | --- |
| `${NAME}` | The variable's value. If it's unset, the run **stops before connecting to anything**, naming every missing variable at once. |
| `${NAME:-default}` | The variable, or `default` if unset. Use for ports, hosts and schema names — **never for a secret**, which would just put the credential back in the file. |
| `${file:/path}` | The file's content, without a trailing newline — how Docker, Kubernetes and secret-store drivers mount secrets. An unreadable file stops the run like an unset variable. |
| `$${NAME}` | A literal `${NAME}`, for SQL that contains one. PostgreSQL dollar-quoting (`$$body$$`) needs no escaping. |

Kept this way, `database.yaml` holds references to secrets rather than secrets, and is safe to commit.


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
| `host`, `user` | required except for `sqlite` | SQLite is a local file with no server or authentication, so these are omitted for it. |
| `password` | required except for `sqlite`, unless `passwordCommand` is set | Held as a secret, so it never appears in a log line or a traceback. |
| `passwordCommand` | optional | A command whose output is the password, run at every connection. A string is split as a shell would split it, without a shell; `validate` rejects one that names no program or has an unterminated quote. For credentials that expire; see [passwords that expire](#passwords-that-expire). |
| `port` | optional | The driver's standard port when omitted. |
| `serviceName` / `sid` | oracle only | Exactly one is required for `type: oracle`. |
| `currentSchema` | optional, postgresql and oracle only | The schema unqualified table names, and every key and column lookup, resolve in. PostgreSQL sets `search_path` to this schema alone; Oracle sets `CURRENT_SCHEMA`. On the other databases, qualify names as `schema.table` instead. |
| `options` | optional | Extra keyword arguments for the driver's `connect()`, for anything the fields above don't cover. See below. |

### Passwords that expire

Cloud databases can take short-lived tokens instead of passwords. `passwordCommand` runs a command each time a connection opens and uses what it prints, so a token is never older than the connection that uses it:

```yaml
orders:
  type: postgresql
  host: orders.abc123.eu-west-1.rds.amazonaws.com
  port: 5432
  database: orders
  user: etl
  passwordCommand: [aws, rds, generate-db-auth-token, --hostname, orders.abc123.eu-west-1.rds.amazonaws.com,
                    --port, "5432", --username, etl, --region, eu-west-1]
  options:
    sslmode: verify-full
    sslrootcert: /etc/ssl/rds-global-bundle.pem
```

| Service | Command |
| --- | --- |
| AWS RDS / Aurora IAM | `aws rds generate-db-auth-token ...` as above. MySQL also needs `options: {auth_plugin: mysql_clear_password, ssl_ca: ...}`. |
| Azure Database for PostgreSQL / MySQL | `[az, account, get-access-token, --resource-type, oss-rdbms, --query, accessToken, -o, tsv]` |
| Google Cloud SQL IAM | `[gcloud, sql, generate-login-token]` |
| Oracle with OCI IAM | Pass the token as `options: {access_token: ...}` instead. |
| Any secret manager | Its CLI, e.g. `[vault, kv, get, -field=password, secret/etl/orders]` |

A list runs as written; a single string is split the way a shell would split it, but no shell runs it. The command has 60 seconds. Its output is never logged, and a failure is retried like any other connection error. `validate` never runs it. SQL Server's Azure AD tokens need a driver that pymssql isn't, so they aren't supported.

### Driver options and TLS

`options` is handed to the driver as it is, so it accepts whatever that driver does: `psycopg2` (any libpq parameter), `mysql.connector`, `oracledb` and `pymssql`. An option that repeats a field above (`host`, say) is refused by `validate`; set the field instead. Values are read from the environment like any other, and are left out of logs.

Encrypting the connection is the common reason to use it:

| Database | TLS |
| --- | --- |
| postgresql | `sslmode: verify-full` and `sslrootcert: /path/ca.pem`. `require` encrypts without checking the certificate. |
| mysql, mariadb | Encrypted by default when the server supports it. Add `ssl_ca: /path/ca.pem` and `ssl_verify_identity: true` to check the certificate; `ssl_disabled: true` turns TLS off. |
| oracle | `protocol: tcps`, plus `wallet_location` (and `wallet_password`) or `ssl_server_cert_dn` as your server requires. |
| mssql | Configure it in FreeTDS, not in `options`: point `FREETDSCONF` at a `freetds.conf` whose `[global]` section says `encryption = require`. pymssql's own `encryption` argument had no effect in testing with pymssql 2.4.0. |

```yaml
warehouse:
  type: postgresql
  database: analytics
  host: ${WAREHOUSE_HOST}
  user: etl
  password: ${WAREHOUSE_PASSWORD}
  currentSchema: reporting
  options:
    sslmode: verify-full
    sslrootcert: /etc/ssl/warehouse-ca.pem
    application_name: understudy
```

Settings describe what was asked for; the server decides what happened. `understudy run --dry-run` and `understudy audit --connect` report whether each connection is actually encrypted, as the server sees it.


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
| `memory` | optional, `memory.yaml` | Where the CLI keeps run state, relative to this file: `../transaction/memory.yaml` keeps it out of the configuration directory. `--memory FILE` overrides it; `validate` prints where it resolves. |
| `jobs` | required | A map of job name to job definition. |

### Scheduling

| Field | | |
| --- | --- | --- |
| `active` | required | Whether the job runs at all. |
| `refresh` | optional | Minimum minutes between runs. Applies across separate invocations too. A predecessor inside its own refresh window is **not** waited for — see [refresh and predecessors](design.md#refresh-and-predecessors). |
| `predecessors` | optional | Jobs that must complete first. A job whose predecessor fails is **skipped**. Predecessors that form a cycle are a validation error. |
| `retries` | optional, `0` | Extra attempts after a failure, with exponential backoff. See [retries](design.md#retries). |
| `retryDelaySeconds` | optional, `5.0` | The first backoff delay; each subsequent one doubles, up to five minutes. |
| `timeoutSeconds` | optional | The most the job may take, retries included. Past it, the job's process is stopped, the job fails, and its dependents are skipped. See [workers](design.md#workers). |

### Extract

| Field | | |
| --- | --- | --- |
| `sourceDatabase` | required | An alias from `database.yaml`. |
| `sourceQuery` | required | The query to extract with. |
| `chunkSize` | required, at least 1 | Rows per batch. Extracts stream, so this is the **memory dial**: peak memory is about `chunkSize` × row width however large the source is — three times that where the [native masker](masking.md#the-native-masker) overlaps reading, masking and writing. |
| `watermarkColumn` | optional | Makes the job incremental. See [incremental loads](design.md#incremental-loads). |
| `watermarkInitial` | required with `watermarkColumn` | The value bound on the first run, before anything is stored. |

A job with `watermarkColumn` must also put a `{{ watermark }}` placeholder in `sourceQuery` and use `insertStrategy: upsert`. Validation enforces all three.

### Transform

| Field | | |
| --- | --- | --- |
| `sourceQueryColumnTransforms` | optional | A map of column name to a list of transformer references, applied in order. |

A reference is `module.path:function_name` — any importable function taking the column value and returning the new one. Further arguments go in parentheses after the name, as Python literals (numbers, quoted strings, `True`, `False`, `None`):

```yaml
sourceQueryColumnTransforms:
  amount:
  - understudy_data.builtinTransforms:currency
  name:
  - understudy_data.builtinTransforms:collapseWhitespace
  - understudy_data.builtinTransforms:truncate(50)
  signup_date:
  - "understudy_data.builtinTransforms:parseDate('%d/%m/%Y')"
```

Quote a reference whose arguments contain `: `, `#` or a leading quote, as YAML would otherwise read them. `validate` checks that each reference imports and that its arguments fit the function, so a missing or misspelled argument fails there rather than on the first row. Only literals are accepted, so a reference can't run code.

These ship with the package, in `understudy_data.builtinTransforms`. Every one passes NULL through unchanged, except `defaultIfNull`, and raises on a value it can't convert rather than guessing.

| Transform | Result |
| --- | --- |
| `upper`, `lower`, `title` | Case changed: `title` gives `Ann-Marie O'Neil`. |
| `strip` | Leading and trailing whitespace removed. |
| `collapseWhitespace` | Stripped, with every run of whitespace inside turned into one space. |
| `removeAccents` | `Zoë Müller` → `Zoe Muller`, so accented and plain spellings match. Letters like `ß` and `ø` are kept. |
| `truncate(maxLength=255)` | At most `maxLength` characters: `truncate(50)`. |
| `padLeft(width, fill='0')` | Filled on the left to `width` characters: `padLeft(5)` turns `42` into `00042`. |
| `replace(old, new='')` | Every `old` replaced: `replace('-')` removes hyphens. |
| `regexReplace(pattern, replacement='')` | A regular-expression replacement; `\1` refers to a group. |
| `digitsOnly` | Only the digits: `+1 (555) 010-9999` → `15550109999`. |
| `nullIfBlank` | NULL for empty or whitespace-only text. |
| `nullIf(*values)` | NULL for any of the listed values: `nullIf('N/A', -1)`. |
| `defaultIfNull(default)` | `default` in place of NULL: `defaultIfNull('unknown')`. |
| `currency(symbol='$', decimals=2)` | `1234.5` → `$1,234.50`, `-5` → `-$5.00`; `currency('€')`, `currency('¥', 0)`. |
| `roundNumber(digits=0)` | Rounded half away from zero, keeping the value's type; a negative `digits` rounds to tens, hundreds and so on. |
| `toInteger` | An integer from text or a whole number. Blank text is NULL; `1.5` raises rather than being cut short. |
| `toDecimal` | An exact decimal from text or a number; `0.1` stays exactly `0.1`. Blank text is NULL. |
| `toBoolean` | True or false from `Y`/`N`, `yes`/`no`, `true`/`false`, `t`/`f`, `on`/`off`, `1`/`0`. Blank text is NULL; anything else raises. |
| `booleanToYN` | `Y` or `N`, for single-character flag columns. |
| `parseDate(format='%Y-%m-%d')` | A date from text, by a [strptime format](https://docs.python.org/3/library/datetime.html#format-codes). Dates pass through; datetimes lose their time. |
| `parseDateTime(format='%Y-%m-%d %H:%M:%S')` | A datetime from text; `%z` in the format keeps the UTC offset. |
| `formatDate(format='%Y-%m-%d')` | A date, datetime or time as text. |
| `epochSecondsToDate` | A date from Unix seconds, in UTC whatever the server's timezone. |
| `epochSecondsToDateTime`, `epochMillisecondsToDateTime` | A timezone-aware UTC datetime from Unix seconds or milliseconds. |
| `toString` | Text: dates as ISO 8601, bytes decoded as UTF-8. |
| `toJson` | A document or list as JSON text, with sorted keys; text passes through as it is. |

Transforms apply to **`sourceQuery`'s own result columns**, not the target's. Naming a column the query doesn't return fails before anything is written. A transformer that raises fails the job; the error names the column and the value's type, never the value.

### Load

| Field | | |
| --- | --- | --- |
| `targetDatabase` | required | An alias from `database.yaml`. |
| `targetTableFinal` | required | The table to load: `table`, or `schema.table` for one outside the connection's current schema. |
| `insertStrategy` | required | `swap` or `upsert` — below. |
| `targetTableStage` | required for `swap` | A staging table with the same shape, emptied before each load, so it must be a different table from `targetTableFinal` (compared ignoring case). For `swap`, it must be in the same schema as `targetTableFinal`. |
| `targetColumns` | optional | Target column names matching `sourceQuery`'s SELECT list **by position**. |
| `preTargetAdhocQueries` | optional | SQL run on the target before any write, the stage load included. |
| `postTargetAdhocQueries` | optional | SQL run on the target after the load. |

- **`swap`** loads `targetTableStage`, then swaps it with `targetTableFinal` by renaming the two. The target is replaced wholesale. See [how the swap works](design.md#how-a-swap-works) for what renaming means for views and on Oracle.
- **`upsert`** inserts or updates by the target's declared primary key — from `targetTableStage` if set, otherwise straight from the extract. UNIQUE constraints aren't part of the match. A target without a primary key fails the job before anything is written; `understudy run --dry-run` checks for one too.

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

**`targetColumns` is purely positional.** Left unset, `sourceQuery` must select every column of `targetTableFinal` in that table's own order. Real column names in the wrong order load data into the wrong columns *without any error*, since both sides are valid; a wrong count fails at the database.

**Column names are quoted** in the statements a load writes, so a reserved word such as `rank` or `order` works as a column. Each name is first matched to the target's own spelling, ignoring case, so `targetColumns: [job]` still finds Oracle's `JOB`; a name the table doesn't have fails the job before anything is written, and so does one that matches two columns differing only in case, until it's spelled exactly. Table names are written as given.


## Validation

`understudy validate` checks everything above without connecting to anything: every field, every alias, every predecessor and that they form no cycle, every transformer reference, and every masking strategy, option and key length. Problems are reported all at once, as `ConfigurationError`, rather than one per run.

`understudy run --dry-run` adds the checks that need a connection: that each database is reachable, that target tables exist, that upsert targets have a primary key, and that each masking policy covers every column its query returns.
