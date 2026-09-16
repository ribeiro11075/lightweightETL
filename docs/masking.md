# Masking

Masking is a stage of a data job. Rows are masked as they stream from source to target, so **unmasked data never reaches the target**, not even a stage table.

```
extract (prod)  →  transform  →  mask  →  load (staging)
```

Since masking runs inside a data job, it also gets streaming, retries, watermarks, `--dry-run` and structured logs.

- [A masked job](#a-masked-job)
- [Strategies](#strategies)
- [Domains: keeping joins intact](#domains-keeping-joins-intact)
- [Every column must be covered](#every-column-must-be-covered)
- [The key](#the-key)
- [Masking in place](#masking-in-place)
- [The manifest](#the-manifest)
- [Proposing a policy: `discover`](#proposing-a-policy-discover)
- [Copying a subset: `subset`](#copying-a-subset-subset)
- [Creating and refreshing the copy](#creating-and-refreshing-the-copy)
- [Limits](#limits)
- [Migrating from `scramble.yaml`](#migrating-from-scrambleyaml)


## A masked job

Add a `masking` section to any data job:

```yaml
maskCustomers:
  active: true
  sourceDatabase: prod
  sourceQuery: select * from customers
  targetDatabase: staging
  targetTableFinal: customers
  insertStrategy: upsert
  chunkSize: 5000
  masking:
    key: ${MASKING_KEY}
    columns:
      id:          { strategy: key, domain: customer }
      email:       email
      phone:       { strategy: digits, keepLeading: 1 }
      full_name:   fakeName
      birth_date:  { strategy: dateShift, maxDays: 30 }
      notes:       'null'
      created_at:  keep
```

| Field | | |
| --- | --- | --- |
| `key` | required | The secret every mask is derived from, at least 16 characters. Read it from the environment. See [the key](#the-key). |
| `columns` | required | Column name → policy. A policy is a strategy name, or a mapping with `strategy`, an optional `domain`, and that strategy's options. |
| `defaultStrategy` | optional | The policy for any column not listed in `columns`. Leave it unset unless you mean it. See [every column must be covered](#every-column-must-be-covered). |

Masking applies to `sourceQuery`'s result columns, after `sourceQueryColumnTransforms`. Normalize values with a transform first (`strip`, `lower`) so that equal values mask equally. Column names match case-insensitively, since Oracle reports them in upper case.

`'null'` needs its quotes, because a bare `null` in YAML means "no value".

`lightweight-etl validate` checks the key length, every strategy name and every option, without connecting to anything.


## Strategies

A NULL stays NULL under every strategy except `constant` and `null`.

| Strategy | Result | Options |
| --- | --- | --- |
| `keep` | Unchanged. The explicit way to say a column was reviewed. | |
| `null` | NULL. The right choice for free text. | |
| `constant` | `value` in every row, NULLs included. | `value` (required) |
| `hash` | An opaque hex token, e.g. `cust_9f86d081884c7d65`. | `length` (12–64, default 16), `prefix` |
| `email` | Still an email address, e.g. `u9f86d081884c@example.test`. Keyed on the lower-cased address. | `length` (8–40, default 12), `mailDomain` (default `example.test`), `keepDomain` |
| `digits` | Each digit replaced, everything else kept: `+1 (555) 010-9999` → `+1 (831) 402-5517`. Keyed on the digits alone, so formatting doesn't matter. Integers keep their digit count. | `keepLeading`, `keepTrailing` (e.g. `4` for a card number) |
| `number` | A number of the same type and precision, either within `variance` of the original (default `0.1`) or within `min`–`max`. | `min` + `max`, or `variance` (0–1); `decimals` |
| `dateShift` | Moved by a keyed number of whole days, never zero. Times of day are kept. ISO 8601 text, which is how SQLite stores dates, is written back in the same format. | `maxDays` (default 30) |
| `key` | A one-to-one mapping, safe for primary and foreign keys. See below. | `charset`: `alphanumeric` (default), `digits`, `hex` |
| `fakeName`, `fakeFirstName`, `fakeLastName`, `fakeCity`, `fakeCompany`, `fakeStreetAddress` | Realistic values from bundled lists. Not unique. | `maxLength` |
| `shuffle` | The column's values rearranged among rows in the same chunk. **Not anonymization:** every real value is still in the table. | |

A value a strategy can't handle fails the job, for example text given to `number`. The error names the column and the value's type, never the value itself.

### `key`

`key` guarantees that different inputs give different outputs. That's what a primary key needs: a hash reduced to a column's width would eventually collide. It is a keyed permutation: a Feistel network with HMAC-SHA256 rounds. That's the same structure as the NIST FF1 and FF3-1 standards, but not a certified implementation of either, and it adds no dependency.

The output has the same shape as the input:

- An integer keeps its sign and number of digits.
- Text keeps its length, and every character that isn't masked stays put. With `alphanumeric`, digits map to digits and letters to letters of the same case. `digits` masks only digits. `hex` masks `0-9a-f`, case-insensitively, which suits UUIDs and hex tokens.
- A UUID object stays a UUID. Its version digit isn't preserved.

`charset` is set once per column rather than detected from each value, because detection could give two different shapes the same output.

`number` handles ordinary numeric columns. `key` is for identifiers, whose values have to stay distinct.


## Domains: keeping joins intact

Every mask is derived from the key, a **domain**, and the value itself:

```
mask = strategy( HMAC(key, domain, value) )
```

The same value in the same domain always masks the same way, in every table and on every run. So joins survive masking as long as both sides share a domain and a strategy:

```yaml
# customers
id:          { strategy: key, domain: customer }
# orders
customer_id: { strategy: key, domain: customer }
```

The domain defaults to the column's lower-cased name, so `email` in two tables already agrees without any configuration. Set `domain` explicitly whenever the two sides of a relationship have different names.

Masking is also reproducible. Masks don't depend on row order or a random seed, so a re-run or next week's incremental load produces the same values. The one exception is `shuffle`, which depends on how rows fall into chunks.

Numbers are keyed on their decimal text. An id read as an `int` from one database, as a `Decimal` from another, or as `'42'` from a text column therefore masks the same way under `hash`. `key` is stricter, since its output keeps the input's type: pick one type per domain.


## Every column must be covered

**Every column `sourceQuery` returns must appear in `columns`.** Otherwise the job fails before it writes anything, and the error names the columns:

```
MaskingError: column(s) returned by sourceQuery but not in the masking policy: ssn.
Add each one -- `keep` if it needs no masking -- or set defaultStrategy
```

This guards against the most common masking failure: someone adds a column to production, nobody updates the policy, and the column's real values flow into a non-production copy. With `select *`, a new column stops the job instead.

A column named in the policy that the query doesn't return is also an error, since it's almost always a typo that leaves the real column uncovered.

These errors are never retried. `lightweight-etl run --dry-run` finds them without loading anything. It runs each masked job's query, reads one row and discards it unexamined.

`defaultStrategy` turns the check off for unlisted columns. Only `'null'` or `constant` keep the safety property, since they discard whatever a new column holds.


## The key

The key is what stops someone who knows this scheme from hashing likely values, such as common names or every phone number in an area code, and matching them against the masked output.

- **Read it from the environment**: `key: ${MASKING_KEY}`. Never give it a `${NAME:-default}`, and never commit it.
- It must be at least 16 characters. Use a random one: `python -c "import secrets; print(secrets.token_urlsafe(32))"`.
- **Rotating it changes every mask.** A copy masked under the old key won't join to one masked under the new key.
- The key never appears in logs, errors or the manifest, and pydantic hides it from the configuration's repr. Runs log a **fingerprint** instead: a short, non-reversible identifier. Two runs with the same fingerprint used the same key.

Whoever holds the key can confirm a guess (for example "is this row Alice?") by masking the guess and comparing, so give it the same care as production credentials.


## Masking in place

To mask a table where it stands, make the source and target the same and load through a stage table:

```yaml
maskCustomersInPlace:
  sourceDatabase: staging
  sourceQuery: select * from customers
  targetDatabase: staging
  targetTableStage: customers_masked_stage   # same shape, created beforehand
  targetTableFinal: customers
  insertStrategy: swap
  chunkSize: 5000
  masking: ...
```

Masked rows stream into the stage table, and the stage is then swapped with the original. If a run fails, the original is untouched. The deprecated `scramble` command truncated the table first, so a failed run could leave it empty.

Use `swap` rather than `upsert` for this if any key column is masked. An upsert matches rows by primary key, and a masked key would add new rows instead of replacing the old ones.

`swap` renames tables, and databases differ in how they treat foreign keys that point at a renamed table. For a table that other tables reference, copy into a separate database instead.


## The manifest

```
lightweight-etl run --manifest audit/manifest.json
```

This writes a record of what was masked, how, and under which key fingerprint. It's the artifact an auditor asks for:

```json
{
  "generatedAt": "2026-09-16T13:25:57+00:00",
  "jobs": [
    {
      "job": "maskCustomers",
      "status": "completed",
      "sourceDatabase": "prod",
      "targetDatabase": "staging",
      "targetTable": "customers",
      "keyFingerprint": "d5930cf83dea",
      "rowCount": 48210,
      "columns": [
        {"column": "id", "strategy": "key", "domain": "customer", "source": "column"},
        {"column": "notes", "strategy": "null", "domain": null, "source": "column"}
      ]
    }
  ]
}
```

- `columns` lists the columns the query actually returned, and the policy applied to each. `source` says whether a column was listed in `columns` or fell to `defaultStrategy`.
- A masked job that failed or was skipped is still listed, with its status and no columns. "This copy was not refreshed" belongs in the record too.
- The manifest is written even when the run fails. It never contains a value or the key.

From Python, `RunResult.maskingManifest(jobsFile.jobs)` returns the same dictionary.


## Proposing a policy: `discover`

```
lightweight-etl discover --database prod --table customers --table orders --target staging --output proposal.yaml
```

For each table, `discover` reads the schema and samples rows (`--sample`, default 1000), then writes a `jobs.yaml` with a proposed policy for every column. Each proposal carries a comment saying what it was based on:

```yaml
    masking:
      key: ${MASKING_KEY}
      columns:
        id: {strategy: keep}  # numeric key (domain customers); use key if the ids themselves are meaningful
        email: {strategy: email}  # name suggests an email address
        phone: {strategy: digits}  # name suggests a phone number
        notes: {strategy: 'null'}  # name suggests free text, which can hold PII anywhere
        status: {strategy: keep}  # no sign of personal data -- review
```

- **Names first, then values.** Column names are matched against common patterns (email, phone, SSN, card, name, address, birth date and so on). A name-based suggestion is dropped if it doesn't fit the column's type, so `place_of_birth` isn't treated as a date. Sampled values are then checked for emails, national identifiers, card numbers (with a Luhn check), IP addresses, UUIDs, dates, phone numbers and long free text.
- **Keys are decided together.** Primary keys, the columns that foreign keys reference, and the foreign-key columns themselves get matching domains, so both ends of a relationship agree. Numeric keys are proposed as `keep`, since surrogate ids reveal little, and text keys as `key`.
- **Sampled values stay in memory.** None of them is printed, logged or written.
- **Load settings.** With a separate `--target`, jobs upsert and load parent tables before child tables. Without one, the proposal masks in place through a `<table>_masked_stage` swap.
- `--output` refuses to overwrite an existing file, so it can't replace a policy that has already been reviewed.

Treat the result as a starting point for review. It isn't a finished policy.


## Copying a subset: `subset`

```
lightweight-etl subset --database prod --target staging \
    --root customers --where "created_at >= '2026-01-01'" --mask --output subset/jobs.yaml
```

`subset` generates one data job per table so that the copy is **referentially complete**: every foreign key in a copied row points at a row that was also copied. It reads the foreign keys from the source database's catalog, including composite keys, and follows them in both directions:

- **Down** (skip this with `--no-children`): rows that reference the selected rows. A customer's orders, and those orders' line items.
- **Up** (always): rows that anything selected references. The products those line items point at, and whatever those products point at in turn.

Each job's `sourceQuery` is plain SQL made of nested `EXISTS` subqueries, which runs unchanged on all six databases. Jobs load parents before children, so the target can keep its foreign keys enabled. `--mask` adds a proposed policy for each table, as `discover` does.

**Cycles** can't be followed in SQL that works on every database. This includes a table that references itself, like `employees.manager_id`. `subset` reports the cycle and stops. Break it with `--ignore-foreign-key employees.manager_id`, and make sure the ignored column is nullable or masked to `'null'` in the target. Otherwise a row may point at one that wasn't copied.

The generated queries grow with the depth of the schema, because every level nests the levels above it. For a very deep schema, root the subset lower down, or use `--no-children`.

The target's tables must already exist. `subset` generates jobs; it doesn't create tables. See the next section for creating them.


## Creating and refreshing the copy

### `schema`: creating the target's tables

```
lightweight-etl schema --database prod --target staging --table customers --related --apply
```

`schema` reads the source's tables and creates matching tables in the target, **in the target's own dialect**: an Oracle `NUMBER(12,2)` becomes `NUMERIC(12,2)` on PostgreSQL, and `NVARCHAR(MAX)` on SQL Server becomes `CLOB` on Oracle.

- **What it copies:** columns, nullability, the primary key, and foreign keys between the tables being created. Not indexes, defaults, check constraints, triggers or permissions. A non-production copy rarely needs them, and translating them between databases is where schema tools go wrong.
- **Which tables:** `--table` names them. `--related` adds every table a subset rooted there would copy, which is what the headers of generated subset jobs suggest. `--no-children` narrows that to the tables `--table` references.
- **Without `--apply`,** it prints the SQL, or writes it to `--output`, for you to review or hand to a DBA. Each lossy choice is a comment above its table.
- **With `--apply`,** it creates the tables in dependency order and **skips any that already exist**. It never alters or drops anything, so it's safe to re-run.
- **`--stage-suffix _stage`** also creates `<table>_stage` tables for `swap` jobs, with the same columns and key but no foreign keys. When `--target` is the source database itself, as for [masking in place](#masking-in-place), only the stage tables are created.
- **`--no-foreign-keys`** leaves foreign keys out. Use it when tables reference each other in a cycle; add those keys yourself once both tables exist.

A few conversions change what a column can hold, and the generated SQL notes each one:

| Source | Target | Becomes |
| --- | --- | --- |
| Oracle `DATE` | anything else | a timestamp, since Oracle's `DATE` includes a time of day |
| any `TIME` | Oracle | `VARCHAR2(16 CHAR)`, since Oracle has no time-of-day type |
| a time-zone-aware timestamp | MySQL, MariaDB | `DATETIME(6)`, and the offset is lost |
| unbounded text in a key | MySQL, SQL Server, Oracle | 255 characters, since those can't index unbounded text |
| a boolean stored as an integer (SQLite, MySQL, Oracle `NUMBER(1)`) | anything | a small integer, since PostgreSQL won't load an integer into `BOOLEAN` |
| a type it doesn't recognize | anything | text |

Every combination of the six databases is tested: tables are created on the target and a copy then loads into them.

### `clear`: emptying the copy before a refresh

A subset's jobs upsert, so rows from an earlier subset stay unless the copy is emptied first:

```
lightweight-etl clear --config subset --dry-run     # which tables, in what order
lightweight-etl clear --config subset --yes
lightweight-etl run --config subset --force
```

- **What it empties:** `clear` deletes every row from each active job's `targetTableFinal`, child tables before their parents, so the target's foreign keys don't block it. `--job` narrows it to particular jobs.
- **All or nothing:** each database is emptied in one transaction. If one table can't be emptied, for example because a table outside the set still references its rows, nothing is deleted.
- **`DELETE`, not `TRUNCATE`:** PostgreSQL, SQL Server and Oracle refuse to truncate a table that a foreign key references.
- **Nothing happens without `--yes`.** Without it, `clear` exits with status 2.
- **Incremental jobs are refused.** Their stored watermark would survive, so the next run would load only new rows into the empty table.
- **Run with `--force` afterwards**, so a `refresh` window can't leave a cleared table empty.

Between `clear` and the end of the run, the copy is empty or partly loaded. For a copy people use while it refreshes, load into stage tables and `swap` instead.


## Limits

- **Free text** can hold personal data anywhere in it, so mask it with `null` or `constant`. `hash` would only replace the text with an opaque token, and `keep` would copy it as it is.
- **Unique columns** need enough bits to avoid collisions. `hash` enforces a minimum length for that reason. The `fake*` strategies are never unique. For a unique column, use `key`, which never collides.
- **`number` with `variance`** keeps magnitudes realistic, which also reveals them roughly. Use `min`/`max` if the magnitude itself is sensitive.
- **`dateShift`** is keyed on the date, so everyone born on the same day still shares a birthday after masking. That's what keeps the data consistent, and it means dates are shifted, not randomized.
- **Masking hides values, not patterns.** Row counts, NULL rates and relationships are all preserved, which is the point, and a combination of kept columns (zip code, birth year and gender) can still identify someone. Review what you `keep`.
- **Hard deletes** aren't propagated by incremental loads, masked or not. See [design.md](design.md#deletes).


## Migrating from `scramble.yaml`

`lightweight-etl scramble` and `scramble.yaml` are **deprecated** and will be removed in the next release. They log a warning on every run. A scramble job holds the whole table in memory, truncates it and reinserts the rows. It has none of the properties above: masks aren't consistent across tables or runs, and a failure can leave the table empty.

To migrate, turn each scramble job into an in-place data job ([masking in place](#masking-in-place)) with `sourceQuery: select * from <table>`, and translate its fields:

| `scramble.yaml` | `masking.columns` |
| --- | --- |
| `defaultColumnValues: {status: active}` | `status: {strategy: constant, value: active}` |
| `identifierColumns: [id]` | `id: keep`, or `key` if the id should be masked too |
| `scrambleColumns: [name]` | `name: shuffle`, or better, `fakeName` |
| `randomColumns` (text) | `hash`, `email` or a `fake*` strategy |
| `randomColumns` (number) | `number` with `min`/`max` |
| `randomColumns` (date) | `dateShift` |
| `randomSalt` | `key`, read from the environment |
| `allDataRandom: true` | list every column explicitly |
| a column mentioned nowhere (shuffled) | must now be listed. Nothing is shuffled by default. |
| `pre/postTargetAdhocQueries` | unchanged: data jobs have the same fields |

`lightweight-etl discover --database <alias> --table <table>` writes that in-place job for you, with a proposed policy.
