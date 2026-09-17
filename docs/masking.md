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
- [Reviewing policies: `audit`](#reviewing-policies-audit)
- [Proposing a policy: `discover`](#proposing-a-policy-discover)
- [Copying a subset: `subset`](#copying-a-subset-subset)
- [Creating and refreshing the copy](#creating-and-refreshing-the-copy)
- [Generating data instead: `synthesize`](#generating-data-instead-synthesize)
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

`understudy validate` checks the key length, every strategy name and every option, without connecting to anything.


## Strategies

A NULL stays NULL under every strategy except `constant` and `null`.

| Strategy | Result | Options |
| --- | --- | --- |
| `keep` | Unchanged. The explicit way to say a column was reviewed. | |
| `null` | NULL. The right choice for free text. | |
| `constant` | `value` in every row, NULLs included. | `value` (required) |
| `hash` | An opaque hex token, e.g. `cust_9f86d081884c7d65`. | `length` (12–64, default 16), `prefix` |
| `email` | Still an email address, e.g. `u9f86d081884c@example.test`. Keyed on the lower-cased address. | `length` (8–40, default 12), `mailDomain` (default `example.test`), `keepDomain` |
| `digits` | Each digit replaced, everything else kept: `+1 (555) 010-9999` → `+1 (831) 402-5517`. Keyed on the digits alone, so formatting doesn't matter. Integers keep their digit count. Digits in any script (full-width `１２３`, Arabic-Indic `١٢٣`) are masked too, and written back in their own script. | `keepLeading`, `keepTrailing` (e.g. `4` for a card number) |
| `number` | A number of the same type and precision, either within `variance` of the original (default `0.1`) or within `min`–`max`. A value the variance would round back to itself, such as a small integer, moves one step instead; zero stays zero. | `min` + `max`, or `variance` (0–1); `decimals` |
| `dateShift` | Moved by a keyed number of whole days, never zero. Times of day are kept. ISO 8601 text, which is how SQLite stores dates, is written back in the same format. `0001-01-01` and `9999-12-31` are kept, since they mean "no date" or "forever"; a date near either is shifted away from it. | `maxDays` (default 30) |
| `key` | A one-to-one mapping, safe for primary and foreign keys. See below. | `charset`: `alphanumeric` (default), `digits`, `hex` |
| `fpe` | Like `key`, but using NIST's FF1 format-preserving encryption, for policies that must name a standard. See below. | `charset`: `alphanumeric` (default), `digits`, `hex`; `strict` |
| `fakeName`, `fakeFirstName`, `fakeLastName`, `fakeCity`, `fakeCompany`, `fakeStreetAddress` | Realistic values from bundled lists. Not unique. | `maxLength`; `locale`, below |
| `redact` | Free text with each recognisable identifier replaced: emails, phone numbers, US SSNs, card numbers and IBANs (both checksum-verified), IPv4 addresses. **Names aren't found.** See below. | `replacement`: `label` (default) or `mask`; `detect`: a list of `email`, `phone`, `ssn`, `card`, `iban`, `ip`; `patterns`: extra regular expressions |
| `shuffle` | The column's values rearranged among rows in the same chunk. **Not anonymization:** every real value is still in the table, and a small chunk barely moves them. See [limits](#limits). | |

A value a strategy can't handle fails the job, for example text given to `number`. The error names the column and the value's type, never the value itself.

### `key`

`key` guarantees that different inputs give different outputs. That's what a primary key needs: a hash reduced to a column's width would eventually collide. It is a keyed permutation: a Feistel network with HMAC-SHA256 rounds. That's the same structure as the NIST FF1 and FF3-1 standards, but not a certified implementation of either, and it adds no dependency.

The output has the same shape as the input:

- An integer keeps its sign and number of digits.
- Text keeps its length, and every character that isn't masked stays put. With `alphanumeric`, digits map to digits and letters to letters of the same case. `digits` masks only digits. `hex` masks `0-9a-f`, case-insensitively, which suits UUIDs and hex tokens.
- A UUID object stays a UUID. Its version digit isn't preserved.

`charset` is set once per column rather than detected from each value, because detection could give two different shapes the same output.

**Only ASCII letters and digits are masked**, so a value with letters or digits in another script fails the job rather than being copied through: `Дмитрий`, `王伟`, `José` or `١٢٣` under `alphanumeric`, and digits outside 0-9 under `digits` or `hex`. Other characters (spaces, punctuation, `€`) are kept as they are. Use `hash`, a `fake*` strategy or `null` for names in any script, and the `digits` strategy for numbers written in other digits.

**Values are limited to 256 characters**, or 256 digits for an integer. `key` is for identifiers, and its cost grows with the square of a value's length; a longer value fails the job with a suggestion of `hash`, `redact` or `null`. The same limits apply to `fpe`.

`number` handles ordinary numeric columns. `key` is for identifiers, whose values have to stay distinct.

### `fpe`

`fpe` is `key`'s alternative for when a security review asks for a published algorithm: FF1 from NIST SP 800-38G Rev. 1, with AES-256. It is checked against NIST's sample vectors, and needs the `cryptography` package (`pip install "understudy-data[fpe]"`; the `oracle` extra already brings it).

- It keeps shapes the way `key` does: integers keep sign and digit count, text keeps its length and every character outside `charset`. With `alphanumeric`, letters and digits share one alphabet, so a letter may become a digit; `key` keeps each character's class.
- The masking key is turned into an AES key per domain, and the domain goes into FF1's tweak.
- **FF1 needs at least a million possible values**: six digits, five hex characters or four alphanumerics. Shorter values are masked with `key`'s permutation instead, and still never collide with longer ones, since lengths are kept. **`strict: true`** fails the job on a shorter value instead, for policies that require FF1 for every value; the error gives the minimum length, never the value. `audit` notes each `fpe` column without `strict`.
- It is slower than `key`: roughly 25,000 distinct values a second per worker. Repeated values are remembered, as described under [speed](#speed).

Only encryption is implemented. Nothing in the package can reverse a mask.

### Fake data by country

The `fake*` strategies draw from an international mix of names and places by default. `locale` picks one country's names, cities and address layout instead: `en_US`, `en_GB`, `de_DE`, `fr_FR`, `es_ES`, `it_IT`, `nl_NL` or `pt_BR`.

```yaml
columns:
  full_name: { strategy: fakeName, locale: de_DE }          # Lukas Schneider
  street:    { strategy: fakeStreetAddress, locale: fr_FR } # 12 rue des Lilas
```

Leaving `locale` out keeps the original lists, so existing masks don't change.

### `redact`

`redact` is for free text worth keeping, like support tickets, where `null` would lose too much:

```yaml
columns:
  ticket_body: { strategy: redact }
  # Called Ann at [PHONE], card [CARD] was declined, reply to [EMAIL]
  agent_notes: { strategy: redact, replacement: mask, patterns: ['ACC-\d{6}'] }
  # Called Ann at +7 (362) 248-5677, card 4095 9565 2378 1111 was declined, account redacted-b934cf4da0a9
```

- `label` writes `[EMAIL]`, `[PHONE]`, `[SSN]`, `[CARD]`, `[IBAN]`, `[IP]`, or `[REDACTED]` for your own `patterns`.
- `mask` writes keyed values of the same shape instead: the same address always becomes the same masked address, a card keeps its last four digits, an IP becomes `10.x.x.x`.
- Phone detection is broad on purpose: any run of 7–15 digits that isn't a date counts, order numbers included. Leave `phone` out of `detect` where that removes too much.
- **It can't recognise names, addresses written in words, or anything else without a fixed shape.** "Call Maria about her divorce" passes through unchanged. Where text may hold that, use `null`. `audit` notes every `redact` column for this reason.

### Your own strategies

A policy can name a class of your own as `module.path:ClassName`:

```python
# acme/masks.py
from understudy_data.masking import Strategy

class Initials(Strategy):
    OPTIONS = {'separator': str}                   # option name -> check that returns the value

    def mask(self, value):                          # called for each non-NULL value
        separator = self.options.get('separator', '.')
        suffix = self.keyedHash.digest(str(value).encode()).hex()[:4]
        return separator.join(word[0] for word in value.split()) + separator + suffix
```

```yaml
columns:
  full_name: { strategy: "acme.masks:Initials", separator: "-" }
```

Derive anything random from `self.keyedHash` (`digest`, `below`, `unit`, `permute`), so the mask stays keyed, consistent within its domain, and reproducible. `validate` imports the class and checks its options; the module must also be importable wherever jobs run. The manifest records the strategy by the name the policy used.

If `mask()` depends on nothing but the value, set `CACHEABLE = True` on the class, and repeated values are remembered rather than masked again. It's off by default, since a strategy could depend on something else.

### Speed

Masking is pure Python, a column at a time. `hash`, `email`, `digits` and the `fake*` strategies manage hundreds of thousands of values a second on one core, and `key` and `fpe` a few tens of thousands, since each value takes several rounds of HMAC or AES.

Those six strategies remember what they've masked, up to 16,384 values per column: text of up to 256 characters, integers and UUIDs, which are the types whose equal values always mask the same way. A foreign key or a low-cardinality column repeats values constantly, so this makes them many times faster; a column of unique values, such as a primary key, gains nothing. A column's cache holds a few megabytes at most, and nothing that isn't already in the job's memory.


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

These errors are never retried. `understudy run --dry-run` finds them without loading anything. It runs each masked job's query, reads one row and discards it unexamined.

`defaultStrategy` turns the check off for unlisted columns. Only `'null'` or `constant` keep the safety property, since they discard whatever a new column holds.


## The key

The key is what stops someone who knows this scheme from hashing likely values, such as common names or every phone number in an area code, and matching them against the masked output.

- **Read it from the environment**: `key: ${MASKING_KEY}`. Never give it a `${NAME:-default}`, and never commit it.
- It must be at least 16 characters. Use a random one: `python -c "import secrets; print(secrets.token_urlsafe(32))"`.
- **Rotating it changes every mask.** A copy masked under the old key won't join to one masked under the new key. So each masked job's key fingerprint is recorded when it completes, and an **upsert** job whose key has changed since stops the run: its target still holds rows masked under the old key. Empty those targets with `understudy clear`, which also forgets the recorded fingerprints, or pass `--accept-key-change` (`acceptKeyChange=True` from Python) if you mean it. A `swap` job replaces its whole target, so it just carries on.
- The key never appears in logs, errors or the manifest, and pydantic hides it from the configuration's repr. Runs log a **fingerprint** instead: a short, non-reversible identifier. Two runs with the same fingerprint used the same key.

Whoever holds the key can confirm a guess (for example "is this row Alice?") by masking the guess and comparing, so give it the same care as production credentials. [security.md](security.md) sets out what masking does and doesn't protect, for a security review.


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

Masked rows stream into the stage table, and the stage is then swapped with the original. If a run fails before the swap, the original is untouched. The removed `scramble` command truncated the table first, so a failed run could leave it empty.

Use `swap` rather than `upsert` for this if any key column is masked. An upsert matches rows by primary key, and a masked key would add new rows instead of replacing the old ones.

`swap` renames tables. Views follow on every database, but foreign keys from other tables, and PostgreSQL's materialized views, don't; see [how a swap works](design.md#how-a-swap-works). For a table that other tables reference, copy into a separate database instead.


## The manifest

```
understudy run --manifest audit/manifest.json
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
  ],
  "tool": {"name": "understudy-data", "version": "0.1.0"},
  "configuration": {"jobsFile": "configuration/jobs.yaml", "sha256": "9f2c…"},
  "integrity": {
    "algorithm": "sha256",
    "digest": "4be1…",
    "signatureAlgorithm": "hmac-sha256",
    "signature": "0c7a…",
    "signingKeyFingerprint": "71d04ab2e913"
  }
}
```

- `columns` lists the columns the query actually returned, and the policy applied to each. `source` says whether a column was listed in `columns` or fell to `defaultStrategy`.
- A masked job that failed or was skipped is still listed, with its status and no columns. "This copy was not refreshed" belongs in the record too.
- The manifest is written even when the run fails. It never contains a value or the key.

- `configuration` names the jobs file and its SHA-256, so a reviewer can tell which policy produced the run.

From Python, `RunResult.maskingManifest(jobsFile.jobs)` returns the manifest before sealing, without `tool`, `configuration` or `integrity`; `sealManifest` adds the last.

### Sealing and verifying

Every manifest carries a SHA-256 digest of its own content, which shows it hasn't been edited since it was written. Anyone can recompute a digest, though, so it doesn't show who wrote it. For that, set a signing key and the manifest is also signed with HMAC-SHA256:

```
export UNDERSTUDY_MANIFEST_KEY=...      # at least 16 characters; not the masking key
understudy run --manifest audit/manifest.json

understudy verify-manifest audit/manifest.json
```

`verify-manifest` exits 0 for an intact manifest (saying whether it was signed), and 1 if it was altered or its signature doesn't match. With `UNDERSTUDY_MANIFEST_KEY` set, an unsigned manifest exits 1 too: otherwise an edited manifest could pass by dropping its signature and recomputing its digest. A signed manifest records its key's fingerprint; verifying it without that key exits 2 rather than half-answering. `--manifest-key-variable` reads the key from another variable, on both commands.


## Reviewing policies: `audit`

```
understudy audit                      # offline, from the configuration alone
understudy audit --connect --strict   # also asks the databases; fails on warnings
```

`audit` lists every job, whether it masks, and what each masked column gets. It then reports what a reviewer should question — things validation allows, because they can be right:

| Severity | Finding |
| --- | --- |
| error | A masked query returns a column the policy doesn't cover, or names one it doesn't return (`--connect`). |
| error | A masked query couldn't be run to check (`--connect`). |
| warning | A column is kept unmasked although its name suggests personal data (`email`, `ssn`, `phone`, ...). |
| warning | `defaultStrategy` is `keep`, so any column added to the source later is copied unmasked. |
| warning | A job copies from a database without masking while other jobs mask what they read from it. |
| warning | A masked job reads over a connection that isn't encrypted, as the server reports it (`--connect`). |
| warning | `shuffle` on an incremental job, whose small chunks leave values near their own rows. |
| warning | A domain is masked two ways, or under two keys, in one target database, so its masks won't match across the columns that share it. Copies in different target databases may use different keys. |
| warning | A foreign-key column isn't masked exactly like the key it references (strategy, options, domain and key), so the copied references won't match (`--connect`). |
| note | Columns that fall to `defaultStrategy`, by name (`--connect`). |

Without `--connect`, columns are shown as declared. With it, each masked query is run for a single row, discarded unexamined, to list the columns it really returns and the policy each one gets.

The foreign-key check reads the foreign keys of each target database and of the sources copied into it, since a copy often declares none. A key's tables are matched to jobs by `targetTableFinal`'s name, without its schema, and a masked job's target columns to its query's columns by position, as the load matches them. A job that doesn't mask copies every column as it is, and so does `keep`; a reference masked with `null` points at nothing, so it can't break.

`audit` exits 1 on an error, and with `--strict` on a warning too, so it can gate a CI pipeline. `--format json` writes the same report for other tools, and `--output FILE` writes it to a file. `--job` narrows it.


## Proposing a policy: `discover`

```
understudy discover --database prod --table customers --table orders --target staging --output proposal.yaml
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
understudy subset --database prod --target staging \
    --root customers --where "created_at >= '2026-01-01'" --mask --output subset/jobs.yaml
```

`subset` generates one data job per table so that the copy is **referentially complete**: every foreign key in a copied row points at a row that was also copied. It reads the foreign keys from the source database's catalog, including composite keys, and follows them in both directions:

- **Down** (skip this with `--no-children`): rows that reference the selected rows. A customer's orders, and those orders' line items.
- **Up** (always): rows that anything selected references. The products those line items point at, and whatever those products point at in turn.

Each job's `sourceQuery` is plain SQL: a `WITH` clause defining each table's selection once, joined by `EXISTS`. It runs unchanged on all six databases (MySQL from 8.0, MariaDB from 10.2). On PostgreSQL and SQLite the selections are marked `MATERIALIZED`, so each is computed once. Jobs load parents before children, so the target can keep its foreign keys enabled. `--mask` adds a proposed policy for each table, as `discover` does.

**Cycles** can't be followed in SQL that works on every database. This includes a table that references itself, like `employees.manager_id`. `subset` reports the cycle and stops. Break it with `--ignore-foreign-key employees.manager_id`, and make sure the ignored column is nullable or masked to `'null'` in the target. Otherwise a row may point at one that wasn't copied.

**Depth is limited.** A subset may follow a chain of up to 16 tables (`customers` → `orders` → `order_items` → ... is a chain of three). MySQL refuses deeper ones, and SQL Server takes seconds to plan them and fails past about 24, so `subset` refuses them up front rather than generating queries that fail. For a deeper schema, root the subset lower down, use `--no-children`, or split it into subsets rooted at different tables.

**Each table is read at a different moment**, by its own job. Rows written to the source between two of those reads can reference rows that weren't copied, and the target's foreign keys will then reject them. Subset from a replica or a snapshot that isn't being written to, or make `--where` exclude recent rows (`created_at < '2026-09-01'`) so late writes fall outside the subset.

The target's tables must already exist. `subset` generates jobs; it doesn't create tables. See the next section for creating them.


## Creating and refreshing the copy

### `schema`: creating the target's tables

```
understudy schema --database prod --target staging --table customers --related --apply
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
understudy clear --config subset --dry-run     # which tables, in what order
understudy clear --config subset --yes
understudy run --config subset --force
```

- **What it empties:** `clear` deletes every row from each active job's `targetTableFinal`, child tables before their parents, so the target's foreign keys don't block it. `--job` narrows it to particular jobs.
- **All or nothing:** each database is emptied in one transaction. If one table can't be emptied, for example because a table outside the set still references its rows, nothing is deleted.
- **`DELETE`, not `TRUNCATE`:** PostgreSQL, SQL Server and Oracle refuse to truncate a table that a foreign key references.
- **Nothing happens without `--yes`.** Without it, `clear` exits with status 2.
- **Incremental jobs are refused.** Their stored watermark would survive, so the next run would load only new rows into the empty table.
- **Run with `--force` afterwards**, so a `refresh` window can't leave a cleared table empty.

Between `clear` and the end of the run, the copy is empty or partly loaded. For a copy people use while it refreshes, load into stage tables and `swap` instead.


## Generating data instead: `synthesize`

Some tables can't be copied at all, even masked, and a new system may have no production data yet. `synthesize` fills existing tables with generated rows, using nothing but the target's own catalog:

```
understudy synthesize --database staging --table customers:1000 --table orders:5000 --dry-run
understudy synthesize --database staging --table customers:1000 --table orders:5000 --yes
```

```
customers: 1000 row(s)
  id                           primary key  sequential, from 1
  email                        name         an email address at example.test
  first_name                   name         a first name
  phone                        name         digits shaped like +1 555 010 0000
  birth_date                   name         a birth date between 1940 and 2004
  balance                      type         a decimal with 2 places, sometimes NULL
  sample: {'id': 1, 'email': 'u5e9bbb2e91df@example.test', 'first_name': 'Hugo', ...}
```

- **Keys are unique.** Integer keys continue after the table's current maximum; text keys run `S1`, `S2`, ... after the current row count (`S0001`, `S0002`, ... in a fixed-width column, so each stays distinct at full width); UUID keys are generated.
- **Foreign keys resolve.** Values are drawn from the parent's existing rows, so parents are filled first; `--table` order doesn't matter. A table whose key is made only of foreign keys gets as many rows as its parents allow, which may be fewer than asked.
- **Names drive realism.** Columns whose names suggest personal data (email, names, phone, postal code, birth date, city, company, address...) get realistic values, from the same rules `discover` uses. Everything else is random within its type: numbers within their precision, text within its length, dates since 2015. Nullable columns are NULL about one time in ten.
- **Reproducible.** The same `--seed` on the same starting tables makes the same rows.
- `--rows` sets the count for any `--table` given without one. Nothing is written without `--yes`.

**Limits:** a table that references itself through a NOT NULL column can't be filled, since its first row would have nothing to point at; a nullable self-reference is left NULL. Values are plausible, not statistically like production: there are no correlations between columns, and no skew. Tables must exist first; `schema` creates them from a source's definitions.


## Limits

- **Free text** can hold personal data anywhere in it. `null` or `constant` remove it all. `redact` keeps the text and removes identifiers with a recognisable shape, but not names. `hash` would only replace the text with an opaque token, and `keep` would copy it as it is.
- **Scripts other than Latin.** `key` and `fpe` refuse letters and digits outside ASCII rather than copy them; `digits` and `redact` handle digits in any script. `redact` finds only email addresses written in ASCII.
- **Unique columns** need enough bits to avoid collisions. `hash` enforces a minimum length for that reason. The `fake*` strategies are never unique. For a unique column, use `key`, which never collides.
- **`number` with `variance`** keeps magnitudes realistic, which also reveals them roughly. Use `min`/`max` if the magnitude itself is sensitive.
- **`dateShift`** is keyed on the date, so everyone born on the same day still shares a birthday after masking. That's what keeps the data consistent, and it means dates are shifted, not randomized.
- **`shuffle` needs large chunks.** Values only move within a chunk, so a row keeps its own value with probability 1/chunk size, and a chunk of one row isn't shuffled at all. The last chunk of a load and a small incremental run are both small. Don't use `shuffle` on incremental jobs.
- **Masking hides values, not patterns.** Row counts, NULL rates and relationships are all preserved, which is the point, and a combination of kept columns (zip code, birth year and gender) can still identify someone. Review what you `keep`.
- **Hard deletes** aren't propagated by incremental loads, masked or not. See [design.md](design.md#deletes).


## Migrating from `scramble.yaml`

`understudy scramble` and `scramble.yaml` have been **removed**. A scramble job held the whole table in memory, truncated it and reinserted the rows. It had none of the properties above: masks weren't consistent across tables or runs, and a failure could leave the table empty.

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

`understudy discover --database <alias> --table <table>` writes that in-place job for you, with a proposed policy.
