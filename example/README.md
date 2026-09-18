# example/

Runnable demonstrations, and a starter configuration to copy. None needs a server or credentials; each runs against throwaway SQLite databases. The test suite runs every one, so none can drift from what the code accepts.

Each demo has the same shape:

| File or folder | What it is |
| --- | --- |
| `demo.py` | the script: `python example/<demo>/demo.py` from the repository root |
| `configuration/` | what it reads: `database.yaml`, and `jobs.yaml` where the demo doesn't generate its own |
| `transaction/` | what it writes: databases, run state, logs, manifests. Emptied at the start of each run, and ignored by git |

The walkthrough runs the `bauta` command itself. The other demos call the Python API, and before each step print the `bauta` command that does the same, with the environment variables its `configuration/` reads — paste it into a shell from the repository root to run that step yourself.

| Demo | Shows |
| --- | --- |
| [`walkthrough/`](#walkthrough) | the whole workflow, through the commands a person would type |
| [`incremental/`](#incremental) | streaming, and incremental loads that extract only what changed |
| [`masking/`](#masking) | masking, discovery and subsetting, from Python |
| [`native-masking/`](#native-masking) | Python against Rust on a narrow table, and Rust on one core against all of them on a wide one: the speed, and identical results |
| [`starter/`](#starter) | not a demo: a configuration to copy for your own databases |


## walkthrough

```
python example/walkthrough/demo.py
```

Builds a small shop's "production" database -- customers with national ids and phone numbers, orders, support tickets full of personal details, and payment cards -- and makes a safe staging copy of its Portuguese and Spanish customers:

1. **`discover`** proposes a masking policy.
2. **`subset --mask`** generates a job per table for those customers and everything they reference. The script then applies what a reviewer would decide, and records each decision at the top of the jobs file: masked ids sharing domains, `fpe` for national ids, `redact` for ticket text, and no copy at all of payment cards.
3. **`schema`** creates staging's tables.
4. **`audit --connect --strict`** checks the reviewed policy against production.
5. **`run`** copies and masks, writing a signed manifest and run history.
6. **`verify-manifest`** checks the manifest.
7. **`synthesize`** fills staging's payment cards with generated rows.
8. **`history`** shows what ran.

Its `configuration/` holds only `database.yaml`, since `subset` generates the jobs. Every command runs from `transaction/` and is pointed at that file with `--databases`, so the generated jobs, run state and manifest land in `transaction/` beside the databases. It ends by showing customers and a support ticket before and after, including what `redact` leaves behind, and writes the session to `transaction/walkthrough.md`.

Tested by `tests/test_walkthrough.py`, which also checks that no production email, phone number or card number reaches staging.


## incremental

```
python example/incremental/demo.py
```

Loads its job the way the CLI does -- YAML, then `${NAME}` expansion, then validation -- and runs it three times against one SQLite database.

The second run is the one to watch. Between runs, an already-loaded row is edited *without* its `updatedAt` changing, and a new row is added. A full re-extract would pick up both; an incremental one sees only the new row. Row counts alone wouldn't show the difference, since `upsert` is idempotent -- the edited row is the tell.

Tested by `tests/test_incremental_demo.py`.


## masking

```
python example/masking/demo.py
```

Builds a "production" and a "staging" SQLite database, then:

1. **Masks** customers and orders into staging. The two tables still join, because `customers.id` and `orders.customer_id` share the `customer` domain.
2. **Adds an `ssn` column** to production, which the policy doesn't cover. The next run fails before writing anything, and staging is left as it was. New columns never leak by default.
3. **Proposes a policy** for the changed table, as `bauta discover` does.
4. **Plans a subset** of European customers and their orders, as `bauta subset` does.

It uses a throwaway key unless `MASKING_KEY` is already set.

Tested by `tests/test_masking_demo.py`.


## native-masking

```
python example/native-masking/demo.py                  # 100,000 narrow rows and 1,000,000 wide ones: about 2½ minutes
python example/native-masking/demo.py 100000 200000    # the same comparison, quicker
```

Masks two tables in Python and in Rust (the [native masker](../docs/masking.md#the-native-masker), `bauta-rs`), each run into a staging copy of its own, and checks every copy of a table is identical.

A narrow table, six masked columns, where writing to the database sets the pace. Reading, masking and writing take turns or overlap (`BAUTA_PIPELINE`), in Python or in Rust (`BAUTA_NATIVE`):

```
Narrow table: 100,000 rows, 6 masked columns
                               seconds   rows a second     vs first
Python, in turn                    7.5          13,309         1.0x
Python, overlapped                 7.5          13,351         1.0x
Rust, in turn                      1.8          54,742         4.1x
Rust, overlapped                   1.3          78,786         5.9x
```

A wide table, 25 masked columns, where masking sets the pace. Rust masks on one core or on all of them (`BAUTA_MASKING_THREADS`):

```
Wide table: 1,000,000 rows, 25 masked columns
                               seconds   rows a second     vs first
Rust, in turn                     46.4          21,529         1.0x
Rust, overlapped                  39.5          25,336         1.2x
Rust, in turn, all cores          19.4          51,669         2.4x
Rust, overlapped, all cores       13.6          73,366         3.4x
```

Ten cores, an M1 Pro. Overlapping gains little for Python, whose masking leaves no wait worth hiding, and more for Rust. The narrow table runs on one core only: its time goes to writing the database, which more cores don't speed up. Against a remote database, where each round trip is a real wait, overlapping gains more still.

The Rust extension is optional. Without it, the demo runs the two Python runs and says how to install it: with Rust 1.83 or newer, `pip install ./mask-rs/py` from the repository root. One run at a time: each empties `transaction/` first, so a second refuses to start while one is running.

Tested by `tests/test_native_masking_demo.py`.


## starter

A complete configuration to start your own from:

```
mkdir configuration
cp example/starter/configuration/*.yaml configuration/
```

| File | What it holds |
| --- | --- |
| `database.yaml` | two database aliases, with credentials read from the environment |
| `jobs.yaml` | data jobs, including an incremental one and two masked ones, with run state, history and the manifest kept in `transaction/` |

Set the variables it reads -- `SOURCE_DB_PASSWORD`, `TARGET_DB_PASSWORD` and `MASKING_KEY` -- then edit it for your databases. The CLI reads `./configuration` by default; `--config DIR` points it anywhere else.

Validated, with every demo's configuration, by `tests/test_shipped_example_configuration.py`. See [docs/configuration.md](../docs/configuration.md) for every field.
