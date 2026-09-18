# example/

Runnable demonstrations. None needs a server or credentials; each runs against throwaway SQLite databases. The test suite runs every one, so none can drift from what the code accepts.

Each demo has the same shape:

| | |
| --- | --- |
| `demo.py` | the script: `python example/<demo>/demo.py` from the repository root |
| `configuration/` | what it reads: `database.yaml`, and `jobs.yaml` where the demo doesn't generate its own |
| `transaction/` | what it writes: databases, run state, logs, manifests. Emptied at the start of each run, and ignored by git |

The walkthrough runs the `bauta` command itself. The other demos call the Python API, and before each step print the `bauta` command that does the same, with the environment variables its `configuration/` reads — paste it into a shell from the repository root to run that step yourself.

For a configuration to start your own from, rather than a demo's, run `bauta init`.

| Demo | Shows |
| --- | --- |
| [`walkthrough/`](#walkthrough) | the whole workflow, through the commands a person would type |
| [`incremental/`](#incremental) | streaming, and incremental loads that extract only what changed |
| [`masking/`](#masking) | masking, discovery and subsetting, from Python |
| [`native-masking/`](#native-masking) | the same job masked in Python and in Rust, in turn and overlapped: the speed, and identical results |


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
python example/native-masking/demo.py            # 100,000 customers
python example/native-masking/demo.py 1000000    # or as many as you like
```

Runs one masked job four times, each into a staging copy of its own: masked in Python or in Rust (the [native masker](../docs/masking.md#the-native-masker), `bauta-rs`), with reading, masking and writing either taking turns or overlapped. `BAUTA_NATIVE` and `BAUTA_PIPELINE` set each combination; by default a job overlaps only with Rust. It prints a table of the four, and checks every copy is identical:

```
                        seconds   rows a second  vs Python in turn
Python, in turn             7.4          13,563               1.0x
Python, overlapped          7.3          13,644               1.0x
Rust, in turn               2.4          41,783               3.1x
Rust, overlapped            1.9          53,486               3.9x
```

Overlapping gains little for Python, whose masking leaves no wait worth hiding, and more for Rust. Against a remote database, where each round trip is a real wait, it gains more still.

The Rust extension is optional. Without it, the demo runs the two Python runs and says how to install it: with Rust 1.83 or newer, `pip install ./mask-rs/py` from the repository root.

Tested by `tests/test_native_masking_demo.py`.

