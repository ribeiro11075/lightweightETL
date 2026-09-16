# example/

Executable documentation. Every part is run by the test suite, so none of it can drift from what the code accepts.

## `incremental_demo.py`

```
python example/incremental_demo.py
```

Watch streaming and incremental loads work, with no server and no credentials. It loads its job from `configuration/demo/` exactly as the CLI would — YAML, then `${NAME}` expansion, then validation — builds a throwaway SQLite database under `memory/`, and runs the job three times.

The second run is the one to watch. Between runs, an already-loaded row is edited *without* its `updatedAt` changing, and a new row is added. A full re-extract would pick up both; an incremental one sees only the new row. Row counts alone wouldn't show the difference, since `upsert` is idempotent — the edited row is the tell.

Tested by `tests/test_incremental_demo.py`.

## `masking_demo.py`

```
python example/masking_demo.py
```

Watch masking, discovery and subsetting work, again with no server. It builds a throwaway "production" and "staging" pair of SQLite files, loads its jobs from `configuration/masking/`, and then:

1. **Masks** customers and orders into staging. The two tables still join, because `customers.id` and `orders.customer_id` share the `customer` domain.
2. **Adds an `ssn` column** to production, which the policy doesn't cover. The next run fails before writing anything, and staging is left as it was. New columns never leak by default.
3. **Proposes a policy** for the changed table, as `lightweight-etl discover` does.
4. **Plans a subset** of European customers and their orders, as `lightweight-etl subset` does.

It uses a throwaway key unless `MASKING_KEY` is already set, and writes the masking manifest next to its databases.

Tested by `tests/test_masking_demo.py`.

## `configuration/`

A complete sample of the files the CLI reads. Copy the top-level files to start your own:

```
mkdir configuration
cp example/configuration/*.yaml configuration/
```

| File | |
| --- | --- |
| `database.yaml` | two database aliases, with credentials read from the environment |
| `jobs.yaml` | data jobs, including an incremental one and two masked ones |
| `scramble.yaml` | a deprecated scramble job, kept until the command is removed |
| `demo/` | the SQLite configuration `incremental_demo.py` runs |
| `masking/` | the SQLite configuration `masking_demo.py` runs |

`demo/` and `masking/` each follow the same `database.yaml` plus `jobs.yaml` layout that `--config DIR` expects. The glob above skips them, so they aren't copied into your own configuration.

The masked jobs read their key from `MASKING_KEY`, so set that along with the database passwords.

Validated by `tests/test_shipped_example_configuration.py`. See [docs/configuration.md](../docs/configuration.md) for every field.
