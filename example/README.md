# example/

Executable documentation — both parts are run by the test suite, so neither can drift from what the code accepts.

## `incremental_demo.py`

```
python example/incremental_demo.py
```

Watch streaming and incremental loads work, with no server and no credentials. It loads its job from `configuration/demo/` exactly as the CLI would — YAML, then `${NAME}` expansion, then validation — builds a throwaway SQLite database under `memory/`, and runs the job three times.

The second run is the one to watch. Between runs, an already-loaded row is edited *without* its `updatedAt` changing, and a new row is added. A full re-extract would pick up both; an incremental one sees only the new row. Row counts alone wouldn't show the difference, since `upsert` is idempotent — the edited row is the tell.

Tested by `tests/test_incremental_demo.py`.

## `configuration/`

A complete sample of the files the CLI reads. Copy the top-level files to start your own:

```
mkdir configuration
cp example/configuration/*.yaml configuration/
```

| File | |
| --- | --- |
| `database.yaml` | two database aliases, with credentials read from the environment |
| `jobs.yaml` | data jobs, including an incremental one |
| `scramble.yaml` | a masking job |
| `demo/` | the SQLite configuration `incremental_demo.py` runs — a complete directory in its own right |

`demo/` follows the same `database.yaml` plus `jobs.yaml` layout that `--config DIR` expects. The glob above skips it, so it isn't copied into your own configuration.

Validated by `tests/test_shipped_example_configuration.py`. See [docs/configuration.md](../docs/configuration.md) for every field.
