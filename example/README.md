# example/

Two unrelated things, both documentation you can execute rather than prose you have to trust.

## `incremental_demo.py` — watch it work

```
python example/incremental_demo.py
```

Self-contained: no configuration, no credentials, no server. It builds a throwaway SQLite database under `memory/incremental_demo/`, runs a real job through `runDataJobs` three times, and prints the watermark moving.

The interesting moment is the second run. Between runs it edits an already-loaded row *without* touching its `updatedAt`, and adds a new one. A full re-extract would pick both up; an incremental one can only see the new row. Row counts alone would show nothing, since `upsert` is idempotent — so the edited row is the discriminator.

It is exercised by `tests/test_incremental_demo.py`, because a showcase that silently breaks on a refactor is worse than no showcase.

## `configuration/` — start from this

A complete, valid set of the three files the CLI expects. Copy it and edit:

```
cp -r example/configuration ./configuration
lightweight-etl validate
```

`--config DIR` looks for `database.yaml` plus `jobs.yaml` (or `scramble.yaml`) in one directory, so this is also the clearest statement of that convention.

Credentials are read from the environment with `${NAME}`, so these files hold *references* to secrets rather than secrets, and are safe to keep in version control. `tests/test_shipped_example_configuration.py` validates all three against the real models on every test run, so what's documented here cannot drift from what the code accepts.

The demo does **not** use this configuration — it builds its own inline, so it can run with nothing installed and nothing configured.
