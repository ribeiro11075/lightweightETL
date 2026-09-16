"""Puts the repo root on sys.path, and stubs oracledb/psycopg2 in sys.modules
so any test that does exercise a dialect's connect() doesn't need those native
client libraries installed (the package itself only imports them lazily, inside
connect(), so this isn't required just to import lightweight_etl -- see
test_lazy_driver_imports.py).

Only stubs a module that's genuinely not installed (checked via find_spec,
which locates a module without importing it). Checking `moduleName not in
sys.modules` instead would be wrong: because the real import is lazy, a
genuinely-installed driver (e.g. psycopg2 for the postgres integration suite)
won't be in sys.modules yet at collection time either, and the stub would
permanently shadow the real package for the rest of the session the first
time something calls connect().
"""
import importlib.util
import sys
import types
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

for moduleName in ('oracledb', 'psycopg2'):
    if moduleName not in sys.modules and importlib.util.find_spec(moduleName) is None:
        sys.modules[moduleName] = types.ModuleType(moduleName)
