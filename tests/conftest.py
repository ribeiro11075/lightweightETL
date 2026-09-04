"""Puts the repo root on sys.path, and stubs cx_Oracle/psycopg2 in sys.modules
so any test that does exercise a dialect's connect() doesn't need those native
client libraries installed (library itself only imports them lazily, inside
connect(), so this isn't required just to import library -- see
test_lazy_driver_imports.py).
"""
import sys
import types
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

for moduleName in ('cx_Oracle', 'psycopg2'):
    if moduleName not in sys.modules:
        sys.modules[moduleName] = types.ModuleType(moduleName)
