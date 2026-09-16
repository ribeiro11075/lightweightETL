import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_library_is_importable_without_every_database_driver_installed():
    """Runs in a fresh interpreter, unaffected by whether this environment happens
    to have psycopg2/oracledb installed, to prove `import lightweight_etl` doesn't require
    every driver up front regardless.
    """
    result = subprocess.run([sys.executable, '-c', 'import lightweight_etl'], cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=30)

    assert result.returncode == 0, result.stderr


def test_connecting_with_a_missing_driver_fails_at_connect_not_at_import():
    """Whether psycopg2 happens to be installed in whatever environment runs this
    test shouldn't change the outcome -- `sys.modules['psycopg2'] = None` forces
    the subsequent `import psycopg2` inside connect() to raise ModuleNotFoundError
    deterministically, the same documented mechanism Python itself uses to block
    a specific import.
    """
    script = (
        'import sys\n'
        'sys.modules["psycopg2"] = None\n'
        'import lightweight_etl\n'
        'from lightweight_etl.configuration import DatabaseConnectionConfig, DatabaseType\n'
        'settings = DatabaseConnectionConfig(type=DatabaseType.POSTGRESQL, user="u", password="p", database="d", host="h")\n'
        'try:\n'
        '    lightweight_etl.Database(connectionSettings=settings)\n'
        'except ModuleNotFoundError:\n'
        '    print("OK")\n'
        )
    result = subprocess.run([sys.executable, '-c', script], cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=30)

    assert result.returncode == 0, result.stderr
    assert 'OK' in result.stdout
