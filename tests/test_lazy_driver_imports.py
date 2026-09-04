import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_library_is_importable_without_every_database_driver_installed():
    """psycopg2/cx_Oracle genuinely aren't installed in this environment (only
    stubbed by conftest.py for this test *process*) -- run a fresh, unstubbed
    interpreter to prove `import library` doesn't require every driver up front.
    """
    result = subprocess.run([sys.executable, '-c', 'import library'], cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=30)

    assert result.returncode == 0, result.stderr


def test_connecting_with_a_missing_driver_fails_at_connect_not_at_import():
    script = (
        'import library\n'
        'from library.configurationInterface import DatabaseConnectionConfig, DatabaseType\n'
        'settings = DatabaseConnectionConfig(type=DatabaseType.POSTGRESQL, user="u", password="p", database="d", host="h")\n'
        'try:\n'
        '    library.Database(connectionSettings=settings)\n'
        'except ModuleNotFoundError:\n'
        '    print("OK")\n'
        )
    result = subprocess.run([sys.executable, '-c', script], cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=30)

    assert result.returncode == 0, result.stderr
    assert 'OK' in result.stdout
