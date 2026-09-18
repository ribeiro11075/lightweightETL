"""The same masked job, four ways: masked in Python or in Rust, with reading,
masking and writing either taking turns or overlapped.

    python example/native-masking/demo.py [rows]

Needs no server and no credentials. It builds a production SQLite database of
`rows` customers (100,000 by default) in transaction/, then runs one masked job
four times, each into a staging copy of its own:

    Python, in turn       BAUTA_NATIVE=0  BAUTA_PIPELINE=0
    Python, overlapped    BAUTA_NATIVE=0  BAUTA_PIPELINE=1
    Rust, in turn                              BAUTA_PIPELINE=0
    Rust, overlapped                           BAUTA_PIPELINE=1

Rust is the optional bauta-rs extension. By default a job overlaps only
with Rust, where it pays; the variables force each combination. It prints how
long each run took, and checks every copy is identical -- the extension's one
promise besides speed. Each run prints the `bauta` command it is
equivalent to.

Without the extension it runs the Python half, and says how to install it:

    pip install ./mask-rs/py
"""
from __future__ import annotations

import importlib.util
import logging
import os
import shlex
import shutil
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

demoDirectory = Path(__file__).resolve().parent
sys.path.append(str(demoDirectory.parents[1]))

from bauta import Configuration, DataJobsFile, FileMemory, expandEnvironmentVariables, runDataJobs

DEFAULT_WORKING_DIRECTORY = demoDirectory / 'transaction'

DEMO_CONFIGURATION_DIRECTORY = demoDirectory / 'configuration'

DEFAULT_ROWS = 100_000

# A throwaway key for a throwaway database. A real key is random, lives in a
# secret store, and is never written into a script or a YAML file.
DEMO_MASKING_KEY = 'native-demo-key-not-for-real-use'

SCHEMA = 'CREATE TABLE customers (id INTEGER PRIMARY KEY, email TEXT, phone TEXT, national_id TEXT, token TEXT, full_name TEXT)'


def loadConfiguration(name: str) -> Any:
    """Loads a demo YAML file the same way the CLI does, ${NAME} expansion included."""

    with open(DEMO_CONFIGURATION_DIRECTORY / name) as file:
        return expandEnvironmentVariables(yaml.safe_load(file))


def showCommand(arguments: List[Any], environment: Optional[Dict[str, Any]] = None) -> None:
    """Prints the `bauta` command that does what the next step does, with
    paths relative to where this was run from, ready to paste into a shell.
    """

    def shown(value: Any) -> str:
        text = os.path.relpath(value) if isinstance(value, Path) else str(value)
        if "'" in text and not any(character in text for character in '"$`\\'):
            return '"{}"'.format(text)
        return shlex.quote(text)

    # A variable, the command, or an option with its value: kept whole on a line.
    pieces = ['{}={}'.format(name, shown(value)) for name, value in (environment or {}).items()]
    pieces.append('bauta ' + shown(arguments[0]))
    rest = [shown(argument) for argument in arguments[1:]]
    while rest:
        takesValue = len(rest) > 1 and not rest[1].startswith('--')
        pieces.append(' '.join(rest[:2] if takesValue else rest[:1]))
        rest = rest[2:] if takesValue else rest[1:]

    lines = ['$']
    for piece in pieces:
        if len(lines[-1]) + len(piece) > 100 and lines[-1] != '$':
            lines.append('   ')
        lines[-1] += ' ' + piece
    print('  ' + ' \\\n  '.join(lines))


def buildProduction(path: Path, rows: int) -> None:

    connection = sqlite3.connect(path)
    connection.execute(SCHEMA)
    connection.executemany('INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?)', (
        (number, 'customer{}@corp.example'.format(number), '+1 555 {:03d} {:04d}'.format(number % 1000, number % 10000),
         '{:09d}'.format(number * 7919 % 10 ** 9), '{:012x}'.format(number * 2654435761 % 16 ** 12), 'Customer {}'.format(number))
        for number in range(1, rows + 1)))
    connection.commit()
    connection.close()


# (label, file name, masked in Rust, overlapped)
RUNS = (
    ('Python, in turn', 'python-in-turn', False, False),
    ('Python, overlapped', 'python-overlapped', False, True),
    ('Rust, in turn', 'rust-in-turn', True, False),
    ('Rust, overlapped', 'rust-overlapped', True, True),
    )


def maskWith(name: str, native: bool, overlapped: bool, workingDirectory: Path) -> float:
    """Runs the job into staging-<name>.db, and returns how long it took.

    Each run gets its own run state, so none is compared against another's
    recorded masking implementation.
    """

    staging = workingDirectory / 'staging-{}.db'.format(name)
    connection = sqlite3.connect(staging)
    connection.execute(SCHEMA)
    connection.close()

    os.environ['NATIVE_DEMO_STAGING_PATH'] = str(staging)
    # Read by each job's process as it starts, so they apply to this run only.
    if native:
        os.environ.pop('BAUTA_NATIVE', None)
    else:
        os.environ['BAUTA_NATIVE'] = '0'
    os.environ['BAUTA_PIPELINE'] = '1' if overlapped else '0'

    databases = Configuration.validateDatabaseConfiguration(loadConfiguration('database.yaml'))
    jobsFile = Configuration.validateJobConfiguration(loadConfiguration('jobs.yaml'), DataJobsFile)
    memory = FileMemory(memoryFile=workingDirectory / 'memory-{}.yaml'.format(name))

    environment: Dict[str, Any] = {} if native else {'BAUTA_NATIVE': '0'}
    environment['BAUTA_PIPELINE'] = os.environ['BAUTA_PIPELINE']
    environment.update(NATIVE_DEMO_PRODUCTION_PATH=workingDirectory / 'production.db', NATIVE_DEMO_STAGING_PATH=staging)
    if os.environ['MASKING_KEY'] == DEMO_MASKING_KEY:
        environment['MASKING_KEY'] = DEMO_MASKING_KEY  # the throwaway one; a real key is never printed
    showCommand(['run', '--config', DEMO_CONFIGURATION_DIRECTORY, '--memory', memory.memoryFile, '--log', workingDirectory / 'demo.log',
                 '--quiet'], environment)

    started = time.perf_counter()
    result = runDataJobs(jobsFile=jobsFile, databaseConfiguration=databases, memory=memory,
                         logFile=workingDirectory / 'demo.log', logLevel=logging.INFO)
    seconds = time.perf_counter() - started

    if not result.succeeded:
        raise SystemExit('the {} run failed: {}'.format(name, result.failed[0].error))

    return seconds


def maskedRows(path: Path) -> List[Any]:

    connection = sqlite3.connect(path)
    try:
        return sorted(connection.execute('SELECT * FROM customers').fetchall())
    finally:
        connection.close()


def nativeVersion() -> Optional[str]:
    """The installed extension's version, or None. Checked without importing
    it into this process, whose own masking isn't what's being measured.
    """

    if importlib.util.find_spec('bauta_rs') is None:
        return None

    from importlib.metadata import PackageNotFoundError, version

    try:
        return version('bauta-rs')
    except PackageNotFoundError:
        return 'installed'


def main(workingDirectory: Path = DEFAULT_WORKING_DIRECTORY, rows: int = DEFAULT_ROWS) -> Dict[str, Any]:
    """Runs the comparison, and returns what it observed so a test can check it."""

    shutil.rmtree(workingDirectory, ignore_errors=True)
    workingDirectory.mkdir(parents=True, exist_ok=True)

    production = workingDirectory / 'production.db'
    buildProduction(production, rows)

    previous = {name: os.environ.get(name) for name in ('NATIVE_DEMO_PRODUCTION_PATH', 'NATIVE_DEMO_STAGING_PATH', 'BAUTA_NATIVE',
                                                        'BAUTA_PIPELINE')}
    os.environ['NATIVE_DEMO_PRODUCTION_PATH'] = str(production)
    os.environ.setdefault('MASKING_KEY', DEMO_MASKING_KEY)
    version = nativeVersion()
    observed: Dict[str, Any] = {'rows': rows, 'nativeVersion': version, 'seconds': {}, 'identical': None}
    runs = [run for run in RUNS if version is not None or not run[2]]

    print('Masking {:,} customers: id and national_id with key, email, phone with digits, token with hash, full_name with fakeName.'.format(rows))

    try:
        for label, name, native, overlapped in runs:
            print('\n{}'.format(label))
            observed['seconds'][name] = seconds = maskWith(name, native, overlapped, workingDirectory)
            print('  {:.1f}s'.format(seconds))

        baseline = observed['seconds']['python-in-turn']
        print('\n{:<22} {:>8} {:>15} {:>18}'.format('', 'seconds', 'rows a second', 'vs Python in turn'))
        for label, name, _, _ in runs:
            seconds = observed['seconds'][name]
            print('{:<22} {:>8.1f} {:>15,.0f} {:>17.1f}x'.format(label, seconds, rows / seconds, baseline / seconds))

        copies = [maskedRows(workingDirectory / 'staging-{}.db'.format(name)) for _, name, _, _ in runs]
        observed['identical'] = all(copy == copies[0] for copy in copies[1:])
        print('\nThe {} staging copies are {}.'.format(len(copies), 'identical' if observed['identical'] else 'NOT identical -- please report this'))

        if version is None:
            print('\nRust is the optional bauta-rs extension, which isn\'t installed. With Rust 1.83 or newer, install it from\n'
                  'the repository root, and run this again to compare all four:\n\n    pip install ./mask-rs/py')
        else:
            print('Rust is the bauta-rs extension, version {}.'.format(version))
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    print('\nEach job\'s log is in {}'.format(workingDirectory / 'demo.log'))

    return observed


if __name__ == '__main__':
    main(rows=int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_ROWS)
