"""Masking in Python and in Rust, on one core and on several, on two tables.

    python example/native-masking/demo.py [rows] [wide rows]

Needs no server and no credentials. It builds throwaway SQLite databases in
transaction/ and runs each job into a staging copy of its own:

A narrow table, `rows` customers (100,000 by default) of six masked columns,
where the database sets the pace -- masked in Python or in Rust, with reading,
masking and writing taking turns or overlapped:

    Python, in turn          BAUTA_NATIVE=0  BAUTA_PIPELINE=0
    Python, overlapped       BAUTA_NATIVE=0  BAUTA_PIPELINE=1
    Rust, in turn                            BAUTA_PIPELINE=0  BAUTA_MASKING_THREADS=1
    Rust, overlapped                         BAUTA_PIPELINE=1  BAUTA_MASKING_THREADS=1

A wide table, `wide rows` accounts (1,000,000 by default) of 25 masked
columns, where masking sets the pace -- in Rust, on one core or on every core
the machine allows:

    Rust, in turn                            BAUTA_PIPELINE=0  BAUTA_MASKING_THREADS=1
    Rust, overlapped                         BAUTA_PIPELINE=1  BAUTA_MASKING_THREADS=1
    Rust, in turn, all cores                 BAUTA_PIPELINE=0  BAUTA_MASKING_THREADS=auto
    Rust, overlapped, all cores              BAUTA_PIPELINE=1  BAUTA_MASKING_THREADS=auto

Rust is the optional bauta-rs extension. By default a job overlaps only with
Rust, where it pays, and masks on one core unless maskingThreads says more;
the variables force each combination. It prints how long each run took, and checks every copy of
a table is identical -- the extension's one promise besides speed. Each run
prints the `bauta` command it is equivalent to.

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

from bauta import Configuration, DataJobsFile, FileMemory, RunInProgressError, exclusiveRun, expandEnvironmentVariables, runDataJobs

DEFAULT_WORKING_DIRECTORY = demoDirectory / 'transaction'

DEMO_CONFIGURATION_DIRECTORY = demoDirectory / 'configuration'

DEFAULT_ROWS = 100_000
DEFAULT_WIDE_ROWS = 1_000_000

# A throwaway key for a throwaway database. A real key is random, lives in a
# secret store, and is never written into a script or a YAML file.
DEMO_MASKING_KEY = 'native-demo-key-not-for-real-use'

SCHEMA = 'CREATE TABLE customers (id INTEGER PRIMARY KEY, email TEXT, phone TEXT, national_id TEXT, token TEXT, full_name TEXT)'

# The wide table's 25 masked columns, as jobs-wide.yaml masks them.
WIDE_COLUMNS = [name for group in range(1, 5) for name in (
    'customer_ref_{}'.format(group), 'account_ref_{}'.format(group), 'email_{}'.format(group), 'api_token_{}'.format(group),
    'session_secret_{}'.format(group), 'phone_{}'.format(group))] + ['order_ref']
WIDE_SCHEMA = 'CREATE TABLE accounts (id INTEGER PRIMARY KEY, {})'.format(', '.join(column + ' TEXT' for column in WIDE_COLUMNS))

# (table, jobs file, schema)
TABLES = {'narrow': ('customers', 'jobs.yaml', SCHEMA), 'wide': ('accounts', 'jobs-wide.yaml', WIDE_SCHEMA)}


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


# (label, file name, table, masked in Rust, overlapped, masking threads)
RUNS = (
    ('Python, in turn', 'python-in-turn', 'narrow', False, False, '1'),
    ('Python, overlapped', 'python-overlapped', 'narrow', False, True, '1'),
    ('Rust, in turn', 'rust-in-turn', 'narrow', True, False, '1'),
    ('Rust, overlapped', 'rust-overlapped', 'narrow', True, True, '1'),
    ('Rust, in turn', 'wide-rust-in-turn', 'wide', True, False, '1'),
    ('Rust, overlapped', 'wide-rust-overlapped', 'wide', True, True, '1'),
    ('Rust, in turn, all cores', 'wide-rust-in-turn-cores', 'wide', True, False, 'auto'),
    ('Rust, overlapped, all cores', 'wide-rust-overlapped-cores', 'wide', True, True, 'auto'),
    )


def buildWideProduction(path: Path, rows: int) -> None:
    """References repeat, as foreign keys do; everything else is distinct."""

    connection = sqlite3.connect(path)
    connection.execute(WIDE_SCHEMA)

    def row(number: int) -> tuple:
        values: List[Any] = [number]
        for group in range(1, 5):
            values += ['C{}{:07d}'.format(group, (number * 7) % 200_000), 'A{}{:08d}'.format(group, number),
                       'person{}.{}@corp{}.example'.format(number, group, number % 97), 'tok-{}-{}-{:x}'.format(group, number, number * 2654435761),
                       'sess-{}-{:x}'.format(group, number * 40503), '+351 9{:08d}'.format((number * 7919 + group) % 10 ** 8)]
        return tuple(values + ['O{:09d}'.format(number)])

    connection.executemany('INSERT INTO accounts VALUES ({})'.format(', '.join('?' * (len(WIDE_COLUMNS) + 1))),
                           (row(number) for number in range(1, rows + 1)))
    connection.commit()
    connection.close()


def maskWith(name: str, table: str, native: bool, overlapped: bool, threads: str, workingDirectory: Path) -> float:
    """Runs the table's job into staging-<name>.db, and returns how long it took.

    Each run gets its own run state, so none is compared against another's
    recorded masking implementation.
    """

    _, jobsFileName, schema = TABLES[table]
    staging = workingDirectory / 'staging-{}.db'.format(name)
    connection = sqlite3.connect(staging)
    connection.execute(schema)
    connection.close()

    os.environ['NATIVE_DEMO_PRODUCTION_PATH'] = str(workingDirectory / 'production-{}.db'.format(table))
    os.environ['NATIVE_DEMO_STAGING_PATH'] = str(staging)
    # Read by each job's process as it starts, so they apply to this run only.
    if native:
        os.environ.pop('BAUTA_NATIVE', None)
    else:
        os.environ['BAUTA_NATIVE'] = '0'
    os.environ['BAUTA_PIPELINE'] = '1' if overlapped else '0'
    os.environ['BAUTA_MASKING_THREADS'] = threads

    databases = Configuration.validateDatabaseConfiguration(loadConfiguration('database.yaml'))
    jobsFile = Configuration.validateJobConfiguration(loadConfiguration(jobsFileName), DataJobsFile)
    memory = FileMemory(memoryFile=workingDirectory / 'memory-{}.yaml'.format(name))

    environment: Dict[str, Any] = {} if native else {'BAUTA_NATIVE': '0'}
    environment['BAUTA_PIPELINE'] = os.environ['BAUTA_PIPELINE']
    if native:
        environment['BAUTA_MASKING_THREADS'] = threads
    environment.update(NATIVE_DEMO_PRODUCTION_PATH=Path(os.environ['NATIVE_DEMO_PRODUCTION_PATH']), NATIVE_DEMO_STAGING_PATH=staging)
    if os.environ['MASKING_KEY'] == DEMO_MASKING_KEY:
        environment['MASKING_KEY'] = DEMO_MASKING_KEY  # the throwaway one; a real key is never printed
    command: List[Any] = ['run', '--config', DEMO_CONFIGURATION_DIRECTORY]
    if jobsFileName != 'jobs.yaml':
        command += ['--jobs', DEMO_CONFIGURATION_DIRECTORY / jobsFileName]
    showCommand(command + ['--memory', memory.memoryFile, '--log', workingDirectory / 'demo.log', '--quiet'], environment)

    started = time.perf_counter()
    result = runDataJobs(jobsFile=jobsFile, databaseConfiguration=databases, memory=memory,
                         logFile=workingDirectory / 'demo.log', logLevel=logging.INFO)
    seconds = time.perf_counter() - started

    if not result.succeeded:
        raise SystemExit('the {} run failed: {}'.format(name, result.failed[0].error))

    return seconds


def maskedRows(path: Path, table: str) -> List[Any]:

    connection = sqlite3.connect(path)
    try:
        return sorted(connection.execute('SELECT * FROM {}'.format(TABLES[table][0])).fetchall())
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


def report(title: str, runs: List[Any], observed: Dict[str, Any], rows: int, baseline: str, workingDirectory: Path) -> bool:
    """Prints one table's runs against its baseline, and whether every copy is identical."""

    print('\n{}\n'.format(title))
    print('{:<29} {:>8} {:>15} {:>12}'.format('', 'seconds', 'rows a second', 'vs first'))
    for label, name, *_ in runs:
        seconds = observed['seconds'][name]
        print('{:<29} {:>8.1f} {:>15,.0f} {:>11.1f}x'.format(label, seconds, rows / seconds, observed['seconds'][baseline] / seconds))

    copies = [maskedRows(workingDirectory / 'staging-{}.db'.format(name), table) for _, name, table, *_ in runs]
    identical = all(copy == copies[0] for copy in copies[1:])
    print('The {} copies are {}.'.format(len(copies), 'identical' if identical else 'NOT identical -- please report this'))

    return identical


def main(workingDirectory: Path = DEFAULT_WORKING_DIRECTORY, rows: int = DEFAULT_ROWS, wideRows: int = DEFAULT_WIDE_ROWS) -> Dict[str, Any]:
    """Runs the comparison, and returns what it observed so a test can check it.

    One at a time per working directory: each run starts by emptying it, so a
    second would delete the first's copies before they were compared.
    """

    workingDirectory.parent.mkdir(parents=True, exist_ok=True)
    try:
        with exclusiveRun(workingDirectory.with_name(workingDirectory.name + '.lock')):
            return _compare(workingDirectory, rows, wideRows)
    except RunInProgressError:
        raise SystemExit('Another run of this demo is using {} -- wait for it to finish.'.format(workingDirectory))


def _compare(workingDirectory: Path, rows: int, wideRows: int) -> Dict[str, Any]:

    shutil.rmtree(workingDirectory, ignore_errors=True)
    workingDirectory.mkdir(parents=True, exist_ok=True)

    version = nativeVersion()
    runs = [run for run in RUNS if version is not None or not run[3]]

    buildProduction(workingDirectory / 'production-narrow.db', rows)
    if version is not None:
        buildWideProduction(workingDirectory / 'production-wide.db', wideRows)

    previous = {name: os.environ.get(name) for name in ('NATIVE_DEMO_PRODUCTION_PATH', 'NATIVE_DEMO_STAGING_PATH', 'BAUTA_NATIVE',
                                                        'BAUTA_PIPELINE', 'BAUTA_MASKING_THREADS')}
    os.environ.setdefault('MASKING_KEY', DEMO_MASKING_KEY)
    observed: Dict[str, Any] = {'rows': rows, 'wideRows': wideRows, 'nativeVersion': version, 'seconds': {}, 'identical': None}

    print('Narrow: {:,} customers, id and national_id with key, email, phone with digits, token with hash, full_name with fakeName.'.format(rows))
    if version is not None:
        print('Wide: {:,} accounts, 25 masked columns: references with key, emails, tokens with hash, phones with digits.'.format(wideRows))

    try:
        for label, name, table, native, overlapped, threads in runs:
            print('\n{}: {}'.format('Narrow' if table == 'narrow' else 'Wide', label))
            observed['seconds'][name] = seconds = maskWith(name, table, native, overlapped, threads, workingDirectory)
            print('  {:.1f}s'.format(seconds))

        narrow = [run for run in runs if run[2] == 'narrow']
        wide = [run for run in runs if run[2] == 'wide']
        identical = report('Narrow table: {:,} rows, 6 masked columns'.format(rows), narrow, observed, rows, 'python-in-turn', workingDirectory)
        if wide:
            identical = report('Wide table: {:,} rows, 25 masked columns'.format(wideRows), wide, observed, wideRows, 'wide-rust-in-turn',
                               workingDirectory) and identical
        observed['identical'] = identical

        if version is None:
            print('\nRust is the optional bauta-rs extension, which isn\'t installed. With Rust 1.83 or newer, install it from\n'
                  'the repository root, and run this again for the Rust runs and the wide table:\n\n    pip install ./mask-rs/py')
        else:
            from bauta.masking import availableCores
            print('\nRust is the bauta-rs extension, version {}. "All cores" is {} here.'.format(version, availableCores()))
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    print('\nEach job\'s log is in {}'.format(workingDirectory / 'demo.log'))

    return observed


if __name__ == '__main__':
    main(rows=int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_ROWS,
         wideRows=int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_WIDE_ROWS)
