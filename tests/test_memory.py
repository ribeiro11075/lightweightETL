import datetime
import decimal

import pytest

from understudy_data.memory import FileMemory


def test_missing_memory_file_reads_as_empty(tmp_path):
    """Regression test: a fresh checkout ships no memory file at all -- this used
    to require the file to already exist and crash with FileNotFoundError.
    """
    memory = FileMemory(memoryFile=tmp_path / 'does_not_exist.yaml')

    assert memory.read() == {}


def test_record_run_persists_and_reloads(tmp_path):
    memoryPath = tmp_path / 'memory.yaml'

    first = FileMemory(memoryFile=memoryPath)
    first.recordRun(job='job1')

    second = FileMemory(memoryFile=memoryPath)

    assert 'job1' in second.read()


def test_record_run_only_touches_its_own_job(tmp_path):
    memory = FileMemory(memoryFile=tmp_path / 'memory.yaml')
    memory.recordRun(job='job1')
    firstTimestamp = memory.read()['job1']

    memory.recordRun(job='job2')

    updated = memory.read()
    assert updated['job1'] == firstTimestamp
    assert 'job2' in updated


def test_empty_memory_file_reads_as_empty_dict(tmp_path):
    memoryPath = tmp_path / 'memory.yaml'
    memoryPath.write_text('')

    memory = FileMemory(memoryFile=memoryPath)

    assert memory.read() == {}


def test_record_run_does_not_clobber_a_concurrent_workers_update(tmp_path):
    """Regression test for a lost-update race: each worker process holds its own
    FileMemory instance. Here, both are constructed against the same (still-empty)
    file before either has written -- simulating two worker processes starting up
    around the same time. Without re-reading the file inside recordRun, the second
    writer's stale empty snapshot would silently overwrite the first writer's entry.
    """
    memoryPath = tmp_path / 'memory.yaml'

    workerA = FileMemory(memoryFile=memoryPath)
    workerB = FileMemory(memoryFile=memoryPath)

    workerB.recordRun(job='jobB')
    workerA.recordRun(job='jobA')

    finalState = FileMemory(memoryFile=memoryPath)
    assert {'jobA', 'jobB'} <= finalState.read().keys()


def test_watermarks_and_run_times_are_stored_independently(tmp_path):
    """They're written by two separate locked read-modify-writes against one
    file -- neither may clobber the other's section.
    """
    memory = FileMemory(memoryFile=tmp_path / 'memory.yaml')

    memory.recordWatermark(job='job1', value='2026-09-15 10:00:00')
    memory.recordRun(job='job1')
    memory.recordWatermark(job='job2', value=4711)

    assert set(memory.read()) == {'job1'}
    assert memory.readWatermarks() == {'job1': '2026-09-15 10:00:00', 'job2': 4711}


def test_the_first_write_creates_the_memory_files_directory(tmp_path):
    memory = FileMemory(memoryFile=tmp_path / 'transaction' / 'memory.yaml')

    memory.recordRun('loadOrders')

    assert 'loadOrders' in memory.read()


def test_a_missing_memory_file_reads_as_no_watermarks(tmp_path):
    assert FileMemory(memoryFile=tmp_path / 'nope.yaml').readWatermarks() == {}


@pytest.mark.parametrize('value', [
    4711,
    3.5,
    'abc',
    datetime.datetime(2026, 9, 15, 10, 30, 0),
    datetime.date(2026, 9, 15),
    ])
def test_watermark_values_round_trip_through_yaml(tmp_path, value):
    """Whatever goes in has to come back as the same type -- it gets bound back
    into a predicate compared against the source column it came from.
    """
    memory = FileMemory(memoryFile=tmp_path / 'memory.yaml')

    memory.recordWatermark(job='job1', value=value)

    assert memory.readWatermarks()['job1'] == value


def test_a_decimal_watermark_is_stored_as_a_number_not_a_python_object(tmp_path):
    """Oracle returns every NUMBER as a Decimal, which PyYAML can only write as a
    python/object tag that FullLoader then refuses to load -- so an Oracle id
    watermark would fail on the way back in.
    """
    memory = FileMemory(memoryFile=tmp_path / 'memory.yaml')

    memory.recordWatermark(job='job1', value=decimal.Decimal('4711'))
    memory.recordWatermark(job='job2', value=decimal.Decimal('3.5'))

    assert 'python/object' not in (tmp_path / 'memory.yaml').read_text()
    assert memory.readWatermarks() == {'job1': 4711, 'job2': 3.5}


def test_an_integral_decimal_keeps_full_precision_beyond_floats_range(tmp_path):
    """int, not float: an id past 2**53 would lose its last digits as a float."""
    memory = FileMemory(memoryFile=tmp_path / 'memory.yaml')
    bigId = 9007199254740993

    memory.recordWatermark(job='job1', value=decimal.Decimal(bigId))

    assert memory.readWatermarks()['job1'] == bigId


def test_a_legacy_flat_memory_file_is_read_as_last_run_times(tmp_path):
    """A memory file written before watermarks existed is a bare job -> timestamp
    mapping. Reading it as anything else would drop every refresh window on
    upgrade and fire every job at once.
    """
    memoryPath = tmp_path / 'memory.yaml'
    memoryPath.write_text('job1: 1726400000.0\njob2: 1726400001.0\n')
    memory = FileMemory(memoryFile=memoryPath)

    assert memory.read() == {'job1': 1726400000.0, 'job2': 1726400001.0}
    assert memory.readWatermarks() == {}

    memory.recordWatermark(job='job1', value=7)

    assert memory.read() == {'job1': 1726400000.0, 'job2': 1726400001.0}
    assert memory.readWatermarks() == {'job1': 7}


def test_a_write_replaces_the_file_rather_than_rewriting_it_in_place(tmp_path, monkeypatch):
    """A process killed mid-write used to leave a truncated file that every
    later run failed to parse. A failed write now leaves the old file intact.
    """
    import yaml

    memoryFile = tmp_path / 'memory.yaml'
    memory = FileMemory(memoryFile=memoryFile)
    memory.recordRun('first')
    before = memoryFile.read_text()

    def dieMidWrite(document, stream):
        stream.write('lastRun:\n  fir')
        raise KeyboardInterrupt

    monkeypatch.setattr(yaml, 'safe_dump', dieMidWrite)

    with pytest.raises(KeyboardInterrupt):
        memory.recordRun('second')

    assert memoryFile.read_text() == before


def test_the_run_lock_is_exclusive_and_released_afterwards(tmp_path):
    from understudy_data.memory import RunInProgressError, exclusiveRun

    lockFile = tmp_path / 'memory.yaml.run.lock'

    with exclusiveRun(lockFile):
        with pytest.raises(RunInProgressError):
            with exclusiveRun(lockFile):
                pass

    with exclusiveRun(lockFile):
        pass


def test_the_database_memory_schema_uses_a_portable_float_type():
    """DOUBLE alone is MySQL's spelling; PostgreSQL, Oracle and SQL Server reject it."""
    from understudy_data.memory import DATABASE_MEMORY_SCHEMA

    assert 'DOUBLE PRECISION' in DATABASE_MEMORY_SCHEMA


def test_file_memory_records_and_forgets_key_fingerprints(tmp_path):
    memory = FileMemory(memoryFile=tmp_path / 'memory.yaml')
    memory.recordRun('maskCustomers')

    memory.recordKeyFingerprint('maskCustomers', 'abc123')
    assert memory.readKeyFingerprints() == {'maskCustomers': 'abc123'}
    assert memory.read().keys() == {'maskCustomers'}

    memory.recordKeyFingerprint('maskCustomers', None)
    assert memory.readKeyFingerprints() == {}


def test_database_memory_keeps_key_fingerprints_out_of_watermarks_and_runs(tmp_path):
    import sqlite3

    from understudy_data.configuration import DatabaseConnectionConfig
    from understudy_data.memory import DATABASE_MEMORY_SCHEMA, DatabaseMemory

    path = tmp_path / 'memory.db'
    connection = sqlite3.connect(path)
    connection.execute(DATABASE_MEMORY_SCHEMA)
    connection.close()
    memory = DatabaseMemory(DatabaseConnectionConfig(type='sqlite', database=str(path)))

    memory.recordWatermark('maskCustomers', 7)
    memory.recordKeyFingerprint('maskCustomers', 'abc123')

    assert memory.readKeyFingerprints() == {'maskCustomers': 'abc123'}
    assert memory.readWatermarks() == {'maskCustomers': 7}
    assert memory.read() == {}

    memory.recordKeyFingerprint('maskCustomers', None)
    assert memory.readKeyFingerprints() == {}


def test_a_backend_without_fingerprint_support_reports_none():
    from understudy_data.memory import MemoryBackend

    class _Minimal(MemoryBackend):
        def read(self):
            return {}

        def recordRun(self, job):
            return None

    memory = _Minimal()
    memory.recordKeyFingerprint('job', 'abc')

    assert memory.readKeyFingerprints() == {}


@pytest.mark.parametrize('value', [b'\x00\x00\x07\xd1', decimal.Decimal('12.50'), datetime.time(10, 30, 5), datetime.datetime(2026, 9, 17, 8, 0),
                                   datetime.date(2026, 9, 17), 4711, 3.5, 'abc'])
def test_database_memory_gives_a_watermark_back_as_the_type_it_was(tmp_path, value):
    """The next run binds it against the source column, which a string doesn't
    compare with the way a SQL Server rowversion's bytes do.
    """
    import sqlite3

    from understudy_data.configuration import DatabaseConnectionConfig
    from understudy_data.memory import DATABASE_MEMORY_SCHEMA, DatabaseMemory

    path = tmp_path / 'memory.db'
    connection = sqlite3.connect(path)
    connection.execute(DATABASE_MEMORY_SCHEMA)
    connection.close()
    memory = DatabaseMemory(DatabaseConnectionConfig(type='sqlite', database=str(path)))

    memory.recordWatermark('job1', value)
    read = memory.readWatermarks()['job1']

    assert type(read) is type(value) and read == value
