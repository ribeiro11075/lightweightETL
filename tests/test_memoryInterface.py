from library.memoryInterface import FileMemory


def test_missing_memory_file_reads_as_empty(tmp_path):
    """Regression test: a fresh checkout ships no memory file at all -- this used
    to require the file to already exist and crash with FileNotFoundError.
    """
    memory = FileMemory(memoryDirectory=tmp_path / 'does_not_exist.yaml')

    assert memory.read() == {}


def test_record_run_persists_and_reloads(tmp_path):
    memoryPath = tmp_path / 'memory.yaml'

    first = FileMemory(memoryDirectory=memoryPath)
    first.recordRun(job='job1')

    second = FileMemory(memoryDirectory=memoryPath)

    assert 'job1' in second.read()


def test_record_run_only_touches_its_own_job(tmp_path):
    memory = FileMemory(memoryDirectory=tmp_path / 'memory.yaml')
    memory.recordRun(job='job1')
    firstTimestamp = memory.read()['job1']

    memory.recordRun(job='job2')

    updated = memory.read()
    assert updated['job1'] == firstTimestamp
    assert 'job2' in updated


def test_empty_memory_file_reads_as_empty_dict(tmp_path):
    memoryPath = tmp_path / 'memory.yaml'
    memoryPath.write_text('')

    memory = FileMemory(memoryDirectory=memoryPath)

    assert memory.read() == {}


def test_record_run_does_not_clobber_a_concurrent_workers_update(tmp_path):
    """Regression test for a lost-update race: each worker process holds its own
    FileMemory instance. Here, both are constructed against the same (still-empty)
    file before either has written -- simulating two worker processes starting up
    around the same time. Without re-reading the file inside recordRun, the second
    writer's stale empty snapshot would silently overwrite the first writer's entry.
    """
    memoryPath = tmp_path / 'memory.yaml'

    workerA = FileMemory(memoryDirectory=memoryPath)
    workerB = FileMemory(memoryDirectory=memoryPath)

    workerB.recordRun(job='jobB')
    workerA.recordRun(job='jobA')

    finalState = FileMemory(memoryDirectory=memoryPath)
    assert {'jobA', 'jobB'} <= finalState.read().keys()
