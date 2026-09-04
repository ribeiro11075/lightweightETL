from library.memoryInterface import Memory


def test_missing_memory_file_starts_empty(tmp_path):
    """Regression test: a fresh checkout ships no memory file at all -- Memory used
    to require the file to already exist and crash with FileNotFoundError.
    """
    memory = Memory(memoryDirectory=tmp_path / 'does_not_exist.yaml')

    assert memory.memory == {}


def test_record_run_persists_and_reloads(tmp_path):
    memoryPath = tmp_path / 'memory.yaml'

    first = Memory(memoryDirectory=memoryPath)
    first.recordRun(job='job1')

    second = Memory(memoryDirectory=memoryPath)

    assert 'job1' in second.memory
    assert second.memory['job1'] == first.memory['job1']


def test_record_run_only_touches_its_own_job(tmp_path):
    memory = Memory(memoryDirectory=tmp_path / 'memory.yaml')
    memory.recordRun(job='job1')
    firstTimestamp = memory.memory['job1']

    memory.recordRun(job='job2')

    assert memory.memory['job1'] == firstTimestamp
    assert 'job2' in memory.memory


def test_empty_memory_file_loads_as_empty_dict(tmp_path):
    memoryPath = tmp_path / 'memory.yaml'
    memoryPath.write_text('')

    memory = Memory(memoryDirectory=memoryPath)

    assert memory.memory == {}
