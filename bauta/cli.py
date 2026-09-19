"""The `bauta` command.

The only place in the package that reads files and the environment; the
modules under it take already-loaded, validated objects.

Exit codes, which are the interface for anything that schedules work:

    0    every active job completed
    1    at least one job failed or was skipped, or the command itself failed
    2    invalid configuration, or a usage error
    130  interrupted
"""
from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import yaml

from .configuration import (Configuration, ConfigurationError, DatabaseConnectionConfig, DataJobConfig, DataJobsFile, StorageLocation, TableLocation,
                            expandEnvironmentVariables)
from .database import DIALECTS, Database
from .databaseDialects import ForeignKey, quoteIdentifier
from .dependencyGraph import DependencyGraph
from .log import Log
from .masking import MaskingError, keyFingerprint, sealManifest, verifyManifest
from .memory import DatabaseMemory, FileMemory, MemoryBackend, RunInProgressError, exclusiveRun
from .runner import RunResult, runDataJobs
from .scrubbing import describeError

EXIT_SUCCESS = 0
EXIT_JOBS_DID_NOT_SUCCEED = 1
EXIT_BAD_CONFIGURATION = 2
EXIT_INTERRUPTED = 130

CONFIG_DIRECTORY_VARIABLE = 'BAUTA_CONFIG'
MANIFEST_KEY_VARIABLE = 'BAUTA_MANIFEST_KEY'
NOTIFY_URL_VARIABLE = 'BAUTA_NOTIFY_URL'


class UsageError(Exception):
    """A problem with how the command was invoked, rather than with a job."""


def _loadYaml(path: Path) -> Any:
    """Load a YAML file, expanding ${NAME} from the environment."""

    try:
        with open(path) as file:
            return expandEnvironmentVariables(yaml.safe_load(file))
    except FileNotFoundError as error:
        raise UsageError('no such file: {}'.format(path)) from error
    except yaml.YAMLError as error:
        raise UsageError('{} is not valid YAML: {}'.format(path, error)) from error


def _configDirectory(arguments: argparse.Namespace) -> Path:
    """--config, else $BAUTA_CONFIG, else ./configuration -- so the
    common case is a bare `bauta run`.
    """

    return Path(arguments.config or os.environ.get(CONFIG_DIRECTORY_VARIABLE) or 'configuration')


def _resolveConfigurationPaths(arguments: argparse.Namespace) -> Tuple[Path, Path]:
    """Explicit --jobs/--databases win; otherwise both come from the config directory."""

    configDirectory = _configDirectory(arguments)
    jobsPath = Path(arguments.jobs) if arguments.jobs else configDirectory / 'jobs.yaml'
    databasesPath = Path(arguments.databases) if arguments.databases else configDirectory / 'database.yaml'

    return jobsPath, databasesPath


Location = Union[Path, TableLocation]

DEFAULT_TABLES = {'memory': 'bauta_memory', 'history': 'bauta_history', 'manifest': 'bauta_manifest'}


def _resolveLocation(arguments: argparse.Namespace, setting: str, configured: Optional[StorageLocation] = None) -> Optional[Location]:
    """Where run state, history or the manifest goes: the setting's file flag
    (--memory, say), else its database flag (--memory-database), else what the
    jobs file says, else None. A file the jobs file names is relative to it,
    not the working directory, so cron, a shell and CI find the same one
    wherever they start. --<setting>-table renames the table either way.
    """

    fileFlag = getattr(arguments, setting, None)
    databaseFlag = getattr(arguments, setting + '_database', None)
    table = getattr(arguments, setting + '_table', None)

    if fileFlag:
        return Path(fileFlag)
    if databaseFlag:
        return TableLocation(database=databaseFlag, table=table or DEFAULT_TABLES[setting])
    if isinstance(configured, TableLocation):
        return TableLocation(database=configured.database, table=table or configured.table or DEFAULT_TABLES[setting])
    if configured:
        jobsPath, _ = _resolveConfigurationPaths(arguments)
        return Path(os.path.normpath(jobsPath.parent / configured))

    return None


def _describeLocation(location: Location) -> str:

    return str(location) if isinstance(location, Path) else 'table {} in {}'.format(location.table, location.database)


def _settingsFor(location: TableLocation, databaseConfiguration: Dict[str, DatabaseConnectionConfig]) -> DatabaseConnectionConfig:

    _requireAlias(databaseConfiguration, location.database)

    return databaseConfiguration[location.database]


def _memoryLocation(arguments: argparse.Namespace, jobsFile: DataJobsFile) -> Location:
    """Run state has a default where history and the manifest don't: memory.yaml beside the jobs file."""

    jobsPath, _ = _resolveConfigurationPaths(arguments)

    return _resolveLocation(arguments, 'memory', jobsFile.memory) or Path(os.path.normpath(jobsPath.parent / 'memory.yaml'))


def _memoryBackend(arguments: argparse.Namespace, jobsFile: DataJobsFile,
                   databaseConfiguration: Dict[str, DatabaseConnectionConfig]) -> Tuple[MemoryBackend, Path]:
    """The run memory to use, and the file a run holds as its lock, beside it.

    Run state in a table still needs a file for the lock, so it goes where a
    memory file would, and keeps overlapping runs apart on one machine only;
    across machines, let the scheduler do it (a CronJob's concurrencyPolicy:
    Forbid).
    """

    location = _memoryLocation(arguments, jobsFile)

    if isinstance(location, TableLocation):
        jobsPath, _ = _resolveConfigurationPaths(arguments)
        wouldBe = jobsFile.memory if isinstance(jobsFile.memory, str) else 'memory.yaml'
        memory = DatabaseMemory(connectionSettings=_settingsFor(location, databaseConfiguration), table=location.table or DEFAULT_TABLES['memory'])
        return memory, Path(os.path.normpath(jobsPath.parent / wouldBe)).with_name('memory.run.lock')

    return FileMemory(memoryFile=location), location.with_name(location.name + '.run.lock')


def _history(location: Location, databaseConfiguration: Dict[str, DatabaseConnectionConfig]) -> Any:

    from .reporting import DatabaseHistory, FileHistory

    if isinstance(location, Path):
        return FileHistory(location)

    return DatabaseHistory(connectionSettings=_settingsFor(location, databaseConfiguration), table=location.table or DEFAULT_TABLES['history'])


def _cycleReporter(arguments: argparse.Namespace, jobsFile: DataJobsFile, databaseConfiguration: Dict[str, DatabaseConnectionConfig],
                   log: Log) -> Optional[Callable[[RunResult], None]]:
    """What `run` does as each cycle ends: history and notifications, as the
    flags and the jobs file ask. None if they ask for nothing.
    """

    from .reporting import RunHistory, newRunId, notify

    historyLocation = _resolveLocation(arguments, 'history', jobsFile.history)
    history: Optional[RunHistory] = _history(historyLocation, databaseConfiguration) if historyLocation else None

    notifyUrl = arguments.notify_url or os.environ.get(NOTIFY_URL_VARIABLE)

    if not (history or notifyUrl):
        return None

    def attempt(what: str, step: Callable[[], Any]) -> None:
        # Each is independent: one failing mustn't cost the others.
        try:
            step()
        except Exception as error:
            log.logging.error('Could not {}: {}'.format(what, describeError(error)))

    def report(result: RunResult) -> None:
        if history is not None:
            attempt('record the run history', lambda: history.append(result, newRunId()))
        if notifyUrl:
            attempt('send the notification', lambda: notify(notifyUrl, result, always=arguments.notify_on == 'always'))

    return report


def _configureLogging(arguments: argparse.Namespace) -> Log:
    """stderr unless --quiet, where a container collects it; --log adds a file."""

    level = getattr(logging, arguments.log_level.upper())
    log = Log(logFile=arguments.log, level=level, logFormat=arguments.log_format)

    if not arguments.quiet:
        log.addStreamHandler(stream=sys.stderr, level=level)

    return log


def _loadDatabases(arguments: argparse.Namespace) -> Dict[str, DatabaseConnectionConfig]:

    _, databasesPath = _resolveConfigurationPaths(arguments)

    return Configuration.validateDatabaseConfiguration(_loadYaml(databasesPath))


def _loadDataJobs(arguments: argparse.Namespace) -> Tuple[DataJobsFile, Dict[str, DatabaseConnectionConfig]]:

    jobsPath, databasesPath = _resolveConfigurationPaths(arguments)
    databaseConfiguration = Configuration.validateDatabaseConfiguration(_loadYaml(databasesPath))
    jobsFile = Configuration.validateJobConfiguration(_loadYaml(jobsPath), DataJobsFile)
    Configuration.validateJobGraph(jobsFile.jobs, databaseAliases=set(databaseConfiguration))

    unknown = ['{}: database "{}" is not a known database alias'.format(setting, location.database)
               for setting, location in jobsFile.tableLocations().items() if location.database not in databaseConfiguration]
    if unknown:
        raise ConfigurationError('Invalid configuration in {}:\n'.format(jobsPath) + '\n'.join(unknown))

    return jobsFile, databaseConfiguration


def _selectJobs(jobs: Dict[str, DataJobConfig], requested: Optional[List[str]], log: Log) -> Dict[str, DataJobConfig]:
    """Narrow a job map to --job selections, naming each predecessor left out."""

    if not requested:
        return jobs

    unknown = [job for job in requested if job not in jobs]
    if unknown:
        raise UsageError('no such job(s): {}. Known jobs: {}'.format(', '.join(unknown), ', '.join(sorted(jobs))))

    selected = {name: jobs[name] for name in requested}

    for name, job in selected.items():
        ignored = [predecessor for predecessor in job.predecessors if predecessor not in selected]
        if ignored:
            log.logging.warning('--job {} is running without its predecessor(s): {}. They will NOT run, and {} may read stale upstream data'.format(
                name, ', '.join(ignored), name))

    return selected


def _applyJobSelection(jobsFile: DataJobsFile, arguments: argparse.Namespace, log: Log) -> DataJobsFile:
    """--job also forces the selected jobs to run, ignoring `refresh`."""

    jobs = _selectJobs(jobsFile.jobs, arguments.job, log)

    if arguments.job or arguments.force:
        jobs = {name: job.model_copy(update={'refresh': None}) for name, job in jobs.items()}

    return jobsFile.model_copy(update={'jobs': jobs, 'workers': arguments.workers or jobsFile.workers})


def _reportRun(result: RunResult, log: Log) -> int:

    for outcome in result.completed:
        log.logging.info('{}: completed in {:.1f}s, {} row(s)'.format(outcome.job, outcome.durationSeconds, outcome.rowCount),
                          extra={'job': outcome.job, 'status': outcome.status.value, 'rowCount': outcome.rowCount,
                                 'durationSeconds': round(outcome.durationSeconds, 3), 'attempts': outcome.attempts})

    if result.interrupted:
        return EXIT_INTERRUPTED

    if result.succeeded:
        return EXIT_SUCCESS

    return EXIT_JOBS_DID_NOT_SUCCEED


def _commandRun(arguments: argparse.Namespace, log: Log) -> int:
    """Holds a lock beside the memory file for the whole run, so an overlapping
    invocation -- a cron interval shorter than a slow run -- exits instead of
    running the same jobs concurrently.
    """

    jobsFile, databaseConfiguration = _loadDataJobs(arguments)
    jobsFile = _applyJobSelection(jobsFile, arguments, log)

    if arguments.dry_run:
        return _dryRunDataJobs(jobsFile, databaseConfiguration, log)

    memory, lockFile = _memoryBackend(arguments, jobsFile, databaseConfiguration)
    lockFile.parent.mkdir(parents=True, exist_ok=True)

    try:
        with exclusiveRun(lockFile):
            result = runDataJobs(jobsFile=jobsFile, databaseConfiguration=databaseConfiguration, logFile=arguments.log,
                                 memory=memory, runForever=arguments.forever,
                                 logLevel=getattr(logging, arguments.log_level.upper()), logFormat=arguments.log_format,
                                 acceptKeyChange=arguments.accept_key_change,
                                 onCycle=_cycleReporter(arguments, jobsFile, databaseConfiguration, log))
    except RunInProgressError as error:
        log.logging.error(str(error))
        return EXIT_JOBS_DID_NOT_SUCCEED

    manifestLocation = _resolveLocation(arguments, 'manifest', jobsFile.manifest)
    if manifestLocation:
        _writeManifest(manifestLocation, result, jobsFile, databaseConfiguration, arguments, log)

    return _reportRun(result, log)


def _toolVersion() -> str:

    from importlib.metadata import PackageNotFoundError, version

    try:
        return version('bauta')
    except PackageNotFoundError:
        return 'unknown'


def _writeManifest(location: Location, result: RunResult, jobsFile: DataJobsFile, databaseConfiguration: Dict[str, DatabaseConnectionConfig],
                   arguments: argparse.Namespace, log: Log) -> None:
    """Writes the run's masking manifest as sealed JSON, to a file or a table,
    even when a job failed, with the tool version and a digest of the jobs
    file. Signed when the signing key's variable is set.
    """

    jobsPath, _ = _resolveConfigurationPaths(arguments)
    manifest = result.maskingManifest(jobsFile.jobs)
    manifest.update(tool={'name': 'bauta', 'version': _toolVersion()},
                    configuration={'jobsFile': str(jobsPath), 'sha256': hashlib.sha256(jobsPath.read_bytes()).hexdigest()})

    signingKey = os.environ.get(arguments.manifest_key_variable)
    manifest = sealManifest(manifest, signingKey=signingKey)

    if isinstance(location, Path):
        location.parent.mkdir(parents=True, exist_ok=True)
        location.write_text(json.dumps(manifest, indent=2) + '\n')
        where = str(location)
    else:
        from .reporting import DatabaseManifests, newRunId

        runId = newRunId()
        DatabaseManifests(_settingsFor(location, databaseConfiguration), table=location.table or DEFAULT_TABLES['manifest']).write(manifest, runId)
        where = '{}, run {}'.format(_describeLocation(location), runId)

    log.logging.info('Wrote the masking manifest for {} job(s) to {}, {}'.format(
        len(manifest['jobs']), where, 'signed' if signingKey else 'unsigned (set ${} to sign it)'.format(arguments.manifest_key_variable)))


def _readManifest(arguments: argparse.Namespace) -> Tuple[str, Dict[str, Any]]:
    """The manifest to verify, and what to call it: the file named, else from
    --manifest-database, else from wherever the jobs file's `manifest` says --
    from a table, the latest run's unless --run names one.
    """

    location: Optional[Location]
    if arguments.manifest:
        location = Path(arguments.manifest)
        databaseConfiguration: Dict[str, DatabaseConnectionConfig] = {}
    elif arguments.manifest_database:
        location = _resolveLocation(arguments, 'manifest')
        databaseConfiguration = _loadDatabases(arguments)
    else:
        jobsFile, databaseConfiguration = _loadDataJobs(arguments)
        location = _resolveLocation(arguments, 'manifest', jobsFile.manifest)
        if location is None:
            raise UsageError('name the manifest to verify: a FILE, --manifest-database ALIAS, or `manifest` in the jobs file')

    if isinstance(location, Path):
        if arguments.run:
            raise UsageError('--run picks a manifest from a table, not a file')
        try:
            return str(location), json.loads(location.read_text())
        except FileNotFoundError as error:
            raise UsageError('no such file: {}'.format(location)) from error
        except ValueError as error:
            raise UsageError('{} is not valid JSON: {}'.format(location, error)) from error

    from .reporting import DatabaseManifests

    assert location is not None
    try:
        runId, manifest = DatabaseManifests(_settingsFor(location, databaseConfiguration),
                                            table=location.table or DEFAULT_TABLES['manifest']).read(arguments.run)
    except KeyError as error:
        raise UsageError(error.args[0]) from error
    except ValueError as error:
        raise UsageError('the manifest in {} is not valid JSON: {}'.format(_describeLocation(location), error)) from error

    return 'run {} in {}'.format(runId, _describeLocation(location)), manifest


def _commandVerifyManifest(arguments: argparse.Namespace, log: Log) -> int:
    """Checks a manifest's digest, and its signature if it has one -- which
    needs the key, or it's a usage error.
    """

    path, manifest = _readManifest(arguments)

    signingKey = os.environ.get(arguments.manifest_key_variable)

    try:
        verification = verifyManifest(manifest, signingKey=signingKey)
    except ValueError as error:
        log.logging.error('{}: {}'.format(path, error))
        return EXIT_JOBS_DID_NOT_SUCCEED

    if not verification.digestValid:
        log.logging.error('{}: the digest does not match -- the manifest was changed after it was written'.format(path))
        return EXIT_JOBS_DID_NOT_SUCCEED

    if not verification.signed:
        if signingKey is not None:
            # With a key to check against, a signature is expected, and a
            # missing one is how an edited manifest would pass: strip it,
            # recompute the digest.
            log.logging.error('{}: not signed, though ${} is set to verify a signature -- a signed manifest may have had its '
                              'signature removed'.format(path, arguments.manifest_key_variable))
            return EXIT_JOBS_DID_NOT_SUCCEED
        print('{}: intact. It is not signed, so this shows only that it is unchanged, not who wrote it.'.format(path))
        return EXIT_SUCCESS

    if signingKey is None:
        raise UsageError('{} is signed with key {}; set ${} to verify the signature'.format(
            path, verification.signingKeyFingerprint, arguments.manifest_key_variable))

    if not verification.signatureValid:
        log.logging.error('{}: the signature is not valid for key {} -- it was signed with key {}, or altered'.format(
            path, keyFingerprint(signingKey), verification.signingKeyFingerprint))
        return EXIT_JOBS_DID_NOT_SUCCEED

    print('{}: intact, and signed with key {}.'.format(path, verification.signingKeyFingerprint))

    return EXIT_SUCCESS


def _commandValidate(arguments: argparse.Namespace, log: Log) -> int:
    """Offline checks only: config schema, the job graph, and that every
    transformer reference resolves. No connection is opened, so this is safe in
    CI and in a pre-commit hook. `run --dry-run` is the online counterpart.
    """

    from .transform import resolveTransformer

    jobsFile, databaseConfiguration = _loadDataJobs(arguments)

    problems = []
    for name, job in jobsFile.jobs.items():
        for column, references in job.sourceQueryColumnTransforms.items():
            for reference in references:
                try:
                    resolveTransformer(reference)
                except Exception as error:
                    problems.append('{}: {} -> {}'.format(name, column, error))

    for alias, settings in sorted(databaseConfiguration.items()):
        try:
            DIALECTS[settings.type].connectArguments(settings, resolvePassword=False)
        except ConfigurationError as error:
            problems.append('{}: {}'.format(alias, error))

    from .masking import effectiveMaskingThreads

    try:
        effectiveMaskingThreads(jobsFile.maskingThreads)
    except ValueError as error:
        problems.append(str(error))

    if problems:
        raise ConfigurationError('invalid configuration:\n' + '\n'.join(problems))

    print('configuration is valid: {} database alias(es), {} job(s)'.format(len(databaseConfiguration), len(jobsFile.jobs)))
    print('run state: {}'.format(_describeLocation(_memoryLocation(arguments, jobsFile))))
    for setting, missing in (('history', 'not recorded'), ('manifest', 'not written')):
        location = _resolveLocation(arguments, setting, getattr(jobsFile, setting))
        print('{}: {}'.format(setting, _describeLocation(location) if location else missing))

    from .masking import MASKING_THREADS_VARIABLE, availableCores, maskingThreadsFor, nativeVersion

    if any(job.masking is not None for job in jobsFile.jobs.values()):
        if nativeVersion() is None:
            print('masking: in Python, one thread per job (pip install "bauta[native]" to use more)')
        else:
            concurrent = max(1, min(jobsFile.workers, sum(job.active for job in jobsFile.jobs.values())))
            source = '${}={}'.format(MASKING_THREADS_VARIABLE, os.environ[MASKING_THREADS_VARIABLE]) if os.environ.get(MASKING_THREADS_VARIABLE) \
                else 'maskingThreads: {}'.format(jobsFile.maskingThreads)
            shared, alone = maskingThreadsFor(jobsFile.maskingThreads, concurrent), maskingThreadsFor(jobsFile.maskingThreads, 1)
            print('masking: bauta-rs {}, {} thread(s) per job ({}; {} core(s)){}'.format(
                nativeVersion(), shared if shared == alone else '{} to {}'.format(shared, alone), source, availableCores(),
                '' if shared == alone else ': {} with {} jobs running, {} for a job running alone'.format(shared, concurrent, alone)))

    rulesPath, rulesFile = _discoveryRulesFile(arguments)
    if rulesFile is None:
        print('discovery rules: built-in')
    else:
        builtins = 'none built-in' if not rulesFile.builtins else \
            'built-in except {}'.format(', '.join(rulesFile.exclude)) if rulesFile.exclude else 'then the built-in ones'
        print('discovery rules: {} ({} name, {} value), {}'.format(rulesPath, len(rulesFile.names), len(rulesFile.values), builtins))

    return EXIT_SUCCESS


def _dryRunDataJobs(jobsFile: DataJobsFile, databaseConfiguration: Dict[str, DatabaseConnectionConfig], log: Log) -> int:
    """Everything `validate` does, plus what needs a connection: that each alias
    connects, target tables exist, and upsert targets have a primary key.
    """

    problems: List[str] = []
    aliases = sorted({job.sourceDatabase for job in jobsFile.jobs.values()} | {job.targetDatabase for job in jobsFile.jobs.values()})

    for alias in aliases:
        try:
            with Database(connectionSettings=databaseConfiguration[alias]) as database:
                encrypted = {True: 'encrypted', False: 'NOT encrypted', None: 'encryption unknown'}[database.isEncrypted()]
                log.logging.info('{}: connected ({}, {})'.format(alias, databaseConfiguration[alias].type.value, encrypted))
        except Exception as error:
            problems.append('{}: cannot connect -- {}'.format(alias, describeError(error)))

    for name, job in jobsFile.jobs.items():
        if any(problem.startswith(job.targetDatabase + ':') for problem in problems):
            continue
        try:
            with Database(connectionSettings=databaseConfiguration[job.targetDatabase]) as database:
                columns = database.getAllColumnNames(table=job.targetTableFinal)
                log.logging.info('{}: target {} has {} column(s)'.format(name, job.targetTableFinal, len(columns)))

                if job.insertStrategy.value == 'upsert' and not database.getPrimaryColumnNames(table=job.targetTableFinal):
                    problems.append('{}: target {} has no primary key, so insertStrategy: upsert cannot match rows'.format(name, job.targetTableFinal))
        except Exception as error:
            problems.append('{}: target {} is not readable -- {}'.format(name, job.targetTableFinal, describeError(error)))

        if job.masking is not None and not any(problem.startswith(job.sourceDatabase + ':') for problem in problems):
            problem = _checkMaskingCoverage(name, job, databaseConfiguration, log)
            if problem:
                problems.append(problem)

    if problems:
        for problem in problems:
            log.logging.error(problem)
        return EXIT_JOBS_DID_NOT_SUCCEED

    print('dry run passed: {} database alias(es), {} job(s), no rows moved'.format(len(aliases), len(jobsFile.jobs)))

    return EXIT_SUCCESS


def _sourceQueryColumns(job: DataJobConfig, databaseConfiguration: Dict[str, DatabaseConnectionConfig]) -> List[str]:
    """The columns a job's sourceQuery returns, by reading and discarding one
    row, since rewriting arbitrary SQL isn't portable.
    """

    with Database(connectionSettings=databaseConfiguration[job.sourceDatabase]) as database:
        query = job.sourceQuery
        parameters = None
        if job.watermarkColumn:
            query = database.substituteWatermarkPlaceholder(query)
            parameters = (job.watermarkInitial,)
        columns, chunks = database.stream(query=query, chunkSize=1, parameters=parameters)
        chunks.close()

    return columns


def _targetColumns(job: DataJobConfig, databaseConfiguration: Dict[str, DatabaseConnectionConfig]) -> List[str]:
    """The target's columns, in the order a load fills them."""

    with Database(connectionSettings=databaseConfiguration[job.targetDatabase]) as database:
        return database.getAllColumnNames(table=job.targetTableFinal)


def _checkMaskingCoverage(name: str, job: Any, databaseConfiguration: Dict[str, DatabaseConnectionConfig], log: Log) -> Optional[str]:
    """Whether the job's masking policy covers every column its query returns."""

    from .masking import MaskingPlan

    try:
        columns = _sourceQueryColumns(job, databaseConfiguration)
        plan = MaskingPlan(key=job.masking.key.get_secret_value(), columns=job.masking.columns, defaultStrategy=job.masking.defaultStrategy)
        plan.bind(columns)
        log.logging.info('{}: masking policy covers all {} column(s), key {}'.format(name, len(columns), plan.fingerprint))
    except MaskingError as error:
        return '{}: {}'.format(name, error)
    except Exception as error:
        return '{}: sourceQuery could not be checked against the masking policy -- {}'.format(name, describeError(error))

    return None


def _writeOutput(text: str, output: Optional[str]) -> None:
    """stdout, or a file that must not already exist -- a generated proposal
    overwriting a reviewed jobs.yaml would lose the review.
    """

    if not output:
        sys.stdout.write(text)
        return

    path = Path(output)
    if path.exists():
        raise UsageError('{} already exists; choose another --output, or remove it first'.format(path))

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    print('wrote {}'.format(path))


def _discoveryRulesFile(arguments: argparse.Namespace) -> Tuple[Optional[Path], Any]:
    """The discovery.yaml in use and its validated rules: --rules, else one in
    the configuration directory if there is one, else (None, None).
    """

    from .discovery import DISCOVERY_FILE

    path = Path(arguments.rules) if arguments.rules else _configDirectory(arguments) / DISCOVERY_FILE
    if not arguments.rules and not path.exists():
        return None, None

    return path, Configuration.validateDiscoveryRules(_loadYaml(path), str(path))


def _discoveryRules(arguments: argparse.Namespace) -> Any:
    """What discover, audit and synthesize recognise personal data by: the
    built-in rules, with a discovery.yaml's ahead of them.
    """

    from .discovery import discoveryRules

    return discoveryRules(_discoveryRulesFile(arguments)[1])


def _requireAlias(databaseConfiguration: Dict[str, DatabaseConnectionConfig], alias: str) -> None:

    if alias not in databaseConfiguration:
        raise UsageError('no database alias {!r}. Known aliases: {}'.format(alias, ', '.join(sorted(databaseConfiguration))))


def _generatedHeading(command: str, source: str, target: str) -> List[str]:

    return [
        'Generated by `bauta {}` on {} from {}, loading into {}.'.format(command, datetime.date.today().isoformat(), source, target),
        '',
        'A proposal, not a policy. Review every column before running this -- the',
        'comment on each says what the suggestion was based on -- and set',
        'MASKING_KEY in the environment. Sampled values were read only to classify',
        'columns; none of them appear in this file.',
        '',
        ]


def _nextSteps(source: str, target: str, tables: Sequence[str], related: bool = False) -> List[str]:
    """How to create the tables the generated jobs load, and how to refresh them."""

    tableArguments = ' '.join('--table {}'.format(table) for table in tables)
    if source == target:
        return [
            'Masking in place loads through <table>_masked_stage. Create the stage tables with:',
            '  bauta schema --database {0} --target {0} {1} --stage-suffix _masked_stage --apply'.format(source, tableArguments),
            '',
            ]

    return [
        'Create any target tables that do not exist yet with:',
        '  bauta schema --database {} --target {} {}{} --apply'.format(source, target, tableArguments, ' --related' if related else ''),
        'To refresh the copy later, empty it first, then run every job:',
        '  bauta clear --config <this directory> --yes && bauta run --config <this directory> --force',
        '',
        ]


def _commandDiscover(arguments: argparse.Namespace, log: Log) -> int:
    """Proposes a masking policy for each table from its schema and a sample,
    writing nothing to any database.
    """

    from .discovery import JobDraft, proposeTable, renderJobs

    databaseConfiguration = _loadDatabases(arguments)
    rules = _discoveryRules(arguments)
    target = arguments.target or arguments.database
    _requireAlias(databaseConfiguration, arguments.database)
    _requireAlias(databaseConfiguration, target)

    drafts = []
    with Database(connectionSettings=databaseConfiguration[arguments.database]) as database:
        try:
            foreignKeys = database.getForeignKeys()
        except NotImplementedError:
            foreignKeys = []
        requested = {table.upper(): table for table in arguments.table}
        for table in arguments.table:
            log.logging.info('Sampling up to {} row(s) of {}'.format(arguments.sample, table))
            proposal = proposeTable(database, table, sampleSize=arguments.sample, foreignKeys=foreignKeys, rules=rules)
            # Parents first, so a target that enforces foreign keys accepts the load.
            parents = sorted({requested[foreignKey.referencedTable.upper()] for foreignKey in foreignKeys
                              if foreignKey.table.upper() == table.upper() and foreignKey.referencedTable.upper() in requested
                              and foreignKey.referencedTable.upper() != table.upper()})
            drafts.append(JobDraft(table=table, sourceQuery='SELECT * FROM {}'.format(table), predecessors=parents, proposal=proposal))

    heading = _generatedHeading('discover', arguments.database, target) + _nextSteps(arguments.database, target, arguments.table)
    _writeOutput(renderJobs(drafts, arguments.database, target, heading, keyVariable=arguments.key_variable,
                            chunkSize=arguments.chunk_size), arguments.output)

    return EXIT_SUCCESS


def _commandSubset(arguments: argparse.Namespace, log: Log) -> int:
    """Generates one data job per table for a referentially complete subset,
    loading parents before children, with masking proposals if --mask is set.
    """

    from .discovery import JobDraft, proposeTable, renderJobs
    from .subset import SubsetError, planSubset

    databaseConfiguration = _loadDatabases(arguments)
    rules = _discoveryRules(arguments)
    _requireAlias(databaseConfiguration, arguments.database)
    _requireAlias(databaseConfiguration, arguments.target)

    if arguments.target == arguments.database:
        raise UsageError('--target must differ from --database: a subset is loaded into another database, not over its source')

    with Database(connectionSettings=databaseConfiguration[arguments.database]) as database:
        foreignKeys = database.getForeignKeys()

        try:
            plan = planSubset(foreignKeys, root=arguments.root, where=arguments.where, followChildren=not arguments.no_children,
                              ignore=arguments.ignore_foreign_key or [], materialize=database.dialect.supportsMaterializedSelections(),
                              quote=lambda name: quoteIdentifier(database.type, name))
        except SubsetError as error:
            raise UsageError(str(error)) from error

        log.logging.info('Subset covers {} table(s): {}'.format(len(plan.tables), ', '.join(plan.tables)))
        for foreignKey in plan.ignored:
            log.logging.warning('Ignoring foreign key {}.{} -> {}; rows may reference ones outside the subset'.format(
                foreignKey.table, ','.join(foreignKey.columns), foreignKey.referencedTable))

        drafts = []
        for table in plan.tables:
            proposal = proposeTable(database, table, sampleSize=arguments.sample, foreignKeys=foreignKeys, rules=rules) if arguments.mask else None
            drafts.append(JobDraft(table=table, sourceQuery=plan.queries[table], predecessors=plan.parents[table], proposal=proposal))

    heading = _generatedHeading('subset', arguments.database, arguments.target) + [
        'Rooted at {} where {}.'.format(arguments.root, arguments.where),
        'Jobs load parents before children, so the target can keep its foreign keys.',
        '',
        ] + _nextSteps(arguments.database, arguments.target, [arguments.root], related=True)
    _writeOutput(renderJobs(drafts, arguments.database, arguments.target, heading, keyVariable=arguments.key_variable,
                            chunkSize=arguments.chunk_size), arguments.output)

    return EXIT_SUCCESS


def _commandSchema(arguments: argparse.Namespace, log: Log) -> int:
    """Generates CREATE TABLE statements for the target, or applies them.

    --apply skips tables the target already has, so it's safe to re-run, and
    never alters or drops anything.
    """

    from .schema import SchemaError, createStatements, readTable, renderScript
    from .subset import relatedTables

    databaseConfiguration = _loadDatabases(arguments)
    _requireAlias(databaseConfiguration, arguments.database)
    _requireAlias(databaseConfiguration, arguments.target)

    sourceSettings = databaseConfiguration[arguments.database]
    targetSettings = databaseConfiguration[arguments.target]
    stagesOnly = arguments.database == arguments.target

    if stagesOnly and not arguments.stage_suffix:
        raise UsageError('--target is the source database, so its tables already exist; pass --stage-suffix to create stage tables for them')

    with Database(connectionSettings=sourceSettings) as source:
        foreignKeys = source.getForeignKeys()
        tables = relatedTables(foreignKeys, arguments.table, followChildren=not arguments.no_children) if arguments.related else arguments.table
        try:
            definitions = [readTable(source, table, foreignKeys) for table in tables]
            statements = createStatements(sourceSettings.type, targetSettings.type, definitions, includeForeignKeys=not arguments.no_foreign_keys,
                                          stageSuffix=arguments.stage_suffix, stagesOnly=stagesOnly)
        except SchemaError as error:
            raise UsageError(str(error)) from error

    if not arguments.apply:
        heading = [
            'Generated by `bauta schema` on {} from {} ({}), for {} ({}).'.format(
                datetime.date.today().isoformat(), arguments.database, sourceSettings.type.value, arguments.target, targetSettings.type.value),
            'Columns, nullability, primary keys and foreign keys only: no indexes, defaults or constraints beyond those.',
            'Review the commented type choices before applying.',
            ]
        _writeOutput(renderScript(statements, heading), arguments.output)
        return EXIT_SUCCESS

    created = skipped = 0
    with Database(connectionSettings=targetSettings) as target:
        for statement in statements:
            if target.tableExists(statement.table):
                log.logging.info('{}: already exists, left as it is'.format(statement.table))
                skipped += 1
                continue
            for note in statement.notes:
                log.logging.warning('{}: {}'.format(statement.table, note))
            try:
                target.alter(statement.sql)
            except Exception as error:
                raise UsageError('could not create {} ({} created before it): {}'.format(
                    statement.table, created, describeError(error))) from error
            log.logging.info('{}: created'.format(statement.table))
            created += 1

    print('schema applied to {}: {} table(s) created, {} already existed'.format(arguments.target, created, skipped))

    return EXIT_SUCCESS


def _parseTableRows(entries: Sequence[str], default: int) -> List[Tuple[str, int]]:
    """`--table orders:500` entries, with `--rows` for any without a count."""

    parsed = []
    for entry in entries:
        table, separator, count = entry.rpartition(':')
        if not separator:
            parsed.append((entry, default))
            continue
        if not table or not count.isdigit() or int(count) < 1:
            raise UsageError('--table takes TABLE or TABLE:ROWS with a positive count, got {!r}'.format(entry))
        parsed.append((table, int(count)))

    return parsed


def _commandSynthesize(arguments: argparse.Namespace, log: Log) -> int:
    """Fills existing tables with generated rows, parents first.

    Writes only with --yes, like `clear`. --dry-run shows what each column
    would get and a few generated rows, which are safe to print.
    """

    from .schema import SchemaError, orderParentsFirst
    from .synthesize import SynthesisError, planTable, synthesizeTable

    databaseConfiguration = _loadDatabases(arguments)
    rules = _discoveryRules(arguments)
    _requireAlias(databaseConfiguration, arguments.database)
    requested = dict(_parseTableRows(arguments.table, arguments.rows))

    if not arguments.dry_run and not arguments.yes:
        raise UsageError('synthesize inserts generated rows into {}; pass --yes to do it, or --dry-run to see the plan'.format(
            ', '.join(requested)))

    with Database(connectionSettings=databaseConfiguration[arguments.database]) as database:
        foreignKeys = database.getForeignKeys()
        try:
            order = orderParentsFirst(requested, foreignKeys)
        except SchemaError as error:
            raise UsageError(str(error)) from error

        for table in order:
            rows = requested[table]
            try:
                if arguments.dry_run:
                    columns, makeRow, plans, available = planTable(database, table, rows, seed=arguments.seed, foreignKeys=foreignKeys, rules=rules)
                    print('{}: {} row(s){}'.format(table, available, '' if available == rows else ' (all its keys allow, of {} asked)'.format(rows)))
                    for plan in plans:
                        print('  {:<28} {:<12} {}'.format(plan.column, plan.source, plan.description))
                    for row in range(min(3, available)):
                        print('  sample: {}'.format(dict(zip(columns, makeRow(row)))))
                    continue
                inserted = synthesizeTable(database, table, rows, seed=arguments.seed, foreignKeys=foreignKeys, rules=rules)
            except SynthesisError as error:
                raise UsageError(str(error)) from error
            log.logging.info('{}: inserted {} synthetic row(s)'.format(table, inserted), extra={'table': table, 'rowCount': inserted})
            print('{}: inserted {} row(s)'.format(table, inserted))

    if arguments.dry_run:
        print('Parents are listed first. Foreign-key samples above come from the parents as they are now; '
              'a real run fills each parent before its children.')

    return EXIT_SUCCESS


def _commandClear(arguments: argparse.Namespace, log: Log) -> int:
    """Empties the target tables of the selected jobs, children first.

    Refuses incremental jobs: their stored watermark would survive, so the
    next run would reload only new rows into an empty table.
    """

    from .schema import SchemaError, clearOrder, clearTables

    jobsFile, databaseConfiguration = _loadDataJobs(arguments)
    jobs = {name: job for name, job in _selectJobs(jobsFile.jobs, arguments.job, log).items() if job.active}

    incremental = sorted(name for name, job in jobs.items() if job.watermarkColumn)
    if incremental:
        raise UsageError('refusing to clear the targets of incremental job(s) {}: their stored watermark would make the next run '
                         'load only new rows. Leave them out with --job'.format(', '.join(incremental)))

    tablesByDatabase: Dict[str, List[str]] = {}
    for job in jobs.values():
        tables = tablesByDatabase.setdefault(job.targetDatabase, [])
        if job.targetTableFinal.upper() not in {table.upper() for table in tables}:
            tables.append(job.targetTableFinal)

    if not arguments.dry_run and not arguments.yes:
        raise UsageError('clear deletes every row of {} table(s); pass --yes to do it, or --dry-run to see the plan'.format(
            sum(len(tables) for tables in tablesByDatabase.values())))

    for alias, tables in sorted(tablesByDatabase.items()):
        with Database(connectionSettings=databaseConfiguration[alias]) as database:
            try:
                if arguments.dry_run:
                    print('{}: would empty, in order: {}'.format(alias, ', '.join(clearOrder(tables, database.getForeignKeys()))))
                    continue
                cleared = clearTables(database, tables)
            except SchemaError as error:
                raise UsageError('{}: {}'.format(alias, error)) from error
            except Exception as error:
                log.logging.error('{}: nothing was cleared -- {}'.format(alias, describeError(error)))
                return EXIT_JOBS_DID_NOT_SUCCEED

        for table, rows in cleared:
            log.logging.info('{}: emptied {} ({} row(s))'.format(alias, table, rows))
        print('{}: emptied {}'.format(alias, ', '.join(table for table, _ in cleared)))

    if not arguments.dry_run:
        # Emptied targets hold nothing masked under the old key any more, so
        # a key change is no longer a reason to refuse these jobs.
        memory, _ = _memoryBackend(arguments, jobsFile, databaseConfiguration)
        for name, job in jobs.items():
            if job.masking is not None:
                memory.recordKeyFingerprint(name, None)
        print('Run the jobs with --force, so a `refresh` window cannot leave a cleared table empty.')

    return EXIT_SUCCESS


def _commandAudit(arguments: argparse.Namespace, log: Log) -> int:
    """Reports what each job does with data, and anything a reviewer should
    question. Offline unless --connect, which also resolves each masked
    query's real columns and checks whether each connection is encrypted.

    Exits 1 on an error finding, and with --strict on a warning too.
    """

    from .audit import auditJobs, renderAudit

    jobsFile, databaseConfiguration = _loadDataJobs(arguments)
    jobs = _selectJobs(jobsFile.jobs, arguments.job, log)
    returnedColumns: Dict[str, List[str]] = {}
    targetColumns: Dict[str, List[str]] = {}
    unreachable: Dict[str, str] = {}
    encryption: Dict[str, Optional[bool]] = {}
    foreignKeys: Dict[str, List[ForeignKey]] = {}

    if arguments.connect:
        for name, job in jobs.items():
            if job.masking is not None:
                try:
                    returnedColumns[name] = _sourceQueryColumns(job, databaseConfiguration)
                    targetColumns[name] = job.targetColumns or _targetColumns(job, databaseConfiguration)
                except Exception as error:
                    unreachable[name] = describeError(error)

        keysByAlias: Dict[str, List[ForeignKey]] = {}
        for alias in sorted({job.sourceDatabase for job in jobs.values()} | {job.targetDatabase for job in jobs.values()}):
            isSqlite = databaseConfiguration[alias].type.value == 'sqlite'
            try:
                with Database(connectionSettings=databaseConfiguration[alias]) as database:
                    if not isSqlite:
                        encryption[alias] = database.isEncrypted()
                    try:
                        keysByAlias[alias] = database.getForeignKeys()
                    except Exception as error:
                        log.logging.warning('{}: could not read foreign keys -- {}'.format(alias, describeError(error)))
            except Exception as error:
                log.logging.warning('{}: could not connect to check encryption and foreign keys -- {}'.format(alias, describeError(error)))
                if not isSqlite:
                    encryption[alias] = None

        # The keys that apply to a copy are the target's own and those of the
        # sources it is copied from, which a target often doesn't declare.
        # Both are matched to jobs by table name.
        for target in {job.targetDatabase for job in jobs.values()}:
            sources = {job.sourceDatabase for job in jobs.values() if job.targetDatabase == target}
            unique: Dict[Any, ForeignKey] = {}
            for alias in [target] + sorted(sources):
                for foreignKey in keysByAlias.get(alias, []):
                    folded = (foreignKey.table.upper(), tuple(column.upper() for column in foreignKey.columns),
                              foreignKey.referencedTable.upper(), tuple(column.upper() for column in foreignKey.referencedColumns))
                    unique.setdefault(folded, foreignKey)
            foreignKeys[target] = list(unique.values())

    report = auditJobs(jobs, returnedColumns=returnedColumns, encryption=encryption, unreachable=unreachable,
                       targetColumns=targetColumns, foreignKeys=foreignKeys, rules=_discoveryRules(arguments))
    _writeOutput(json.dumps(report, indent=2, default=str) + '\n' if arguments.format == 'json' else renderAudit(report), arguments.output)

    if report['summary']['error'] or (arguments.strict and report['summary']['warning']):
        return EXIT_JOBS_DID_NOT_SUCCEED

    return EXIT_SUCCESS


def _commandHistory(arguments: argparse.Namespace, log: Log) -> int:
    """The latest outcomes `run` recorded, newest first, from the flags'
    history or else the jobs file's.
    """

    from .reporting import renderHistory

    location: Optional[Location]
    if arguments.history:
        location, databaseConfiguration = _resolveLocation(arguments, 'history'), {}
    elif arguments.history_database:
        location, databaseConfiguration = _resolveLocation(arguments, 'history'), _loadDatabases(arguments)
    else:
        jobsFile, databaseConfiguration = _loadDataJobs(arguments)
        location = _resolveLocation(arguments, 'history', jobsFile.history)
        if location is None:
            raise UsageError('name the history to read: --history FILE, --history-database ALIAS, or `history` in the jobs file')

    assert location is not None
    history = _history(location, databaseConfiguration)

    records = history.read(limit=arguments.limit, job=arguments.job)
    sys.stdout.write(json.dumps(records, indent=2) + '\n' if arguments.format == 'json' else renderHistory(records))

    return EXIT_SUCCESS


def _commandJobs(arguments: argparse.Namespace, log: Log) -> int:
    """Prints the graph as the scheduler sees it now, including which jobs
    their refresh window holds back.
    """

    jobsFile, databaseConfiguration = _loadDataJobs(arguments)
    memory, _ = _memoryBackend(arguments, jobsFile, databaseConfiguration)
    graph = DependencyGraph(jobs=jobsFile.jobs, memory=memory.read())
    watermarks = memory.readWatermarks()

    print('{:<28} {:<10} {:<22} {}'.format('JOB', 'STATE', 'WAITS FOR', 'WATERMARK'))

    for name, job in sorted(jobsFile.jobs.items()):
        if not job.active:
            state = 'inactive'
        elif name not in graph.activeJobs:
            state = 'throttled'
        else:
            state = 'due'

        waitsFor = ', '.join(graph.activePredecessors.get(name, job.predecessors)) or '-'
        print('{:<28} {:<10} {:<22} {}'.format(name, state, waitsFor, watermarks.get(name, '-')))

    print('\n{} of {} job(s) due this cycle. "throttled" means inside its `refresh` window.'.format(len(graph.activeJobs), len(jobsFile.jobs)))

    return EXIT_SUCCESS


def _addCommonArguments(parser: argparse.ArgumentParser, jobs: bool = True, memory: bool = True) -> None:

    parser.add_argument('--config', help='directory holding jobs.yaml and database.yaml (default: ${} or ./configuration)'.format(
        CONFIG_DIRECTORY_VARIABLE))
    if jobs:
        parser.add_argument('--jobs', help='explicit path to the jobs file, overriding --config')
    parser.add_argument('--databases', help='explicit path to the database file, overriding --config')
    if jobs and memory:
        parser.add_argument('--memory', help='path to the run-memory file (default: jobs.yaml\'s `memory`, else memory.yaml beside jobs.yaml)')
        parser.add_argument('--memory-database', metavar='ALIAS',
                            help='keep run memory in this database instead of a file (see docs/operations.md)')
        parser.add_argument('--memory-table', help='the run-memory table (default: jobs.yaml\'s, else bauta_memory)')
    _addLoggingArguments(parser)


def _addHistoryArguments(parser: argparse.ArgumentParser) -> None:

    parser.add_argument('--history', metavar='FILE', help='run history as JSON lines (default: jobs.yaml\'s `history`)')
    parser.add_argument('--history-database', metavar='ALIAS', help='run history in this database')
    parser.add_argument('--history-table', help='the history table (default: jobs.yaml\'s, else bauta_history)')


def _addManifestLocationArguments(parser: argparse.ArgumentParser) -> None:

    parser.add_argument('--manifest-database', metavar='ALIAS', help='the masking manifest in this database')
    parser.add_argument('--manifest-table', help='the manifest table (default: jobs.yaml\'s, else bauta_manifest)')


def _addManifestKeyArgument(parser: argparse.ArgumentParser) -> None:

    parser.add_argument('--manifest-key-variable', default=MANIFEST_KEY_VARIABLE,
                        help='environment variable holding the manifest signing key (default: {})'.format(MANIFEST_KEY_VARIABLE))


def _addLoggingArguments(parser: argparse.ArgumentParser) -> None:

    parser.add_argument('--log', help='also write logs to this file (logs always go to stderr unless --quiet)')
    parser.add_argument('--log-level', default='info', choices=['debug', 'info', 'warning', 'error'], help='default: info')
    parser.add_argument('--log-format', default='text', choices=['text', 'json'],
                        help='json emits one object per record, carrying job/status/rowCount as fields a log collector can filter and alert on')
    parser.add_argument('--quiet', action='store_true', help='do not log to stderr')


def _positiveInteger(text: str) -> int:

    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError('must be at least 1, got {}'.format(value))

    return value


def _addRunArguments(parser: argparse.ArgumentParser) -> None:

    parser.add_argument('--forever', action='store_true',
                        help='keep running, honoring each job\'s refresh window. Prefer a single run from cron or a CronJob; '
                             'use this only for freshness below cron\'s one-minute floor, or where there is no scheduler')
    parser.add_argument('--job', action='append', help='run only this job (repeatable). Implies --force, and does NOT run its predecessors')
    parser.add_argument('--force', action='store_true', help='ignore refresh windows')
    parser.add_argument('--workers', type=_positiveInteger, help='override the worker count from configuration')
    parser.add_argument('--dry-run', action='store_true',
                        help='check connections, target tables, primary keys and masking coverage without moving any rows')


def _addRulesArgument(parser: argparse.ArgumentParser) -> None:

    parser.add_argument('--rules', metavar='FILE', help='your own rules for recognising personal data, ahead of the built-in ones '
                                                        '(default: discovery.yaml in the configuration directory, if there is one)')


def _addGeneratorArguments(parser: argparse.ArgumentParser) -> None:

    parser.add_argument('--sample', type=_positiveInteger, default=1000, help='rows sampled per table to classify columns (default: 1000)')
    parser.add_argument('--key-variable', default='MASKING_KEY', help='environment variable the generated jobs read the masking key from')
    parser.add_argument('--chunk-size', type=_positiveInteger, default=5000, help='chunkSize for the generated jobs (default: 5000)')
    parser.add_argument('--output', help='write the generated jobs here instead of stdout; must not already exist')


class _PrintVersion(argparse.Action):
    """`bauta --version`: this package's version, and which masker it would
    use -- the two things anyone helping with a problem asks first.
    """

    def __init__(self, option_strings: Sequence[str], dest: str = argparse.SUPPRESS, default: Any = argparse.SUPPRESS,
                 help: Optional[str] = None) -> None:
        super().__init__(option_strings, dest=dest, default=default, nargs=0, help=help)

    def __call__(self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: Optional[str] = None) -> None:
        from .masking import nativeVersion

        native = nativeVersion()
        if native:
            masker = 'bauta-rs {}'.format(native)
        elif os.environ.get('BAUTA_NATIVE') == '0':
            masker = 'python (BAUTA_NATIVE=0 turns the native masker off)'
        else:
            masker = 'python (pip install "bauta[native]" for the native masker)'
        print('bauta {}'.format(_toolVersion()))
        print('masking: {}'.format(masker))
        parser.exit()


def _buildParser() -> argparse.ArgumentParser:

    parser = argparse.ArgumentParser(prog='bauta', description='Move, mask and subset data between databases, with jobs defined in YAML.')
    parser.add_argument('--version', action=_PrintVersion, help='print the version, and which masker it would use')
    subparsers = parser.add_subparsers(dest='command', required=True)

    runParser = subparsers.add_parser('run', help='run data jobs')
    _addCommonArguments(runParser)
    _addRunArguments(runParser)
    runParser.add_argument('--manifest', metavar='FILE',
                           help='write a JSON record of what was masked, how, and under which key fingerprint (default: jobs.yaml\'s `manifest`)')
    _addManifestLocationArguments(runParser)
    _addManifestKeyArgument(runParser)
    runParser.add_argument('--accept-key-change', action='store_true',
                           help='run upsert jobs even though their masking key changed since their last run')
    _addHistoryArguments(runParser)
    runParser.add_argument('--notify-url', metavar='URL', help='post a JSON summary to this webhook when a cycle does not succeed '
                                                              '(default: ${})'.format(NOTIFY_URL_VARIABLE))
    runParser.add_argument('--notify-on', default='failure', choices=['failure', 'always'], help='default: failure')
    runParser.set_defaults(handler=_commandRun)

    validateParser = subparsers.add_parser('validate', help='check configuration offline, without connecting to anything')
    _addCommonArguments(validateParser)
    _addRulesArgument(validateParser)
    validateParser.set_defaults(handler=_commandValidate)

    auditParser = subparsers.add_parser('audit', help='report what each job does with data, and what a reviewer should question')
    # Reads the jobs, never run state, so no --memory flags.
    _addCommonArguments(auditParser, memory=False)
    auditParser.add_argument('--connect', action='store_true',
                             help='also run each masked query for its real columns, and check whether each connection is encrypted')
    auditParser.add_argument('--job', action='append', help='audit only this job (repeatable)')
    auditParser.add_argument('--format', default='text', choices=['text', 'json'], help='default: text')
    auditParser.add_argument('--strict', action='store_true', help='exit 1 on warnings as well as errors')
    auditParser.add_argument('--output', help='write the report here instead of stdout; must not already exist')
    _addRulesArgument(auditParser)
    auditParser.set_defaults(handler=_commandAudit)

    verifyParser = subparsers.add_parser('verify-manifest', help='check that a manifest is unaltered, and who signed it')
    verifyParser.add_argument('manifest', nargs='?', help='a manifest file (default: --manifest-database, else jobs.yaml\'s `manifest`)')
    _addManifestLocationArguments(verifyParser)
    verifyParser.add_argument('--run', metavar='RUN_ID', help='from a table, this run\'s manifest rather than the latest')
    _addCommonArguments(verifyParser, memory=False)
    _addManifestKeyArgument(verifyParser)
    verifyParser.set_defaults(handler=_commandVerifyManifest)

    historyParser = subparsers.add_parser('history', help='show recent job outcomes recorded with run --history')
    _addCommonArguments(historyParser, memory=False)
    _addHistoryArguments(historyParser)
    historyParser.add_argument('--job', help='only this job')
    historyParser.add_argument('--limit', type=_positiveInteger, default=20, help='how many records (default: 20)')
    historyParser.add_argument('--format', default='text', choices=['text', 'json'], help='default: text')
    historyParser.set_defaults(handler=_commandHistory)

    jobsParser = subparsers.add_parser('jobs', help='show the job graph and which jobs are due')
    _addCommonArguments(jobsParser)
    jobsParser.set_defaults(handler=_commandJobs)

    discoverParser = subparsers.add_parser('discover', help='propose a masking policy for tables, from their schema and a sample')
    _addCommonArguments(discoverParser, jobs=False)
    discoverParser.add_argument('--database', required=True, help='the alias to read from')
    discoverParser.add_argument('--table', action='append', required=True, help='a table to propose a policy for (repeatable)')
    discoverParser.add_argument('--target', help='the alias the generated jobs load into (default: --database, masking in place)')
    _addGeneratorArguments(discoverParser)
    _addRulesArgument(discoverParser)
    discoverParser.set_defaults(handler=_commandDiscover)

    subsetParser = subparsers.add_parser('subset', help='generate jobs that copy a referentially complete subset')
    _addCommonArguments(subsetParser, jobs=False)
    subsetParser.add_argument('--database', required=True, help='the alias to read from')
    subsetParser.add_argument('--target', required=True, help='the alias the generated jobs load into')
    subsetParser.add_argument('--root', required=True, help='the table the subset starts from')
    subsetParser.add_argument('--where', required=True, help='SQL filter on the root table, e.g. "created_at >= \'2026-01-01\'"')
    subsetParser.add_argument('--no-children', action='store_true', help='copy only the root rows and what they reference, not rows referencing them')
    subsetParser.add_argument('--ignore-foreign-key', action='append', metavar='TABLE.COLUMN',
                              help='do not follow this foreign key (repeatable); needed to break a cycle')
    subsetParser.add_argument('--mask', action='store_true', help='also propose a masking policy for every table, as discover does')
    _addGeneratorArguments(subsetParser)
    _addRulesArgument(subsetParser)
    subsetParser.set_defaults(handler=_commandSubset)

    schemaParser = subparsers.add_parser('schema', help='generate or apply CREATE TABLE statements for a target, from source tables')
    _addCommonArguments(schemaParser, jobs=False)
    schemaParser.add_argument('--database', required=True, help='the alias to read table definitions from')
    schemaParser.add_argument('--target', required=True, help='the alias the tables are for; its dialect decides the types')
    schemaParser.add_argument('--table', action='append', required=True, help='a table to create (repeatable)')
    schemaParser.add_argument('--related', action='store_true', help='also every table a subset rooted at --table would copy')
    schemaParser.add_argument('--no-children', action='store_true', help='with --related, only the tables --table references')
    schemaParser.add_argument('--no-foreign-keys', action='store_true', help='leave foreign keys out of the generated tables')
    schemaParser.add_argument('--stage-suffix', help='also create <table><suffix> stage tables, for swap jobs; alone if --target is --database')
    schemaParser.add_argument('--apply', action='store_true', help='create the tables in --target, skipping any that already exist')
    schemaParser.add_argument('--output', help='write the SQL here instead of stdout; must not already exist')
    schemaParser.set_defaults(handler=_commandSchema)

    synthesizeParser = subparsers.add_parser('synthesize', help='fill existing tables with generated rows, for data that can\'t be copied')
    _addCommonArguments(synthesizeParser, jobs=False)
    synthesizeParser.add_argument('--database', required=True, help='the alias whose tables to fill')
    synthesizeParser.add_argument('--table', action='append', required=True, metavar='TABLE[:ROWS]',
                                  help='a table to fill, and how many rows (repeatable); parents are filled first')
    synthesizeParser.add_argument('--rows', type=_positiveInteger, default=100, help='rows for a --table without a count (default: 100)')
    synthesizeParser.add_argument('--seed', type=int, default=0, help='the same seed makes the same rows (default: 0)')
    synthesizeParser.add_argument('--dry-run', action='store_true', help='show what each column gets, and sample rows, without writing')
    synthesizeParser.add_argument('--yes', action='store_true', help='actually insert the rows')
    _addRulesArgument(synthesizeParser)
    synthesizeParser.set_defaults(handler=_commandSynthesize)

    clearParser = subparsers.add_parser('clear', help='empty the target tables of data jobs, children first (destructive)')
    _addCommonArguments(clearParser)
    clearParser.add_argument('--job', action='append', help='clear only this job\'s target (repeatable)')
    clearParser.add_argument('--dry-run', action='store_true', help='show which tables would be emptied, and in what order')
    clearParser.add_argument('--yes', action='store_true', help='actually delete the rows')
    clearParser.set_defaults(handler=_commandClear)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:

    parser = _buildParser()
    arguments = parser.parse_args(argv)

    for name in ('job', 'force', 'workers', 'dry_run', 'forever', 'manifest', 'jobs', 'memory', 'memory_database', 'memory_table', 'yes', 'config',
                 'databases', 'accept_key_change', 'history', 'history_database', 'history_table', 'manifest_database', 'manifest_table', 'run',
                 'notify_url', 'rules'):
        if not hasattr(arguments, name):
            setattr(arguments, name, None)

    log = _configureLogging(arguments)

    try:
        return int(arguments.handler(arguments, log))
    except (ConfigurationError, UsageError) as error:
        log.logging.error(str(error))
        return EXIT_BAD_CONFIGURATION
    except KeyboardInterrupt:
        log.logging.warning('Interrupted')
        return EXIT_INTERRUPTED
    except Exception as error:
        # Logged, rather than left to Python's own traceback, so a driver
        # error's quoted values are scrubbed on the way out.
        log.logging.error('Failed: {}'.format(describeError(error)), exc_info=error)
        return EXIT_JOBS_DID_NOT_SUCCEED


if __name__ == '__main__':
    sys.exit(main())
