"""The `lightweight-etl` command.

This is the one place in the package that knows about file paths. Everything
under it stays I/O-free -- Configuration validates dicts it is handed, and the
runners take already-validated objects -- so the CLI is where YAML loading,
path defaults and environment lookups live, and nowhere else.

Exit codes, because for anything that schedules work the exit code *is* the
interface:

    0    every active job completed
    1    at least one job failed or was skipped
    2    invalid configuration, or a usage error
    130  interrupted

Single-shot is the default. `--forever` exists for freshness below cron's
one-minute floor, or where there is no scheduler to hook into -- see runner.py's
runDataJobs for the reasoning.
"""
from __future__ import annotations

import argparse
import datetime
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import yaml

from .configuration import Configuration, ConfigurationError, DatabaseConnectionConfig, DataJobConfig, DataJobsFile, expandEnvironmentVariables
from .database import Database
from .dependencyGraph import DependencyGraph
from .log import Log
from .masking import MaskingError
from .memory import FileMemory, RunInProgressError, exclusiveRun
from .runner import RunResult, runDataJobs

EXIT_SUCCESS = 0
EXIT_JOBS_DID_NOT_SUCCEED = 1
EXIT_BAD_CONFIGURATION = 2
EXIT_INTERRUPTED = 130

CONFIG_DIRECTORY_VARIABLE = 'LIGHTWEIGHT_ETL_CONFIG'


class UsageError(Exception):
    """A problem with how the command was invoked, rather than with a job."""


def _loadYaml(path: Path) -> Any:
    """Load a YAML file, expanding ${NAME} from the environment.

    Expansion happens here rather than in Configuration because reading the
    environment is I/O, and this module is where this package does its I/O.
    A library caller who loads their own YAML calls expandEnvironmentVariables
    themselves -- see docs/library.md.
    """

    try:
        with open(path) as file:
            return expandEnvironmentVariables(yaml.load(file, Loader=yaml.FullLoader))
    except FileNotFoundError as error:
        raise UsageError('no such file: {}'.format(path)) from error
    except yaml.YAMLError as error:
        raise UsageError('{} is not valid YAML: {}'.format(path, error)) from error


def _configDirectory(arguments: argparse.Namespace) -> Path:
    """--config, else $LIGHTWEIGHT_ETL_CONFIG, else ./configuration -- so the
    common case is a bare `lightweight-etl run`.
    """

    return Path(arguments.config or os.environ.get(CONFIG_DIRECTORY_VARIABLE) or 'configuration')


def _resolveConfigurationPaths(arguments: argparse.Namespace) -> Tuple[Path, Path]:
    """Explicit --jobs/--databases win; otherwise both come from the config directory."""

    configDirectory = _configDirectory(arguments)
    jobsPath = Path(arguments.jobs) if arguments.jobs else configDirectory / 'jobs.yaml'
    databasesPath = Path(arguments.databases) if arguments.databases else configDirectory / 'database.yaml'

    return jobsPath, databasesPath


def _resolveMemoryPath(arguments: argparse.Namespace, log: Log) -> Path:
    """--memory, else memory.yaml in the config directory.

    Beside the configuration rather than in the working directory, so that a
    cron entry or a container that starts somewhere else still finds the same
    run state -- losing it silently means every incremental job re-extracts
    from watermarkInitial. A memory.yaml left in the working directory by an
    earlier version is still used, with a warning, until it is moved.
    """

    if arguments.memory:
        return Path(arguments.memory)

    path = _configDirectory(arguments) / 'memory.yaml'
    legacy = Path('memory.yaml')

    if not path.exists() and legacy.exists() and legacy.resolve() != path.resolve():
        log.logging.warning('Using ./memory.yaml from the working directory; run state now defaults to {}. '
                            'Move the file there, or pass --memory'.format(path))
        return legacy

    return path


def _configureLogging(arguments: argparse.Namespace) -> Log:
    """stdout by default; --log additionally writes a file.

    This inverts the library's file-first default deliberately. In a container
    logs have to reach stdout to be collected at all, and a CLI that writes its
    only output to a file nobody named is a CLI that looks like it did nothing.
    """

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

    return jobsFile, databaseConfiguration


def _selectJobs(jobs: Dict[str, DataJobConfig], requested: Optional[List[str]], log: Log) -> Dict[str, DataJobConfig]:
    """Narrow a job map to --job selections, warning about predecessors dropped.

    Running only what was asked for is the right default here: the use case is
    iterating on one job and wanting a fast loop. But silently ignoring a
    declared dependency is how a --job ends up in a cron and produces a
    stale-upstream incident months later, so every skipped predecessor is named.
    """

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
    """--job also forces the selected jobs to run, ignoring `refresh`.

    Asking for a job explicitly and getting nothing because it ran four minutes
    ago is baffling behaviour to debug, so the selection implies --force.
    """

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

    memoryPath = _resolveMemoryPath(arguments, log)
    memoryPath.parent.mkdir(parents=True, exist_ok=True)

    try:
        with exclusiveRun(memoryPath.with_name(memoryPath.name + '.run.lock')):
            result = runDataJobs(jobsFile=jobsFile, databaseConfiguration=databaseConfiguration, logFile=arguments.log,
                                 memory=FileMemory(memoryFile=memoryPath), runForever=arguments.forever,
                                 logLevel=getattr(logging, arguments.log_level.upper()), logFormat=arguments.log_format)
    except RunInProgressError as error:
        log.logging.error(str(error))
        return EXIT_JOBS_DID_NOT_SUCCEED

    if arguments.manifest:
        _writeManifest(Path(arguments.manifest), result, jobsFile, log)

    return _reportRun(result, log)


def _writeManifest(path: Path, result: RunResult, jobsFile: DataJobsFile, log: Log) -> None:
    """Writes the run's masking manifest as JSON.

    Written even when a job failed -- a record that a masked copy was *not*
    refreshed is as much a part of the audit trail as one that it was.
    """

    manifest = result.maskingManifest(jobsFile.jobs)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, default=str) + '\n')
    log.logging.info('Wrote the masking manifest for {} job(s) to {}'.format(len(manifest['jobs']), path))


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

    if problems:
        raise ConfigurationError('unresolvable transformer reference(s):\n' + '\n'.join(problems))

    print('configuration is valid: {} database alias(es), {} job(s)'.format(len(databaseConfiguration), len(jobsFile.jobs)))

    return EXIT_SUCCESS


def _dryRunDataJobs(jobsFile: DataJobsFile, databaseConfiguration: Dict[str, DatabaseConnectionConfig], log: Log) -> int:
    """Everything `validate` does, plus what needs a connection: that each alias
    actually connects, that its driver is installed, that target tables exist,
    and that upsert targets have a primary key.

    The last is a real trap -- without a key the column buckets come back empty,
    and the generated upsert silently degrades rather than failing loudly.
    """

    problems: List[str] = []
    aliases = sorted({job.sourceDatabase for job in jobsFile.jobs.values()} | {job.targetDatabase for job in jobsFile.jobs.values()})

    for alias in aliases:
        try:
            with Database(connectionSettings=databaseConfiguration[alias]) as database:
                log.logging.info('{}: connected ({})'.format(alias, databaseConfiguration[alias].type.value))
        except Exception as error:
            problems.append('{}: cannot connect -- {}: {}'.format(alias, type(error).__name__, error))

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
            problems.append('{}: target {} is not readable -- {}: {}'.format(name, job.targetTableFinal, type(error).__name__, error))

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


def _checkMaskingCoverage(name: str, job: Any, databaseConfiguration: Dict[str, DatabaseConnectionConfig], log: Log) -> Optional[str]:
    """Whether the job's masking policy covers every column its query returns.

    Finding the columns means running the query, so this reads a single row --
    and discards it unexamined -- rather than trusting a WHERE 1=0 rewrite of
    arbitrary SQL to be valid on every dialect.
    """

    from .masking import MaskingPlan

    try:
        with Database(connectionSettings=databaseConfiguration[job.sourceDatabase]) as database:
            query = job.sourceQuery
            parameters = None
            if job.watermarkColumn:
                query = database.substituteWatermarkPlaceholder(query)
                parameters = (job.watermarkInitial,)
            columns, chunks = database.stream(query=query, chunkSize=1, parameters=parameters)
            chunks.close()  # type: ignore[attr-defined]

        plan = MaskingPlan(key=job.masking.key.get_secret_value(), columns=job.masking.columns, defaultStrategy=job.masking.defaultStrategy)
        plan.bind(columns)
        log.logging.info('{}: masking policy covers all {} column(s), key {}'.format(name, len(columns), plan.fingerprint))
    except MaskingError as error:
        return '{}: {}'.format(name, error)
    except Exception as error:
        return '{}: sourceQuery could not be checked against the masking policy -- {}: {}'.format(name, type(error).__name__, error)

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


def _requireAlias(databaseConfiguration: Dict[str, DatabaseConnectionConfig], alias: str) -> None:

    if alias not in databaseConfiguration:
        raise UsageError('no database alias {!r}. Known aliases: {}'.format(alias, ', '.join(sorted(databaseConfiguration))))


def _generatedHeading(command: str, source: str, target: str) -> List[str]:

    return [
        'Generated by `lightweight-etl {}` on {} from {}, loading into {}.'.format(command, datetime.date.today().isoformat(), source, target),
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
            '  lightweight-etl schema --database {0} --target {0} {1} --stage-suffix _masked_stage --apply'.format(source, tableArguments),
            '',
            ]

    return [
        'Create any target tables that do not exist yet with:',
        '  lightweight-etl schema --database {} --target {} {}{} --apply'.format(source, target, tableArguments, ' --related' if related else ''),
        'To refresh the copy later, empty it first, then run every job:',
        '  lightweight-etl clear --config <this directory> --yes && lightweight-etl run --config <this directory> --force',
        '',
        ]


def _commandDiscover(arguments: argparse.Namespace, log: Log) -> int:
    """Proposes a masking policy for each table, from its schema and a sample.

    Offline in the sense that matters: it reads, and writes nothing to any
    database. The proposal goes to stdout or --output for review.
    """

    from .discovery import JobDraft, proposeTable, renderJobs

    databaseConfiguration = _loadDatabases(arguments)
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
            proposal = proposeTable(database, table, sampleSize=arguments.sample, foreignKeys=foreignKeys)
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
    _requireAlias(databaseConfiguration, arguments.database)
    _requireAlias(databaseConfiguration, arguments.target)

    if arguments.target == arguments.database:
        raise UsageError('--target must differ from --database: a subset is loaded into another database, not over its source')

    with Database(connectionSettings=databaseConfiguration[arguments.database]) as database:
        foreignKeys = database.getForeignKeys()

        try:
            plan = planSubset(foreignKeys, root=arguments.root, where=arguments.where, followChildren=not arguments.no_children,
                              ignore=arguments.ignore_foreign_key or [])
        except SubsetError as error:
            raise UsageError(str(error)) from error

        log.logging.info('Subset covers {} table(s): {}'.format(len(plan.tables), ', '.join(plan.tables)))
        for foreignKey in plan.ignored:
            log.logging.warning('Ignoring foreign key {}.{} -> {}; rows may reference ones outside the subset'.format(
                foreignKey.table, ','.join(foreignKey.columns), foreignKey.referencedTable))

        drafts = []
        for table in plan.tables:
            proposal = proposeTable(database, table, sampleSize=arguments.sample, foreignKeys=foreignKeys) if arguments.mask else None
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
            'Generated by `lightweight-etl schema` on {} from {} ({}), for {} ({}).'.format(
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
                raise UsageError('could not create {} ({} created before it): {}: {}'.format(
                    statement.table, created, type(error).__name__, error)) from error
            log.logging.info('{}: created'.format(statement.table))
            created += 1

    print('schema applied to {}: {} table(s) created, {} already existed'.format(arguments.target, created, skipped))

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
                log.logging.error('{}: nothing was cleared -- {}: {}'.format(alias, type(error).__name__, error))
                return EXIT_JOBS_DID_NOT_SUCCEED

        for table, rows in cleared:
            log.logging.info('{}: emptied {} ({} row(s))'.format(alias, table, rows))
        print('{}: emptied {}'.format(alias, ', '.join(table for table, _ in cleared)))

    if not arguments.dry_run:
        print('Run the jobs with --force, so a `refresh` window cannot leave a cleared table empty.')

    return EXIT_SUCCESS


def _commandJobs(arguments: argparse.Namespace, log: Log) -> int:
    """Prints the graph as the scheduler sees it right now, including which jobs
    are suppressed by their refresh window -- the fastest way to find out why a
    job isn't running.
    """

    jobsFile, _ = _loadDataJobs(arguments)
    memory = FileMemory(memoryFile=_resolveMemoryPath(arguments, log))
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


def _addCommonArguments(parser: argparse.ArgumentParser, jobs: bool = True) -> None:

    parser.add_argument('--config', help='directory holding jobs.yaml and database.yaml (default: ${} or ./configuration)'.format(
        CONFIG_DIRECTORY_VARIABLE))
    if jobs:
        parser.add_argument('--jobs', help='explicit path to the jobs file, overriding --config')
    parser.add_argument('--databases', help='explicit path to the database file, overriding --config')
    if jobs:
        parser.add_argument('--memory', help='path to the run-memory file (default: memory.yaml in the --config directory)')
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


def _addGeneratorArguments(parser: argparse.ArgumentParser) -> None:

    parser.add_argument('--sample', type=int, default=1000, help='rows sampled per table to classify columns (default: 1000)')
    parser.add_argument('--key-variable', default='MASKING_KEY', help='environment variable the generated jobs read the masking key from')
    parser.add_argument('--chunk-size', type=int, default=5000, help='chunkSize for the generated jobs (default: 5000)')
    parser.add_argument('--output', help='write the generated jobs here instead of stdout; must not already exist')


def _buildParser() -> argparse.ArgumentParser:

    parser = argparse.ArgumentParser(prog='lightweight-etl', description='Move, mask and subset data between databases, with jobs defined in YAML.')
    subparsers = parser.add_subparsers(dest='command', required=True)

    runParser = subparsers.add_parser('run', help='run data jobs')
    _addCommonArguments(runParser)
    _addRunArguments(runParser)
    runParser.add_argument('--manifest', help='write a JSON record of what was masked, how, and under which key fingerprint')
    runParser.set_defaults(handler=_commandRun)

    validateParser = subparsers.add_parser('validate', help='check configuration offline, without connecting to anything')
    _addCommonArguments(validateParser)
    validateParser.set_defaults(handler=_commandValidate)

    jobsParser = subparsers.add_parser('jobs', help='show the job graph and which jobs are due')
    _addCommonArguments(jobsParser)
    jobsParser.set_defaults(handler=_commandJobs)

    discoverParser = subparsers.add_parser('discover', help='propose a masking policy for tables, from their schema and a sample')
    _addCommonArguments(discoverParser, jobs=False)
    discoverParser.add_argument('--database', required=True, help='the alias to read from')
    discoverParser.add_argument('--table', action='append', required=True, help='a table to propose a policy for (repeatable)')
    discoverParser.add_argument('--target', help='the alias the generated jobs load into (default: --database, masking in place)')
    _addGeneratorArguments(discoverParser)
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

    for name in ('job', 'force', 'workers', 'dry_run', 'forever', 'manifest', 'jobs', 'memory', 'yes'):
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


if __name__ == '__main__':
    sys.exit(main())
