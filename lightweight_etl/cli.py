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
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import yaml

from .configuration import Configuration, ConfigurationError, DatabaseConnectionConfig, DataJobsFile, ScrambleJobsFile, expandEnvironmentVariables
from .database import Database
from .dependencyGraph import DependencyGraph
from .log import LOGGER_NAME, Log
from .memory import FileMemory
from .runner import RunResult, runDataJobs, runScrambleJobs

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
    themselves -- see the README.
    """

    try:
        with open(path) as file:
            return expandEnvironmentVariables(yaml.load(file, Loader=yaml.FullLoader))
    except FileNotFoundError as error:
        raise UsageError('no such file: {}'.format(path)) from error
    except yaml.YAMLError as error:
        raise UsageError('{} is not valid YAML: {}'.format(path, error)) from error


def _resolveConfigurationPaths(arguments: argparse.Namespace, jobsFileName: str) -> Tuple[Path, Path]:
    """Explicit --jobs/--databases win; otherwise both come from a config directory.

    The directory falls back to $LIGHTWEIGHT_ETL_CONFIG and then to ./configuration,
    so the common case is a bare `lightweight-etl run` -- passing two paths on
    every invocation gets old quickly.
    """

    directory = arguments.config or os.environ.get(CONFIG_DIRECTORY_VARIABLE) or 'configuration'
    configDirectory = Path(directory)

    jobsPath = Path(arguments.jobs) if arguments.jobs else configDirectory / jobsFileName
    databasesPath = Path(arguments.databases) if arguments.databases else configDirectory / 'database.yaml'

    return jobsPath, databasesPath


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


def _loadDataJobs(arguments: argparse.Namespace) -> Tuple[DataJobsFile, Dict[str, DatabaseConnectionConfig]]:

    jobsPath, databasesPath = _resolveConfigurationPaths(arguments, jobsFileName='jobs.yaml')
    databaseConfiguration = Configuration.validateDatabaseConfiguration(_loadYaml(databasesPath))
    jobsFile = Configuration.validateJobConfiguration(_loadYaml(jobsPath), DataJobsFile)
    Configuration.validateJobGraph(jobsFile.jobs, databaseAliases=set(databaseConfiguration))

    return jobsFile, databaseConfiguration


def _loadScrambleJobs(arguments: argparse.Namespace) -> Tuple[ScrambleJobsFile, Dict[str, DatabaseConnectionConfig]]:

    jobsPath, databasesPath = _resolveConfigurationPaths(arguments, jobsFileName='scramble.yaml')
    databaseConfiguration = Configuration.validateDatabaseConfiguration(_loadYaml(databasesPath))
    jobsFile = Configuration.validateJobConfiguration(_loadYaml(jobsPath), ScrambleJobsFile)
    Configuration.validateJobGraph(jobsFile.jobs)

    return jobsFile, databaseConfiguration


def _selectJobs(jobs: Dict[str, Any], requested: Optional[List[str]], log: Log) -> Dict[str, Any]:
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


def _applyJobSelection(jobsFile: Any, arguments: argparse.Namespace, log: Log) -> Any:
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

    if result.succeeded:
        return EXIT_SUCCESS

    return EXIT_JOBS_DID_NOT_SUCCEED


def _commandRun(arguments: argparse.Namespace, log: Log) -> int:

    jobsFile, databaseConfiguration = _loadDataJobs(arguments)
    jobsFile = _applyJobSelection(jobsFile, arguments, log)

    if arguments.dry_run:
        return _dryRunDataJobs(jobsFile, databaseConfiguration, log)

    memoryPath = Path(arguments.memory) if arguments.memory else Path('memory.yaml')
    result = runDataJobs(jobsFile=jobsFile, databaseConfiguration=databaseConfiguration, logFile=arguments.log,
                          memory=FileMemory(memoryFile=memoryPath), runForever=arguments.forever,
                          logLevel=getattr(logging, arguments.log_level.upper()), logFormat=arguments.log_format)

    return _reportRun(result, log)


def _commandScramble(arguments: argparse.Namespace, log: Log) -> int:

    jobsFile, databaseConfiguration = _loadScrambleJobs(arguments)
    jobsFile = _applyJobSelection(jobsFile, arguments, log)

    if arguments.dry_run:
        return _dryRunScrambleJobs(jobsFile, databaseConfiguration, log)

    result = runScrambleJobs(jobsFile=jobsFile, databaseConfiguration=databaseConfiguration, logFile=arguments.log,
                              runForever=arguments.forever, logLevel=getattr(logging, arguments.log_level.upper()),
                              logFormat=arguments.log_format)

    return _reportRun(result, log)


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

    if problems:
        for problem in problems:
            log.logging.error(problem)
        return EXIT_JOBS_DID_NOT_SUCCEED

    print('dry run passed: {} database alias(es), {} job(s), no rows moved'.format(len(aliases), len(jobsFile.jobs)))

    return EXIT_SUCCESS


def _dryRunScrambleJobs(jobsFile: ScrambleJobsFile, databaseConfiguration: Dict[str, DatabaseConnectionConfig], log: Log) -> int:

    problems: List[str] = []

    for name, job in jobsFile.jobs.items():
        try:
            with Database(connectionSettings=databaseConfiguration[job.database]) as database:
                columns = database.getAllColumnNames(table=job.table)
                log.logging.info('{}: {} has {} column(s)'.format(name, job.table, len(columns)))
        except Exception as error:
            problems.append('{}: {} is not readable -- {}: {}'.format(name, job.table, type(error).__name__, error))

    if problems:
        for problem in problems:
            log.logging.error(problem)
        return EXIT_JOBS_DID_NOT_SUCCEED

    print('dry run passed: {} job(s), nothing scrambled'.format(len(jobsFile.jobs)))

    return EXIT_SUCCESS


def _commandJobs(arguments: argparse.Namespace, log: Log) -> int:
    """Prints the graph as the scheduler sees it right now, including which jobs
    are suppressed by their refresh window -- the fastest way to find out why a
    job isn't running.
    """

    jobsFile, _ = _loadDataJobs(arguments)
    memoryPath = Path(arguments.memory) if arguments.memory else Path('memory.yaml')
    memory = FileMemory(memoryFile=memoryPath)
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


def _addCommonArguments(parser: argparse.ArgumentParser) -> None:

    parser.add_argument('--config', help='directory holding jobs.yaml/scramble.yaml and database.yaml (default: ${} or ./configuration)'.format(
        CONFIG_DIRECTORY_VARIABLE))
    parser.add_argument('--jobs', help='explicit path to the jobs file, overriding --config')
    parser.add_argument('--databases', help='explicit path to the database file, overriding --config')
    parser.add_argument('--memory', help='path to the run-memory file (default: ./memory.yaml)')
    parser.add_argument('--log', help='also write logs to this file (logs always go to stderr unless --quiet)')
    parser.add_argument('--log-level', default='info', choices=['debug', 'info', 'warning', 'error'], help='default: info')
    parser.add_argument('--log-format', default='text', choices=['text', 'json'],
                        help='json emits one object per record, carrying job/status/rowCount as fields a log collector can filter and alert on')
    parser.add_argument('--quiet', action='store_true', help='do not log to stderr')


def _addRunArguments(parser: argparse.ArgumentParser) -> None:

    parser.add_argument('--forever', action='store_true',
                        help='keep running, honoring each job\'s refresh window. Prefer a single run from cron or a CronJob; '
                             'use this only for freshness below cron\'s one-minute floor, or where there is no scheduler')
    parser.add_argument('--job', action='append', help='run only this job (repeatable). Implies --force, and does NOT run its predecessors')
    parser.add_argument('--force', action='store_true', help='ignore refresh windows')
    parser.add_argument('--workers', type=int, help='override the worker count from configuration')
    parser.add_argument('--dry-run', action='store_true', help='check connections, target tables and primary keys without moving any rows')


def _buildParser() -> argparse.ArgumentParser:

    parser = argparse.ArgumentParser(prog='lightweight-etl', description='Run ETL and table-masking jobs defined in YAML.')
    subparsers = parser.add_subparsers(dest='command', required=True)

    runParser = subparsers.add_parser('run', help='run data jobs')
    _addCommonArguments(runParser)
    _addRunArguments(runParser)
    runParser.set_defaults(handler=_commandRun)

    scrambleParser = subparsers.add_parser('scramble', help='run table-masking jobs (destructive: rewrites rows in place)')
    _addCommonArguments(scrambleParser)
    _addRunArguments(scrambleParser)
    scrambleParser.set_defaults(handler=_commandScramble)

    validateParser = subparsers.add_parser('validate', help='check configuration offline, without connecting to anything')
    _addCommonArguments(validateParser)
    validateParser.set_defaults(handler=_commandValidate)

    jobsParser = subparsers.add_parser('jobs', help='show the job graph and which jobs are due')
    _addCommonArguments(jobsParser)
    jobsParser.set_defaults(handler=_commandJobs)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:

    parser = _buildParser()
    arguments = parser.parse_args(argv)

    for name in ('job', 'force', 'workers', 'dry_run', 'forever'):
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
