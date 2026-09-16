from __future__ import annotations

import os
import re
from enum import Enum
from typing import Annotated, Any, Dict, List, Mapping, Optional, Set, Type, TypeVar

from pydantic import BaseModel, BeforeValidator, Field, ValidationError, model_validator


def _dropNoneListItems(value: Any) -> Any:
    """YAML's "key:\\n-\\n" idiom (an empty list item) parses to [None] -- treat
    that, and a bare `~`/omitted key, as an empty list rather than a validation error.
    """
    if value is None:
        return []
    if isinstance(value, list):
        return [item for item in value if item is not None]

    return value


def _dropNoneMappingEntries(value: Any) -> Any:
    if value is None:
        return {}
    if isinstance(value, dict):
        return {key: item for key, item in value.items() if item is not None}

    return value


def _dropNoneMappingListItems(value: Any) -> Any:
    cleaned = _dropNoneMappingEntries(value)

    if isinstance(cleaned, dict):
        return {key: (_dropNoneListItems(item) if isinstance(item, list) else item) for key, item in cleaned.items()}

    return cleaned


CleanedStringList = Annotated[List[str], BeforeValidator(_dropNoneListItems)]
CleanedMapping = Annotated[Dict[str, Any], BeforeValidator(_dropNoneMappingEntries)]
CleanedListMapping = Annotated[Dict[str, List[str]], BeforeValidator(_dropNoneMappingListItems)]


class DatabaseType(str, Enum):
    ORACLE = 'oracle'
    MYSQL = 'mysql'
    POSTGRESQL = 'postgresql'
    MSSQL = 'mssql'
    SQLITE = 'sqlite'
    MARIADB = 'mariadb'


class ConfigurationError(Exception):
    """Raised when user-supplied YAML configuration fails validation."""


WATERMARK_PLACEHOLDER = re.compile(r'\{\{\s*watermark\s*\}\}')

# ${NAME} or ${NAME:-default}. A doubled $$ escapes the whole construct, which
# matters because a sourceQuery is arbitrary SQL -- PostgreSQL's dollar-quoting
# ($$body$$) is untouched here since it is never followed by a brace.
ENVIRONMENT_VARIABLE = re.compile(r'(\$?)\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}')


def _expand(value: Any, missing: List[str]) -> Any:
    """Recursive worker: collects unset names rather than raising on the first."""

    if isinstance(value, dict):
        return {key: _expand(item, missing) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand(item, missing) for item in value]
    if not isinstance(value, str):
        return value

    def replace(match: Any) -> str:
        escape, name, default = match.group(1), match.group(2), match.group(3)

        if escape:
            return match.group(0)[1:]
        if name in os.environ:
            return os.environ[name]
        if default is not None:
            return default

        missing.append(name)

        return ''

    return ENVIRONMENT_VARIABLE.sub(replace, value)


def expandEnvironmentVariables(value: Any) -> Any:
    """Recursively replace ${NAME} in a loaded configuration with the environment.

    This is what keeps credentials out of the YAML that sits next to your job
    definitions, which is the first thing a security review objects to:

        password: ${PROD_DB_PASSWORD}
        port: ${PROD_DB_PORT:-5432}

    An unset variable with no default raises ConfigurationError rather than
    expanding to an empty string. A blank password that fails at connect time
    with the database's own unhelpful error is a much worse outcome than
    refusing to start, and an empty host would silently connect somewhere
    unintended. Every unset name in the whole document is reported at once, the
    way validation errors are here -- fixing them one run at a time is miserable.

    Defaults exist for the values that are awkward without them: ports, hosts,
    schema names. Don't give a secret a default; that just moves the credential
    back into the file.

    Escape a literal ${...} as $${...}. A sourceQuery is arbitrary SQL and may
    legitimately contain the sequence. PostgreSQL's dollar-quoting ($$body$$) is
    never followed by a brace, so it passes through untouched either way.
    """

    missing: List[str] = []
    expanded = _expand(value, missing)

    if missing:
        raise ConfigurationError(
            'configuration references environment variable(s) that are not set: {}. '
            'Set them, or give a default with ${{NAME:-value}} (never for a secret).'.format(', '.join(sorted(set(missing)))))

    return expanded


class InsertStrategy(str, Enum):
    SWAP = 'swap'
    UPSERT = 'upsert'


class DatabaseConnectionConfig(BaseModel):
    type: DatabaseType
    database: str
    user: Optional[str] = None
    password: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    serviceName: Optional[str] = None
    sid: Optional[str] = None

    @model_validator(mode='after')
    def _requireOracleIdentifier(self) -> 'DatabaseConnectionConfig':
        """Oracle connections need exactly one of serviceName/sid to build a DSN;
        without this check, connect() would silently never assign self.connection.
        """

        if self.type == DatabaseType.ORACLE and not (bool(self.serviceName) ^ bool(self.sid)):
            raise ValueError('oracle connections require exactly one of serviceName or sid')

        return self


    @model_validator(mode='after')
    def _requireNetworkCredentialsExceptSqlite(self) -> 'DatabaseConnectionConfig':
        """Every dialect but sqlite connects over the network and authenticates --
        sqlite is a local file (`database` holds its path, or ":memory:") with no
        server, user, or password to speak of.
        """

        if self.type != DatabaseType.SQLITE and (self.user is None or self.password is None or self.host is None):
            raise ValueError('user, password, and host are required for every database type except sqlite')

        return self


class BaseJobConfig(BaseModel):
    active: bool
    refresh: Optional[int] = None
    predecessors: CleanedStringList = Field(default_factory=list)


class DataJobConfig(BaseJobConfig):
    sourceDatabase: str
    sourceQuery: str
    targetColumns: CleanedStringList = Field(default_factory=list)
    sourceQueryColumnTransforms: CleanedListMapping = Field(default_factory=dict)
    targetDatabase: str
    targetTableStage: Optional[str] = None
    targetTableFinal: str
    insertStrategy: InsertStrategy
    chunkSize: int
    watermarkColumn: Optional[str] = None
    watermarkInitial: Optional[Any] = None
    retries: int = 0
    retryDelaySeconds: float = 5.0
    preTargetAdhocQueries: CleanedStringList = Field(default_factory=list)
    postTargetAdhocQueries: CleanedStringList = Field(default_factory=list)

    @model_validator(mode='after')
    def _requireStageTableForSwap(self) -> 'DataJobConfig':

        if self.insertStrategy == InsertStrategy.SWAP and not self.targetTableStage:
            raise ValueError('targetTableStage is required when insertStrategy is swap')

        return self


    @model_validator(mode='after')
    def _requireNonNegativeRetries(self) -> 'DataJobConfig':

        if self.retries < 0:
            raise ValueError('retries cannot be negative')
        if self.retryDelaySeconds < 0:
            raise ValueError('retryDelaySeconds cannot be negative')

        return self


    @model_validator(mode='after')
    def _requireCoherentWatermarkConfiguration(self) -> 'DataJobConfig':
        """watermarkColumn and the {{ watermark }} token in sourceQuery are two
        halves of one feature and neither is any use alone: a column with no
        token extracts everything and then advances a watermark nobody filtered
        on, and a token with no column has no value to bind or to carry forward.

        watermarkInitial is required because the first run has no stored
        watermark to bind, and there is no safe value to invent -- None would
        make the predicate match nothing on most dialects, so the job would
        quietly load zero rows forever.
        """

        hasPlaceholder = bool(WATERMARK_PLACEHOLDER.search(self.sourceQuery))

        if self.watermarkColumn and not hasPlaceholder:
            raise ValueError('watermarkColumn is set but sourceQuery has no {{ watermark }} placeholder to bind it into')

        if hasPlaceholder and not self.watermarkColumn:
            raise ValueError('sourceQuery has a {{ watermark }} placeholder but watermarkColumn is not set')

        if self.watermarkColumn and self.watermarkInitial is None:
            raise ValueError('watermarkInitial is required when watermarkColumn is set -- the first run has no stored watermark to bind')

        return self


    @model_validator(mode='after')
    def _rejectWatermarkWithSwap(self) -> 'DataJobConfig':
        """swap replaces targetTableFinal wholesale with the stage table's
        contents. An incremental extract only ever stages the rows that changed,
        so swapping one in would discard every row that didn't -- silently
        deleting most of the target on the first incremental run.

        upsert is the only strategy that composes with a watermark, and not by
        accident: it's also what makes re-reading an overlap window (the standard
        guard against missing rows committed by in-flight transactions) safe to
        do at all.
        """

        if self.watermarkColumn and self.insertStrategy != InsertStrategy.UPSERT:
            raise ValueError('watermarkColumn requires insertStrategy: upsert -- swap would replace the whole target with only the rows that changed')

        return self


class ScrambleJobConfig(BaseJobConfig):
    database: str
    table: str
    defaultColumnValues: CleanedMapping = Field(default_factory=dict)
    identifierColumns: CleanedStringList = Field(default_factory=list)
    scrambleColumns: CleanedStringList = Field(default_factory=list)
    randomColumns: CleanedStringList = Field(default_factory=list)
    randomSalt: str
    allDataRandom: bool = False
    preTargetAdhocQueries: CleanedStringList = Field(default_factory=list)
    postTargetAdhocQueries: CleanedStringList = Field(default_factory=list)


class DataJobsFile(BaseModel):
    workers: int
    cycleSleepSeconds: float = 0.5
    jobs: Dict[str, DataJobConfig]


class ScrambleJobsFile(BaseModel):
    workers: int
    cycleSleepSeconds: float = 0.5
    jobs: Dict[str, ScrambleJobConfig]


T = TypeVar('T', bound=BaseModel)


class Configuration:
    """Validates already-loaded configuration data.

    This is deliberately I/O-free: it doesn't know or care whether the raw dict
    came from a YAML file, JSON, environment variables, or a database -- the
    caller loads it however they want and hands over plain dicts.
    """

    @staticmethod
    def _validate(schema: Type[T], rawConfiguration: Any, sourceDescription: str) -> T:

        try:
            return schema.model_validate(rawConfiguration)
        except ValidationError as error:
            messages = [f'{".".join(str(part) for part in issue["loc"])}: {issue["msg"]}' for issue in error.errors()]
            raise ConfigurationError(f'Invalid configuration in {sourceDescription}:\n' + '\n'.join(messages)) from error


    @staticmethod
    def validateDatabaseConfiguration(rawConfiguration: Dict[str, Any]) -> Dict[str, DatabaseConnectionConfig]:

        return {
            alias: Configuration._validate(DatabaseConnectionConfig, connectionSettings, f'database configuration -> {alias}')
            for alias, connectionSettings in (rawConfiguration or {}).items()
            }


    @staticmethod
    def validateJobConfiguration(rawConfiguration: Any, schema: Type[T]) -> T:

        return Configuration._validate(schema, rawConfiguration, schema.__name__)


    @staticmethod
    def validateJobGraph(jobs: Mapping[str, BaseJobConfig], databaseAliases: Optional[Set[str]] = None) -> None:

        problems: List[str] = []

        for jobName, job in jobs.items():

            for predecessor in job.predecessors:
                if predecessor not in jobs:
                    problems.append(f'{jobName}: predecessor "{predecessor}" is not a known job')

            if databaseAliases is not None and isinstance(job, DataJobConfig):
                if job.sourceDatabase not in databaseAliases:
                    problems.append(f'{jobName}: sourceDatabase "{job.sourceDatabase}" is not a known database alias')
                if job.targetDatabase not in databaseAliases:
                    problems.append(f'{jobName}: targetDatabase "{job.targetDatabase}" is not a known database alias')

        if problems:
            raise ConfigurationError('Invalid job graph:\n' + '\n'.join(problems))
