from __future__ import annotations

import os
import re
import shlex
import subprocess
from enum import Enum
from typing import Annotated, Any, Dict, List, Mapping, Optional, Sequence, Set, Type, TypeVar, Union

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, SecretStr, ValidationError, field_validator, model_validator

from .masking import validateColumnPolicy, validateKey


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

# ${NAME}, ${NAME:-default} or ${file:/path}; $${...} escapes one.
ENVIRONMENT_VARIABLE = re.compile(r'(\$?)\$\{(?:file:([^}]+)|([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?)\}')


def _expand(value: Any, missing: List[str]) -> Any:
    """Recursive worker: collects what couldn't be read rather than raising on the first."""

    if isinstance(value, dict):
        return {key: _expand(item, missing) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand(item, missing) for item in value]
    if not isinstance(value, str):
        return value

    def replace(match: Any) -> str:
        escape, path, name, default = match.groups()

        if escape:
            return match.group(0)[1:]

        if path is not None:
            try:
                with open(path) as file:
                    # Secret files usually end with a newline nobody meant as
                    # part of the secret.
                    return file.read().rstrip('\r\n')
            except OSError as error:
                missing.append('file {} ({})'.format(path, error.strerror or error))
                return ''

        if name in os.environ:
            return os.environ[name]
        if default is not None:
            return default

        missing.append('${}'.format(name))

        return ''

    return ENVIRONMENT_VARIABLE.sub(replace, value)


def expandEnvironmentVariables(value: Any) -> Any:
    """Recursively replace ${NAME} and ${file:/path} in a loaded configuration:

        password: ${PROD_DB_PASSWORD}
        port: ${PROD_DB_PORT:-5432}
        key: ${file:/run/secrets/masking-key}

    A file reference reads the file less a trailing newline, as mounted
    secrets are written. An unset variable with no default, or an unreadable
    file, raises ConfigurationError rather than expanding to an empty string,
    naming every one at once. Escape a literal ${...} as $${...}.
    """

    missing: List[str] = []
    expanded = _expand(value, missing)

    if missing:
        raise ConfigurationError(
            'configuration references value(s) that could not be read: {}. '
            'Set each variable, or give it a default with ${{NAME:-value}} (never for a secret).'.format(', '.join(sorted(set(missing)))))

    return expanded


class InsertStrategy(str, Enum):
    SWAP = 'swap'
    UPSERT = 'upsert'


# A plain SQL identifier -- what currentSchema is written into a session
# statement as, so nothing else may get through.
IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_$#]*$')

# The dialects that can change a session's current schema with a statement.
# SQL Server takes the default schema from the login, and MySQL, MariaDB and
# SQLite have no schema separate from the database.
CURRENT_SCHEMA_TYPES = frozenset({'postgresql', 'oracle'})


PASSWORD_COMMAND_TIMEOUT_SECONDS = 60


class PasswordCommandError(RuntimeError):
    """passwordCommand failed. Not a ConfigurationError: the usual cause -- a
    token service that's briefly unreachable -- is worth a retry.
    """


def splitPasswordCommand(command: Union[str, List[str]]) -> List[str]:
    """The program and arguments a passwordCommand runs. A list is taken as it
    is; a string is split like a shell would split it, but no shell is involved.

    Raises ValueError for a command with no program, or a string a shell
    couldn't split, such as one with an unterminated quote.
    """

    if isinstance(command, str):
        try:
            arguments = shlex.split(command)
        except ValueError as error:
            raise ValueError('passwordCommand cannot be split into a program and arguments: {}'.format(error)) from None
    else:
        arguments = list(command)

    if not arguments or not arguments[0].strip():
        raise ValueError('passwordCommand names no program to run')

    return arguments


def runPasswordCommand(command: Union[str, List[str]]) -> str:
    """Runs a passwordCommand and returns what it printed, stripped. The
    output, being the secret, never appears in an error.
    """

    try:
        arguments = splitPasswordCommand(command)
    except ValueError as error:
        raise ConfigurationError(str(error)) from None

    try:
        completed = subprocess.run(arguments, capture_output=True, text=True, timeout=PASSWORD_COMMAND_TIMEOUT_SECONDS)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise PasswordCommandError('passwordCommand {} could not run: {}'.format(arguments[0], error)) from None

    if completed.returncode != 0:
        raise PasswordCommandError('passwordCommand {} exited with status {}: {}'.format(
            arguments[0], completed.returncode, completed.stderr.strip()[-500:]))

    password = completed.stdout.strip()
    if not password:
        raise PasswordCommandError('passwordCommand {} printed nothing'.format(arguments[0]))

    return password


class DatabaseConnectionConfig(BaseModel):
    """One database connection. `password` is a SecretStr, and `options` --
    extra driver arguments, TLS above all -- are left out of the repr, since
    they can hold secrets too. `passwordCommand` runs at every connect, for
    expiring credentials such as IAM tokens.
    """

    type: DatabaseType
    database: str
    user: Optional[str] = None
    password: Optional[SecretStr] = None
    host: Optional[str] = None
    port: Optional[int] = None
    serviceName: Optional[str] = None
    sid: Optional[str] = None
    passwordCommand: Optional[Union[str, List[str]]] = None
    currentSchema: Optional[str] = None
    options: CleanedMapping = Field(default_factory=dict, repr=False)

    @model_validator(mode='after')
    def _checkCurrentSchema(self) -> 'DatabaseConnectionConfig':

        if self.currentSchema is None:
            return self

        if self.type.value not in CURRENT_SCHEMA_TYPES:
            raise ValueError('currentSchema is supported for {} only; for {}, qualify table names as schema.table instead'.format(
                ' and '.join(sorted(CURRENT_SCHEMA_TYPES)), self.type.value))

        if not IDENTIFIER.match(self.currentSchema):
            raise ValueError('currentSchema must be a plain identifier, got {!r}'.format(self.currentSchema))

        return self

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
        """Every dialect but sqlite, a local file, needs a server and a login."""

        if self.password is not None and self.passwordCommand is not None:
            raise ValueError('set password or passwordCommand, not both')

        if self.passwordCommand is not None:
            splitPasswordCommand(self.passwordCommand)

        if self.type != DatabaseType.SQLITE and (self.user is None or self.host is None or (self.password is None and not self.passwordCommand)):
            raise ValueError('user, host, and a password or passwordCommand are required for every database type except sqlite')

        return self


    def plainPassword(self) -> Optional[str]:
        """The password to connect with -- running passwordCommand, if that's
        how it is configured, so call it only when about to connect.
        """

        if self.passwordCommand:
            return runPasswordCommand(self.passwordCommand)

        return None if self.password is None else self.password.get_secret_value()


class BaseJobConfig(BaseModel):
    active: bool
    refresh: Optional[int] = None
    predecessors: CleanedStringList = Field(default_factory=list)


class MaskingConfig(BaseModel):
    """A job's masking policy, normalized here so a bad strategy or option
    fails `bauta validate` rather than a run.
    """

    key: SecretStr
    columns: Dict[str, Any]
    defaultStrategy: Optional[Any] = None

    @field_validator('key')
    @classmethod
    def _requireStrongKey(cls, key: SecretStr) -> SecretStr:

        validateKey(key.get_secret_value())

        return key


    @field_validator('columns')
    @classmethod
    def _validateColumns(cls, columns: Dict[str, Any]) -> Dict[str, Any]:

        normalized = {}
        problems = []
        folded: Dict[str, str] = {}

        for column, policy in columns.items():
            try:
                normalized[column] = validateColumnPolicy(policy)
            except ValueError as error:
                problems.append('{}: {}'.format(column, error))
            if column.upper() in folded:
                problems.append('{}: differs only in case from {} -- column names match case-insensitively'.format(column, folded[column.upper()]))
            folded[column.upper()] = column

        if problems:
            raise ValueError('; '.join(problems))

        return normalized


    @field_validator('defaultStrategy')
    @classmethod
    def _validateDefaultStrategy(cls, policy: Any) -> Any:

        return None if policy is None else validateColumnPolicy(policy)


class DataJobConfig(BaseJobConfig):
    sourceDatabase: str
    sourceQuery: str
    targetColumns: CleanedStringList = Field(default_factory=list)
    sourceQueryColumnTransforms: CleanedListMapping = Field(default_factory=dict)
    targetDatabase: str
    targetTableStage: Optional[str] = None
    targetTableFinal: str
    insertStrategy: InsertStrategy
    chunkSize: int = Field(ge=1)
    watermarkColumn: Optional[str] = None
    watermarkInitial: Optional[Any] = None
    retries: int = 0
    retryDelaySeconds: float = 5.0
    timeoutSeconds: Optional[float] = Field(default=None, gt=0)
    masking: Optional[MaskingConfig] = None
    preTargetAdhocQueries: CleanedStringList = Field(default_factory=list)
    postTargetAdhocQueries: CleanedStringList = Field(default_factory=list)

    @model_validator(mode='after')
    def _requireSeparateStageTable(self) -> 'DataJobConfig':
        """A stage table naming the target would have the target truncated
        before each load. Compared case-insensitively; a qualified and an
        unqualified name for the same table still slip through.
        """

        if self.targetTableStage is not None and self.targetTableStage.upper() == self.targetTableFinal.upper():
            raise ValueError('targetTableStage must be a different table from targetTableFinal: the stage table is emptied before each load')

        return self


    @model_validator(mode='after')
    def _requireStageTableForSwap(self) -> 'DataJobConfig':
        """A rename never moves a table between schemas, so a swap's stage
        table must share the target's.
        """

        if self.insertStrategy != InsertStrategy.SWAP:
            return self

        if not self.targetTableStage:
            raise ValueError('targetTableStage is required when insertStrategy is swap')

        stageSchema, finalSchema = (table.rpartition('.')[0].upper() for table in (self.targetTableStage, self.targetTableFinal))
        if stageSchema != finalSchema:
            raise ValueError('targetTableStage and targetTableFinal must be in the same schema for insertStrategy: swap, '
                             'since a rename cannot move a table between schemas')

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
        """watermarkColumn and the {{ watermark }} token need each other, and
        watermarkInitial is required: binding None would match no rows, forever.
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
        """A watermark needs upsert: a swap would replace the target with only
        the rows that changed.
        """

        if self.watermarkColumn and self.insertStrategy != InsertStrategy.UPSERT:
            raise ValueError('watermarkColumn requires insertStrategy: upsert -- swap would replace the whole target with only the rows that changed')

        return self


class NameRuleConfig(BaseModel):
    """A discovery.yaml rule on column names."""

    model_config = ConfigDict(extra='forbid')

    words: List[Annotated[str, Field(min_length=1)]] = Field(min_length=1)
    # A strategy name alone, or a mapping with a `strategy`, as in jobs.yaml.
    policy: Dict[str, Any]
    reason: Optional[str] = Field(default=None, min_length=1)

    @field_validator('words')
    @classmethod
    def _wordsHaveLetters(cls, words: List[str]) -> List[str]:

        empty = [word for word in words if not re.search(r'[A-Za-z0-9]', word)]
        if empty:
            raise ValueError('a word needs a letter or digit: {}'.format(', '.join(repr(word) for word in empty)))

        return words

    @field_validator('policy', mode='before')
    @classmethod
    def _policyIsValid(cls, policy: Any) -> Dict[str, Any]:

        return validateColumnPolicy(policy)


class ValueRuleConfig(BaseModel):
    """A discovery.yaml rule on sampled values: a regular expression the whole
    value must match.
    """

    model_config = ConfigDict(extra='forbid')

    pattern: str = Field(min_length=1)
    # A strategy name alone, or a mapping with a `strategy`, as in jobs.yaml.
    policy: Dict[str, Any]
    reason: Optional[str] = Field(default=None, min_length=1)

    @field_validator('pattern')
    @classmethod
    def _patternCompiles(cls, pattern: str) -> str:

        try:
            re.compile(pattern)
        except re.error as error:
            raise ValueError('not a valid regular expression: {}'.format(error)) from error

        return pattern

    @field_validator('policy', mode='before')
    @classmethod
    def _policyIsValid(cls, policy: Any) -> Dict[str, Any]:

        return validateColumnPolicy(policy)


class DiscoveryRulesFile(BaseModel):
    """discovery.yaml: rules of your own for what personal data looks like,
    checked before the built-in ones in builtinDiscovery.
    """

    model_config = ConfigDict(extra='forbid')

    builtins: bool = True
    exclude: List[str] = Field(default_factory=list)
    names: List[NameRuleConfig] = Field(default_factory=list)
    values: List[ValueRuleConfig] = Field(default_factory=list)
    personalTables: List[Annotated[str, Field(min_length=1)]] = Field(default_factory=list)

    @field_validator('exclude')
    @classmethod
    def _excludeNamesBuiltins(cls, exclude: List[str]) -> List[str]:

        from .builtinDiscovery import RULE_NAMES

        unknown = sorted(set(exclude) - RULE_NAMES)
        if unknown:
            raise ValueError('no built-in rule named {}. Built-in rules: {}'.format(', '.join(unknown), ', '.join(sorted(RULE_NAMES))))

        return exclude


class TableLocation(BaseModel):
    """A table in one of database.yaml's aliases, to keep run state, history or
    manifests in rather than a file. `table` defaults per use.
    """

    model_config = ConfigDict(extra='forbid')

    database: str = Field(min_length=1)
    table: Optional[str] = Field(default=None, min_length=1)


# A file, relative to the jobs file, or a table.
StorageLocation = Union[Annotated[str, Field(min_length=1)], TableLocation]


class DataJobsFile(BaseModel):
    workers: int = Field(ge=1)
    cycleSleepSeconds: float = 0.5
    # Where the CLI keeps run state, records history and writes the masking
    # manifest; command-line flags override each. See cli._resolveLocation.
    memory: Optional[StorageLocation] = None
    history: Optional[StorageLocation] = None
    manifest: Optional[StorageLocation] = None
    jobs: Dict[str, DataJobConfig]

    def tableLocations(self) -> Dict[str, TableLocation]:
        """The settings that name a table, by setting."""

        return {name: location for name, location in (('memory', self.memory), ('history', self.history), ('manifest', self.manifest))
                if isinstance(location, TableLocation)}


def findCycle(predecessors: Mapping[str, Sequence[str]]) -> Optional[List[str]]:
    """A cycle in a job -> predecessors graph, as a path that ends where it
    starts, or None. Predecessors that aren't keys are ignored.
    """

    state: Dict[str, int] = {}
    path: List[str] = []

    def visit(job: str) -> Optional[List[str]]:
        state[job] = 1
        path.append(job)
        for predecessor in predecessors[job]:
            if predecessor not in predecessors:
                continue
            if state.get(predecessor) == 1:
                return path[path.index(predecessor):] + [predecessor]
            if predecessor not in state:
                found = visit(predecessor)
                if found:
                    return found
        path.pop()
        state[job] = 2
        return None

    for job in sorted(predecessors):
        if job not in state:
            found = visit(job)
            if found:
                return found

    return None


T = TypeVar('T', bound=BaseModel)


class Configuration:
    """Validates already-loaded configuration data, from wherever the caller
    loaded it.
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
    def validateDiscoveryRules(rawConfiguration: Any, sourceDescription: str = 'discovery.yaml') -> DiscoveryRulesFile:
        """An empty file is no rules of your own, not an error."""

        return Configuration._validate(DiscoveryRulesFile, rawConfiguration or {}, sourceDescription)


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

        cycle = findCycle({jobName: job.predecessors for jobName, job in jobs.items()})
        if cycle:
            problems.append('predecessors form a cycle, so none of these jobs could ever start: {}'.format(' -> '.join(cycle)))

        if problems:
            raise ConfigurationError('Invalid job graph:\n' + '\n'.join(problems))
