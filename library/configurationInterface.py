from __future__ import annotations

from enum import Enum
from typing import Annotated, Any, Dict, List, Optional, Set, Type, TypeVar

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


class InsertStrategy(str, Enum):
    SWAP = 'swap'
    UPSERT = 'upsert'


class ConfigurationError(Exception):
    """Raised when user-supplied YAML configuration fails validation."""


class DatabaseConnectionConfig(BaseModel):
    type: DatabaseType
    user: str
    password: str
    database: str
    host: str
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


class BaseJobConfig(BaseModel):
    active: bool
    predecessors: CleanedStringList = Field(default_factory=list)
    refresh: Optional[int] = None


class DataJobConfig(BaseJobConfig):
    sourceDatabase: str
    targetDatabase: str
    insertStrategy: InsertStrategy
    chunkSize: int
    targetTableStage: Optional[str] = None
    targetTableFinal: str
    columnTransforms: CleanedListMapping = Field(default_factory=dict)
    preTargetAdhocQueries: CleanedStringList = Field(default_factory=list)
    postTargetAdhocQueries: CleanedStringList = Field(default_factory=list)
    sourceQuery: str

    @model_validator(mode='after')
    def _requireStageTableForSwap(self) -> 'DataJobConfig':

        if self.insertStrategy == InsertStrategy.SWAP and not self.targetTableStage:
            raise ValueError('targetTableStage is required when insertStrategy is swap')

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
    jobs: Dict[str, DataJobConfig]


class ScrambleJobsFile(BaseModel):
    workers: int
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
    def validateJobGraph(jobs: Dict[str, BaseJobConfig], databaseAliases: Optional[Set[str]] = None) -> None:

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
