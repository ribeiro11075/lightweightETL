from __future__ import annotations

import base64
import datetime
import hashlib
import random
from enum import Enum
from typing import Any, Dict, List, Sequence, Tuple


class ColumnCategory(str, Enum):
    NUMBER = 'number'
    DATE = 'date'
    TEXT = 'text'


_MYSQL_NUMBER_TYPES = {'INT', 'BIGINT'}
_MYSQL_DATE_TYPES = {'DATETIME', 'TIMESTAMP', 'DATE'}
_MYSQL_TEXT_TYPES = {'TEXT', 'VARCHAR', 'CHAR'}

_POSTGRESQL_NUMBER_OIDS = {20, 21, 23}
_POSTGRESQL_DATE_OIDS = {1114, 1018}
_POSTGRESQL_TEXT_OIDS = {1043, 18, 25}


class Scramble:

    def __init__(self, job: str, data: List[Tuple[Any, ...]], columns: List[str], dataTypes: List[Any],
                 defaultColumnValues: Dict[str, Any] = {}, identifierColumns: List[str] = [],
                 scrambleColumns: List[str] = [], randomColumns: List[str] = [], allDataRandom: bool = False,
                 randomSalt: str = 'w3aK7ess') -> None:
        """dataTypes are mysql type name strings (e.g. "VARCHAR") for a mysql source,
        or postgresql OIDs (ints) for a postgresql source -- categorizing by
        dataType.upper() raises AttributeError for the OID case, which is how the
        two are told apart below. Needs to be fixed better for PostgreSQL.
        """
        self.job = job
        self.data = data
        self.columns = columns
        self.dataTypes = dataTypes
        self.defaultColumnValues = defaultColumnValues
        self.identifierColumns = identifierColumns
        self.scrambleColumns = scrambleColumns
        self.randomColumns = randomColumns
        self.allDataRandom = allDataRandom
        self.randomSalt = randomSalt

        self.dataZip = zip(*self.data)
        self.dataDict: Dict[str, Sequence[Any]] = {}
        self.numberRecords = len(self.data)

        try:
            self.columnCategories: Dict[str, ColumnCategory] = {
                column: ColumnCategory.NUMBER for column, dataType in zip(columns, dataTypes) if dataType.upper() in _MYSQL_NUMBER_TYPES
                }
            self.columnCategories.update({column: ColumnCategory.DATE for column, dataType in zip(columns, dataTypes) if dataType.upper() in _MYSQL_DATE_TYPES})
            self.columnCategories.update({column: ColumnCategory.TEXT for column, dataType in zip(columns, dataTypes) if dataType.upper() in _MYSQL_TEXT_TYPES})
        except AttributeError:
            self.columnCategories = {
                column: ColumnCategory.NUMBER for column, dataType in zip(columns, dataTypes) if dataType in _POSTGRESQL_NUMBER_OIDS
                }
            self.columnCategories.update({column: ColumnCategory.DATE for column, dataType in zip(columns, dataTypes) if dataType in _POSTGRESQL_DATE_OIDS})
            self.columnCategories.update({column: ColumnCategory.TEXT for column, dataType in zip(columns, dataTypes) if dataType in _POSTGRESQL_TEXT_OIDS})

    def hashString(self, nonce: int) -> bytes:
        """A fresh hasher per call, keyed by nonce, so output varies deterministically
        per record.
        """
        hasher = hashlib.sha1()
        hasher.update('{}{}'.format(self.randomSalt, nonce).encode('utf-8'))

        return base64.urlsafe_b64encode(hasher.digest())


    def _createRandomTextColumn(self, column: str, data: Sequence[Any]) -> None:
        textLengths = [len(x) for x in data if x is not None]

        if textLengths:
            maxLength = max(textLengths)
            randomData = tuple(self.hashString(nonce=index)[0:maxLength].decode('ascii') for index in range(self.numberRecords))
            self.dataDict[column] = randomData
        else:
            self.dataDict[column] = data


    def _createRandomDateColumn(self, column: str, data: Sequence[Any]) -> None:
        dataFilteredNone = [x for x in data if x is not None]

        if dataFilteredNone:
            minDate = min(dataFilteredNone)
            maxDate = max(dataFilteredNone)
            delta = (maxDate - minDate).total_seconds()

            if minDate == maxDate:
                self.dataDict[column] = data
            else:
                randomData = tuple(minDate + datetime.timedelta(seconds=random.randint(0, int(delta))) for _ in range(self.numberRecords))
                self.dataDict[column] = randomData

        else:
            self.dataDict[column] = data


    def _createRandomNumberColumn(self, column: str, data: Sequence[Any]) -> None:
        dataFilteredNone = [x for x in data if x is not None]

        if dataFilteredNone:
            maxNumber = max(dataFilteredNone)
            minNumber = min(dataFilteredNone)

            if maxNumber == minNumber:
                self.dataDict[column] = data
            else:
                randomData = tuple(random.randint(minNumber, maxNumber) for _ in range(self.numberRecords))
                self.dataDict[column] = randomData

        else:
            self.dataDict[column] = data


    def _scrambleColumn(self, column: str, data: Sequence[Any]) -> None:
        dataList = list(data)
        random.shuffle(dataList)
        self.dataDict[column] = dataList


    def _iterateColumns(self) -> None:
        for column, data in zip(self.columns, self.dataZip):

            if column in self.defaultColumnValues.keys():
                self.dataDict[column] = (self.defaultColumnValues[column],) * self.numberRecords

            elif column in self.identifierColumns:
                self.dataDict[column] = data

            elif column in self.scrambleColumns:
                self._scrambleColumn(column=column, data=data)

            elif column in self.randomColumns or self.allDataRandom:

                columnCategory = self.columnCategories.get(column)

                if columnCategory == ColumnCategory.NUMBER:
                    self._createRandomNumberColumn(column=column, data=data)
                elif columnCategory == ColumnCategory.DATE:
                    self._createRandomDateColumn(column=column, data=data)
                else:
                    self._createRandomTextColumn(column=column, data=data)

            else:
                self._scrambleColumn(column=column, data=data)


    def scramble(self) -> None:
        if not self.numberRecords:
            self.dataScrambled = []
            return

        self._iterateColumns()
        self.dataScrambled = list(zip(*(self.dataDict[column] for column in self.columns)))
