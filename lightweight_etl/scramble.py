from __future__ import annotations

import base64
import datetime
import hashlib
import random
from typing import Any, Dict, List, Sequence, Tuple

from .databaseDialects import ColumnCategory


class Scramble:
    """Pure row/column-level scrambling logic -- knows nothing about any
    particular database. It works entirely off of `data`/`columns` (whatever a
    caller already extracted) and `columnCategories`, a plain
    Dict[str, ColumnCategory] describing which columns are numbers/dates/text --
    mapping a database's own raw column types to that is DatabaseDialect's job
    (see databaseDialects.py's columnCategory()), not this class's.
    """

    def __init__(self, job: str, data: List[Tuple[Any, ...]], columns: List[str], columnCategories: Dict[str, ColumnCategory] = {},
                 defaultColumnValues: Dict[str, Any] = {}, identifierColumns: List[str] = [],
                 scrambleColumns: List[str] = [], randomColumns: List[str] = [], allDataRandom: bool = False,
                 randomSalt: str = 'w3aK7ess') -> None:
        self.job = job
        self.data = data
        self.columns = columns
        self.columnCategories = columnCategories
        self.defaultColumnValues = defaultColumnValues
        self.identifierColumns = identifierColumns
        self.scrambleColumns = scrambleColumns
        self.randomColumns = randomColumns
        self.allDataRandom = allDataRandom
        self.randomSalt = randomSalt

        self.dataZip = zip(*self.data)
        self.dataDict: Dict[str, Sequence[Any]] = {}
        self.numberRecords = len(self.data)

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
