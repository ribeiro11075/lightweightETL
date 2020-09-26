import random
import datetime
import hashlib
import base64


class SCRAMBLE():

	def __init__(self, job, data, columns, dataTypes, defaultColumnValues={}, identifierColumns=[], scrambleColumns=[], randomColumns=[], allDataRandom=False, randomSalt='w3aK7ess'):
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
		self.hasher = hashlib.sha1()

		self.dataZip = zip(*self.data)
		self.dataDict = {}
		self.numberRecords = len(self.data)

		# Needs to be fixed better for PostgreSQL
		try:
			self.numberColumns = list(i for i, j in zip(columns, dataTypes) if j.upper() in ['INT','BIGINT'])
			self.dateColumns = list(i for i, j in zip(columns, dataTypes) if j.upper() in ['DATETIME', 'TIMESTAMP', 'DATE'])
			self.textColumns = list(i for i, j in zip(columns, dataTypes) if j.upper() in ['TEXT', 'VARCHAR', 'CHAR'])
		except:
			self.numberColumns = list(i for i, j in zip(columns, dataTypes) if j in [20, 21, 23])
			self.dateColumns = list(i for i, j in zip(columns, dataTypes) if j in [1114, 1018])
			self.textColumns = list(i for i, j in zip(columns, dataTypes) if j in [1043, 18, 25])


	def hashString(self):

		# apply random salt to string
		self.hasher.update(self.randomSalt)

		return base64.urlsafe_b64encode(self.hasher.digest())


	def _createRandomTextColumn(self, column, data):

		# calculate lengths of values in column
		textLengths = [len(x) for x in data if x is not None]

		# check if data exists
		# fine the lnogest length
		# create random string with max length considerations
		if textLengths:
			maxLength = max(textLengths)
			randomData = (self.hashString()[0:maxLength] for _ in range(self.numberRecords))
			self.dataDict[column] = tuple(randomData)
		else:
			self.dataDict[column] = data


	def _createRandomDateColumn(self, column, data):

		# check if date column has values
		dataFilteredNone = [x for x in data if x is not None]

		# check if data exists
		# find earliest date
		# find maximum date
		# calculate difference between max and min dates
		if dataFilteredNone:
			minDate = min(dataFilteredNone)
			maxDate = max(dataFilteredNone)
			delta = (maxDate - minDate).total_seconds()

			# check if only 1 unique date exists
			# create random date with min and max date considerations
			if minDate == maxDate:
				self.dataDict[column] = data
			else:
				randomData = (minDate + datetime.timedelta(seconds=random.randint(0, delta)) for _ in range(self.numberRecords))
				self.dataDict[column] = tuple(randomData)

		else:
			self.dataDict[column] = data


	def _createRandomNumberColumn(self, column, data):

		# check if integer has values
		dataFilteredNone = [x for x in data if x is not None]

		# check if data exists
		# find maximum integer
		# find minimum integer
		if dataFilteredNone:
			maxNumber = max(dataFilteredNone)
			minNumber = min(dataFilteredNone)

			# check if only 1 unique integer exists
			# create random integer with min and max integer considerations
			if maxNumber == minNumber:
				self.dataDict[column] = data
			else:
				randomData = (random.randint(minNumber, maxNumber) for _ in range(self.numberRecords))
				self.dataDict[column] = tuple(randomData)

		else:
			self.dataDict[column] = data


	def _scrambleColumn(self, column, data):

		# shuffle data in column
		dataList= list(data)
		random.shuffle(dataList)
		self.dataDict[column] = dataList


	def _iterateColumns(self):

		# iterate through all columns
		for column, data in zip(self.columns, self.dataZip):

      		# check if column should have default value entered
			if column in self.defaultColumnValues.keys():
				self.dataDict[column] = (self.defaultColumnValues[column]) * self.numberRecords

      		# check if column placeholder should be entered
			elif column in self.identifierColumns:
				self.dataDict[column] = data

      		# check if column value will be randomly selected from data
			elif column in self.scrambleColumns:
				self._scrambleColumn(column=column, data=data)

      		# check if column value will be randomly generated
			elif column in self.randomColumns or self.allDataRandom:

        		# check if column is a number
				if column in self.numberColumns:
					self._createRandomNumberColumn(column=column, data=data)

				# check if column is a date
				elif column in self.dateColumns:
					self._createRandomDateColumn(column=column, data=data)

				# check if column is a string
				else:
					self._createRandomTextColumn(column=column, data=data)

			# select random value in existing dataset
			else:
				self._scrambleColumn(column=column, data=data)


	def scramble(self):

		# check if data exists
		# iterate through all columns
		if self.numberRecords:
			self._iterateColumns()

		self.dataScrambled = list(zip(*(self.dataDict[column] for column in self.columns)))
