class TRANSFORM():

	def __init__(self, data, columns, columnTransforms):
		self.data = data
		self.columns = columns
		self.columnTransforms = columnTransforms


	def _currency(self, index):

		self.data = [list(row) for row in self.data]

		# iterate through rows
		# transform row item to currency value
		for i, row in enumerate(self.data):
			self.data[i][index] = '${:,.2f}'.format(row[index] if row[index] else 0)

		return [tuple(row) for row in self.data]


	def transform(self):

		# check if data is empty
		if not len(self.data):
			return self.data

		# iterate through columns that need to be transformed
		for index, column in enumerate(self.columns):

			# check if columns need to be transformed to currency
			if 'currency' in self.columnTransforms and column in self.columnTransforms['currency']:
				self.data = self._currency(index=index)

		return self.data
