import time
import yaml


class PREVIOUS():

	def __init__(self, previousDirectory):
		self.previousDirectory = previousDirectory

		# load integration job previous
		with open(previousDirectory) as file:
			self.previous = yaml.load(file, Loader=yaml.FullLoader)


	def addPrevious(self, job):

		# set current time to latest integration job run
		self.previous[job] = time.time()

		# save integration job previous
		with open(self.previousDirectory, 'w') as file:
			yaml.dump(self.previous, file)
