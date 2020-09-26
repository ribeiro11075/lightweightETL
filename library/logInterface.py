import logging


class LOG():

	def __init__(self, logDirectory):

		# set logging configuration variables
		# create root logger
		# set default logging level
		format = '%(asctime)s.%(msecs)03d [%(levelname)s] :: %(message)s [%(filename)s:%(lineno)d]'
		datefmt = '%Y-%m-%d %H:%M:%S'
		formatter = logging.Formatter(fmt=format, datefmt=datefmt)
		self.logging = logging.getLogger()
		self.logging.setLevel(logging.INFO)

		# create file logging system
		# set file logging system level
		# set file logging system formatter
		# add file logging system
		fh = logging.FileHandler(filename=logDirectory)
		fh.setLevel(logging.INFO)
		fh.setFormatter(formatter)
		self.logging.addHandler(fh)
