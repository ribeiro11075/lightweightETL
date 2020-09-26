import yaml
import os


class CONFIGURATION():

    def __init__(self):
        self.jobConfigurationFolderDirectory = '../configuration/job/'
        self.databaseConfigurationDirectory = '../configuration/environment/database.yaml'
        self.history = {
            'log': {
                'path': '../history/log/',
                'fileExtension': '.log'
                },
            'previous': {
                'path': '../history/previous/',
                'fileExtension': '.yaml'
                }
            }


    def getJobConfiguration(self, job):

        jobConfigurationDirectory = os.path.join(self.jobConfigurationFolderDirectory, job + '.yaml')

        with open(jobConfigurationDirectory) as file:
            jobConfiguration = yaml.load(file, Loader=yaml.FullLoader)

        return jobConfiguration


    def getDatabaseConfiguration(self):

        with open(self.databaseConfigurationDirectory) as file:
            databaseConfiguration = yaml.load(file, Loader=yaml.FullLoader)

            return databaseConfiguration


    def getHistoryDirectory(self, historyType, fileName):

        historyFolderDirectory = self.history[historyType]['path']
        historyFileExtension = self.history[historyType]['fileExtension']

        historyDirectory = os.path.join(historyFolderDirectory, fileName + historyFileExtension)

        return historyDirectory
