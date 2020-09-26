# lightweight-etl
A lightweight python library to to perform ETL (Extract, Transform, Load)

![ETL](https://www.blastanalytics.com/wp-content/uploads/extract-transform-load-icons-800x279.png)


## Getting Started
1. Follow the installation guide outlined below
1. Follow the code outlined in the `script` folder for guidance


## Installation

1. Setup prerequisites
    - [ ] **Required**: Install python3 by following the instructions [here](https://realpython.com/installing-python)
    - [ ] **Required**: Install pip by following the instructions [here](https://howchoo.com/g/mze4ntbknjk/install-pip-python)

1. Create a virtual environment and install dependencies
    - [ ] **Optional**: Install the virtualenv python package with `pip install virtualenv`
    - [ ] **Optional**: Create a folder to manage virtual environments with `mkdir <environment_folder_directory>`
    - [ ] **Optional**: Create a virtual environment with `virtualenv <environment_folder_directory>/<environment_name>`
    - [ ] **Optional**: Activate the virtual environment
      - Windows: `.\<environment_folder_directory>/<environment_name>\Scripts\activate`
      - MacOS: `source <environment_folder_directory>/<environment_name>\bin\activate`
    - [ ] **Required**: Navigate to the root directory of the repository with `cd <repository_directory>`
    - [ ] **Required**: Install python dependencies in the virtual environment `pip install -r requirements.txt`
    - [ ] **Optional**: Deactivate the virtual environment when finished with `deactivate`

1. Define configuration
    - [ ] **Required**: Open `configuration/environment/database.yaml` with an example at `configuration/environment/example/database.yaml`
      - <database_alias> (**Required**): Alias that will be used to get database configuration throughout scripts
      - <database_type> (**Required**): Database installation that will be connected to with options of [oracle, mysql, postgresql]
      - <database_user> (**Required**): Database user credential
      - <database_password> (**Required**): Database password credential
    - [ ] **Required**: Create `configuration/environment/runtime.yaml` (COMING SOON)
    - [ ] **Required**: Create `configuration/job/<fileName>.yaml` with an example at `configuration/job/example/job.yaml`
      - <workers> (**Required**): Concurrent multiprocessing cores to run
      - <jobName> (**Required**): Alias that will be used for logging
      - <active> (**Required**): Ability to turn on/off job with options of [true, false]
      - <refresh> (**Required**): Number of seconds before rerunning the job
      - <predecessors1> (**Optional**): Jobs that should run before defined in the file
      - <sourceDatabase> (**Required**): Database alias that data will be extracted from
      - <targetDatabase> (**Required**): Database alias that data will be loaded into
      - <insertStrategy> (**Required**): Strategy for inserting data with options of [swap, upsert]
      - <chunkSize> (**Required**): Number of rows that will be inserted to create chunks for performance
      - <targetTableStage> (**Optional**): Initial staging table load (required when <insertStrategy> = swap and used for upsert when populated)
      - <targetTableFinal> (**Required**): Final table load
      - <columnTransforms> (**Optional**): Transformations applied to columns
      - <preTargetAdhocQueries> (**Optional**): Queries that will be run before loading data
      - <postTargetAdhocQueries> (**Optional**): Queries that will be run after loading data
      - <sourceQuery> (**Required**): Query to extract data from source data
